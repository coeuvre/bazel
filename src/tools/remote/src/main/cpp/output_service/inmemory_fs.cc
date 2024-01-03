#include "src/tools/remote/src/main/cpp/output_service/inmemory_fs.h"

#include <pthread.h>
#include <semaphore.h>
#include <sys/stat.h>
#include <sys/xattr.h>

#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "inmemory_fs.h"
#include "src/tools/remote/src/main/cpp/output_service/common.h"
#include "third_party/fuse/fuse.h"

enum NodeType {
  kFile,
  kDirectory,
  kSymlink,
};

struct FileNode {
  off_t size;
  char *buf;
  off_t cap;
};

struct DirectoryNode {
  std::map<std::string, Node *> children;
};

struct SymlinkNode {
  std::string target;
};

struct Node {
  NodeType type;

  FileNode file;
  DirectoryNode dir;
  SymlinkNode symlink;

  timespec mtime;

  std::unordered_map<std::string, std::vector<char>> xattrs;
};

struct FileSystem {
  std::string mount_point;
  std::string unix_digest_hash_attribute_name;
  struct fuse *fuse;
  pthread_t thread;

  Node root;
  // An arena for Node
  std::vector<std::unique_ptr<Node>> nodes;
};

static int GetChild(Node *node, const char *name, int count, Node **out_child) {
  if (node->type != kDirectory) {
    return -ENOTDIR;
  }

  std::string key(name, count);
  auto iter = node->dir.children.find(key);
  if (iter == node->dir.children.end()) {
    return -ENOENT;
  }

  if (out_child) {
    *out_child = iter->second;
  }

  return 0;
}

// Returns 0 if parent dir exists or path is '/'.
//   Only use *out_parent and *out_node if returned 0.
static int GetNode(FileSystem *fs, const char *path, Node **out_parent,
                   Node **out_node, const char **out_node_name = nullptr) {
  if (path[0] != '/') {
    return -EBADF;
  }

  int ret = 0;
  const char *end = path + strlen(path);

  Node *node = &fs->root;
  path += 1;
  while (true) {
    const char *s = strchr(path, '/');
    if (s == nullptr) {
      break;
    }

    if (ret == 0) {
      Node *child = nullptr;
      ret = GetChild(node, path, s - path, &child);
      node = child;
    }

    path = s + 1;
  }

  if (ret == 0) {
    if (path < end) {
      if (out_parent) {
        *out_parent = node;
      }
      ret = GetChild(node, path, end - path, out_node);
      if (ret == -ENOENT) {
        if (out_node) {
          *out_node = nullptr;
        }
        ret = 0;
      }
      if (out_node_name) {
        *out_node_name = path;
      }
    } else {
      // path is '/'
      if (out_parent) {
        *out_parent = nullptr;
      }
      if (out_node) {
        *out_node = node;
      }
      if (out_node_name) {
        *out_node_name = end - 1;
      }
    }
  }

  return ret;
}

static FileSystem *GetFileSystem() {
  fuse_context *ctx = fuse_get_context();
  FileSystem *fs = (FileSystem *)ctx->private_data;
  return fs;
}

static Node *RemoveNode(Node *parent, const char *name) {
  ASSERT(parent->type == kDirectory);

  auto nh = parent->dir.children.extract(std::string(name));
  ASSERT(!nh.empty());

  return nh.mapped();
}

static void InsertNode(Node *parent, const char *name, Node *child) {
  ASSERT(parent->type == kDirectory);

  auto key = std::string(name);
  ASSERT(parent->dir.children.find(key) == parent->dir.children.end());
  parent->dir.children[key] = child;
}

static Node *CreateNode(FileSystem *fs, Node *parent, const char *name,
                        NodeType type) {
  ASSERT(parent->type == kDirectory);

  auto key = std::string(name);
  ASSERT(parent->dir.children.find(key) == parent->dir.children.end());

  fs->nodes.push_back(std::make_unique<Node>());
  Node *child = (*(fs->nodes.end() - 1)).get();
  child->type = type;
  clock_gettime(CLOCK_REALTIME, &child->mtime);

  parent->dir.children.emplace(key, child);

  return child;
}

static Node *CreateDirectory(FileSystem *fs, Node *parent, const char *name) {
  Node *child = CreateNode(fs, parent, name, kDirectory);
  child->dir = {};
  return child;
}

static Node *CreateFile(FileSystem *fs, Node *parent, const char *name) {
  Node *child = CreateNode(fs, parent, name, kFile);
  child->file.size = 0;
  child->file.buf = nullptr;
  child->file.cap = 0;
  return child;
}

static Node *CreateSymlink(FileSystem *fs, Node *parent, const char *name,
                           const char *target) {
  Node *child = CreateNode(fs, parent, name, kSymlink);
  child->symlink.target = std::string(target);
  return child;
}

static void TruncateFile(Node *node) {
  ASSERT(node->type == kFile);
  node->file.size = 0;
  clock_gettime(CLOCK_REALTIME, &node->mtime);
}

int WriteFile(Node *node, const char *buf, size_t size, off_t offset) {
  ASSERT(node->type == kFile);

  FileNode *file = &node->file;
  off_t end = offset + size;

  if (end > file->cap) {
    file->buf = (char *)realloc(file->buf, end);
    ASSERT(file->buf);
    file->cap = end;
  }

  if (end > file->size) {
    file->size = end;
  }

  memcpy(file->buf + offset, buf, size);

  clock_gettime(CLOCK_REALTIME, &node->mtime);

  return size;
}

static int ReadFile(Node *node, char *buf, size_t size, off_t offset) {
  ASSERT(node->type == kFile);

  FileNode *file = &node->file;

  off_t end = offset + size;
  if (end > file->size) {
    end = file->size;
  }
  size = end - offset;

  memcpy(buf, file->buf + offset, size);

  return size;
}

static int FuseAccess(const char *path, int mask) {
  FileSystem *fs = GetFileSystem();
  Node *node;
  int ret = GetNode(fs, path, nullptr, &node);
  if (ret != 0) {
    return ret;
  }
  if (!node) {
    return -ENOENT;
  }
  return 0;
}

static int FuseGetattr(const char *path, struct stat *stbuf,
                       struct fuse_file_info *fi) {
  // TODO: handle fi?

  FileSystem *fs = GetFileSystem();

  Node *node;
  int ret = GetNode(fs, path, nullptr, &node);
  if (ret != 0) {
    return ret;
  }
  if (node == nullptr) {
    return -ENOENT;
  }

  switch (node->type) {
    case kFile: {
      stbuf->st_mode = S_IFREG | 0777;
      stbuf->st_nlink = 1;
      stbuf->st_uid = 0;
      stbuf->st_gid = 0;
      stbuf->st_size = node->file.size;
      stbuf->st_blksize = 4096;
      stbuf->st_blocks = node->file.size / 512 + ((node->file.size % 512) != 0);
      stbuf->st_atim = node->mtime;
      stbuf->st_mtim = node->mtime;
      stbuf->st_ctim = node->mtime;
    } break;

    case kDirectory: {
      stbuf->st_mode = S_IFDIR | 0777;
      stbuf->st_nlink = 1;
      stbuf->st_uid = 0;
      stbuf->st_gid = 0;
      stbuf->st_size = 1;
      stbuf->st_blksize = 1;
      stbuf->st_blocks = 1;
      stbuf->st_atim = node->mtime;
      stbuf->st_mtim = node->mtime;
      stbuf->st_ctim = node->mtime;
    } break;

    case kSymlink: {
      stbuf->st_mode = S_IFLNK | 0777;
      stbuf->st_nlink = 1;
      stbuf->st_uid = 0;
      stbuf->st_gid = 0;
      stbuf->st_size = node->symlink.target.size();
      stbuf->st_blksize = 1;
      stbuf->st_blocks = 1;
      stbuf->st_atim = node->mtime;
      stbuf->st_mtim = node->mtime;
      stbuf->st_ctim = node->mtime;
    } break;

    default: {
      return -ENOSYS;
    } break;
  }

  return 0;
}

static int FuseSetxattr(const char *path, const char *name, const char *value,
                        size_t size, int flags) {
  FileSystem *fs = GetFileSystem();

  Node *node;
  int ret = GetNode(fs, path, nullptr, &node);
  if (ret != 0) {
    return ret;
  }
  if (!node) {
    return -ENOENT;
  }

  auto name_str = std::string(name);
  bool exist = node->xattrs.find(name_str) != node->xattrs.end();
  if (flags == XATTR_CREATE && exist) {
    return -EEXIST;
  }
  if (flags == XATTR_REPLACE && !exist) {
    return -ENODATA;
  }

  node->xattrs[name_str] = std::vector<char>(value, value + size);

  return 0;
}

static int FuseGetxattr(const char *path, const char *name, char *value,
                        size_t size) {
  FileSystem *fs = GetFileSystem();

  Node *node;
  int ret = GetNode(fs, path, nullptr, &node);
  if (ret != 0) {
    return ret;
  }
  if (!node) {
    return -ENOENT;
  }

  auto iter = node->xattrs.find(std::string(name));
  if (iter == node->xattrs.end()) {
    return -ENODATA;
  }

  auto &v = iter->second;
  if (size > 0) {
    if (v.size() > size) {
      return -ERANGE;
    }
    memcpy(value, v.data(), v.size());
  }
  return v.size();
}

static int FuseListxattr(const char *path, char *list, size_t size) {
  FileSystem *fs = GetFileSystem();

  Node *node;
  int ret = GetNode(fs, path, nullptr, &node);
  if (ret != 0) {
    return ret;
  }
  if (!node) {
    return -ENOENT;
  }

  int idx = 0;
  for (auto &it : node->xattrs) {
    auto &name = it.first;
    auto name_size = name.size() + 1;  // include the null-terminator
    if (size > 0) {
      if (idx + name_size > size) {
        return -ERANGE;
      }
      memcpy(list + idx, name.c_str(), name_size);
    }
    idx += name_size;
  }

  return idx;
}

static int FuseOpen(const char *path, struct fuse_file_info *fi) {
  if ((fi->flags & O_APPEND) != 0) {
    std::cerr << "TODO: Handle O_APPEND" << std::endl;
    return -ENOSYS;
  }

  FileSystem *fs = GetFileSystem();
  Node *parent, *node;
  const char *name;
  int ret = GetNode(fs, path, &parent, &node, &name);
  if (ret != 0) {
    return ret;
  }
  if (!parent) {
    return -ENOTDIR;
  }

  if (!node) {
    if ((fi->flags & O_CREAT) == 0) {
      return -ENOENT;
    }
    node = CreateFile(fs, parent, name);
  } else {
    if (node->type != kFile) {
      return -EBADF;
    }

    if (fi->flags & O_TRUNC) {
      TruncateFile(node);
    }
  }

  fi->fh = (size_t)node;

  return 0;
}

static int FuseCreate(const char *path, mode_t mode,
                      struct fuse_file_info *fi) {
  FileSystem *fs = GetFileSystem();

  Node *parent, *node;
  const char *name;
  int ret = GetNode(fs, path, &parent, &node, &name);
  if (ret != 0) {
    return ret;
  }
  if (!parent) {
    return -ENOTDIR;
  }

  if (node) {
    if (node->type != kFile) {
      return -EBADF;
    }
    TruncateFile(node);
  } else {
    node = CreateFile(fs, parent, name);
  }

  fi->fh = (uint64_t)node;

  return 0;
}

static int FuseWrite(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi) {
  if (fi == NULL) {
    return -EBADF;
  }

  Node *node = (Node *)fi->fh;
  if (!node || node->type != kFile) {
    return -EBADF;
  }

  return WriteFile(node, buf, size, offset);
}

static int FuseRead(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi) {
  if (fi == NULL) {
    return -EBADF;
  }

  Node *node = (Node *)fi->fh;
  if (!node || node->type != kFile) {
    return -EBADF;
  }

  return ReadFile(node, buf, size, offset);
}

static int FuseFlush(const char *path, struct fuse_file_info *fi) { return 0; }

static int FuseRelease(const char *path, struct fuse_file_info *fi) {
  return 0;
}

static int FuseMkdir(const char *path, mode_t mode) {
  FileSystem *fs = GetFileSystem();
  Node *parent, *node;
  const char *name;
  int ret = GetNode(fs, path, &parent, &node, &name);
  if (ret != 0) {
    return ret;
  }
  if (node != nullptr) {
    return -EEXIST;
  }
  if (parent == nullptr) {
    return -ENOTDIR;
  }

  CreateDirectory(fs, parent, name);

  return 0;
}

static int FuseChmod(const char *path, mode_t mode, struct fuse_file_info *fi) {
  FileSystem *fs = GetFileSystem();
  Node *parent, *node;
  const char *name;
  int ret = GetNode(fs, path, &parent, &node, &name);
  if (ret != 0) {
    return ret;
  }
  if (!node) {
    return -ENOENT;
  }

  return 0;
}

static int FuseUtime(const char *path, const struct timespec ts[2],
                     struct fuse_file_info *fi) {
  FileSystem *fs = GetFileSystem();
  Node *parent, *node;
  const char *name;
  int ret = GetNode(fs, path, &parent, &node, &name);
  if (ret != 0) {
    return ret;
  }
  if (!node) {
    return -ENOENT;
  }
  node->mtime = ts[1];
  return 0;
}

static int FuseSymlink(const char *target, const char *linkpath) {
  FileSystem *fs = GetFileSystem();
  Node *parent, *node;
  const char *name;
  int ret = GetNode(fs, linkpath, &parent, &node, &name);
  if (ret != 0) {
    return ret;
  }
  if (node) {
    return -EEXIST;
  }
  if (!parent) {
    return -ENOTDIR;
  }

  CreateSymlink(fs, parent, name, target);

  return 0;
}

static int FuseRename(const char *from, const char *to, unsigned int flags) {
  if (flags) {
    std::cerr << "Invalid flags " << flags << std::endl;
    return -EINVAL;
  }

  FileSystem *fs = GetFileSystem();
  Node *from_node, *from_parent;
  const char *from_name;
  int ret = GetNode(fs, from, &from_parent, &from_node, &from_name);
  if (ret != 0) {
    return ret;
  }
  if (!from_node) {
    return -ENOENT;
  }

  Node *to_node, *to_parent;
  const char *to_name;
  ret = GetNode(fs, to, &to_parent, &to_node, &to_name);
  if (ret != 0) {
    return ret;
  }
  if (!to_parent) {
    return -ENOTDIR;
  }

  if (to_node == from_node) {
    return 0;
  }

  if (to_node) {
    RemoveNode(to_parent, to_name);
  }

  // TODO: cycles?
  InsertNode(to_parent, to_name, RemoveNode(from_parent, from_name));

  return 0;
}

static int FuseReaddir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi,
                       enum fuse_readdir_flags flags) {
  FileSystem *fs = GetFileSystem();
  Node *node;
  int ret = GetNode(fs, path, nullptr, &node);
  if (ret != 0) {
    return ret;
  }
  if (!node) {
    return -ENOENT;
  }
  if (node->type != kDirectory) {
    return -ENOTDIR;
  }

  for (const auto &entry : node->dir.children) {
    if (filler(buf, entry.first.c_str(), nullptr, 0, (fuse_fill_dir_flags)0)) {
      return -ENOMEM;
    }
  }

  return 0;
}

static int FuseUnlink(const char *path) {
  FileSystem *fs = GetFileSystem();
  Node *node, *parent;
  const char *name;
  int ret = GetNode(fs, path, &parent, &node, &name);
  if (ret != 0) {
    return ret;
  }
  if (!node) {
    return -ENOENT;
  }
  if (node->type == kDirectory) {
    return -EISDIR;
  }

  ASSERT(parent);

  RemoveNode(parent, name);

  return 0;
}

static int FuseRmdir(const char *path) {
  FileSystem *fs = GetFileSystem();
  Node *node, *parent;
  const char *name;
  int ret = GetNode(fs, path, &parent, &node, &name);
  if (ret != 0) {
    return ret;
  }
  if (!node) {
    return -ENOENT;
  }
  if (node->type != kDirectory) {
    return -ENOTDIR;
  }

  if (!parent) {
    // path is '/'
    return -EBUSY;
  }

  ASSERT(parent);

  RemoveNode(parent, name);

  return 0;
}

static int FuseReadlink(const char *path, char *buf, size_t size) {
  FileSystem *fs = GetFileSystem();
  Node *node;
  int ret = GetNode(fs, path, nullptr, &node);
  if (ret != 0) {
    return ret;
  }
  if (!node) {
    return -ENOENT;
  }
  if (node->type != kSymlink) {
    return -EINVAL;
  }

  size_t str_size = std::min(node->symlink.target.size(), size - 1);
  memcpy(buf, node->symlink.target.c_str(), str_size);
  buf[str_size] = 0;

  return 0;
}

struct RunParam {
  FileSystem *fs;
  sem_t init_sem;
  bool has_init_error;
};

static void *InitError(RunParam *param) {
  param->has_init_error = true;
  sem_post(&param->init_sem);
  return nullptr;
}

static void *RunFuseEventLoop(void *param_) {
  RunParam *param = (RunParam *)param_;
  FileSystem *fs = param->fs;
  const char *mount_point = fs->mount_point.c_str();

  std::cerr << "Mounting fuse at " << fs->mount_point << " ..." << std::endl;
  std::cerr << "    unix_digest_hash_attribute_name = "
            << fs->unix_digest_hash_attribute_name << std::endl;

  // TODO: In an incremental buidl, the output path might not be clean. How do
  // we handle that case? Delete the dir?
  int ret = mkdir(mount_point, 0755);
  if (!(ret == 0 || (ret == -1 && errno == EEXIST))) {
    std::cerr << "Failed to mount at " << mount_point << ": " << strerror(errno)
              << std::endl;
    return InitError(param);
  }

  if (fuse_mount(fs->fuse, mount_point) != 0) {
    std::cerr << "Failed to mount fuse" << std::endl;
    return InitError(param);
  }

  sem_post(&param->init_sem);

  if (!param->has_init_error) {
    fuse_loop(fs->fuse);
  }

  return nullptr;
}

FileSystem *Mount(const char *mount_point,
                  const char *unix_digest_hash_attribute_name) {
  FileSystem *fs = new FileSystem();
  fs->mount_point = std::string(mount_point);
  fs->unix_digest_hash_attribute_name =
      std::string(unix_digest_hash_attribute_name);
  fs->root.type = kDirectory;

  fuse_operations op = {
      .getattr = FuseGetattr,
      .readlink = FuseReadlink,
      .mkdir = FuseMkdir,
      .unlink = FuseUnlink,
      .rmdir = FuseRmdir,
      .symlink = FuseSymlink,
      .rename = FuseRename,
      .chmod = FuseChmod,
      .open = FuseOpen,
      .read = FuseRead,
      .write = FuseWrite,
      .flush = FuseFlush,
      .release = FuseRelease,
      .setxattr = FuseSetxattr,
      .getxattr = FuseGetxattr,
      .listxattr = FuseListxattr,
      .readdir = FuseReaddir,
      .access = FuseAccess,
      .create = FuseCreate,
      .utimens = FuseUtime,
  };
  const char *argv[] = {
      "",
      "-d",  // enable fuse debug
  };
  fuse_args args = FUSE_ARGS_INIT(sizeof(argv) / sizeof(*argv), (char **)argv);
  fs->fuse = fuse_new(&args, &op, sizeof(op), fs);
  ASSERT(fs->fuse);

  std::unique_ptr<RunParam> param = std::make_unique<RunParam>();
  param->fs = fs;
  int ret = sem_init(&param->init_sem, 0, 0);
  ASSERT(ret == 0);
  ret = pthread_create(&fs->thread, nullptr, RunFuseEventLoop, param.get());
  ASSERT(ret == 0);
  sem_wait(&param->init_sem);
  if (param->has_init_error) {
    fuse_destroy(fs->fuse);
    delete fs;
    return nullptr;
  }

  return fs;
}

void Unmount(FileSystem *fs) {
  std::cerr << "Unmounting fuse at " << fs->mount_point << std::endl;
  // Unmount fuse will stop the event loop.
  ASSERT(fs->fuse);
  fuse_unmount(fs->fuse);
  pthread_join(fs->thread, nullptr);
  fuse_destroy(fs->fuse);
  delete fs;
}
