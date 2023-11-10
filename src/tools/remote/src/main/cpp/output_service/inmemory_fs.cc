#include "src/tools/remote/src/main/cpp/output_service/inmemory_fs.h"

#include <pthread.h>
#include <semaphore.h>
#include <sys/stat.h>

#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

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
  std::map<std::string, std::unique_ptr<Node>> children;
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
};

struct FileSystem {
  struct fuse *fuse;
  pthread_t thread;

  Node root;
  std::vector<std::unique_ptr<Node>> deleted_nodes;
};

static void *FuseInit(struct fuse_conn_info *conn, struct fuse_config *cfg) {
  FileSystem *fs = new FileSystem;
  fs->root.type = kDirectory;
  fs->root.dir = {};
  return fs;
}

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
    *out_child = iter->second.get();
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

std::unique_ptr<Node> RemoveNode(Node *parent, const char *name) {
  ASSERT(parent->type == kDirectory);

  auto nh = parent->dir.children.extract(std::string(name));
  ASSERT(!nh.empty());

  return std::move(nh.mapped());
}

Node *InsertNode(Node *parent, const char *name, std::unique_ptr<Node> up) {
  ASSERT(parent->type == kDirectory);

  auto key = std::string(name);
  ASSERT(parent->dir.children.find(key) == parent->dir.children.end());

  return (parent->dir.children[key] = std::move(up)).get();
}

Node *CreateNode(Node *parent, const char *name, NodeType type) {
  ASSERT(parent->type == kDirectory);

  auto key = std::string(name);
  auto result =
      parent->dir.children.emplace(key, std::move(std::make_unique<Node>()));
  Node *child = result.first->second.get();
  child->type = type;
  clock_gettime(CLOCK_REALTIME, &child->mtime);
  return child;
}

Node *CreateDirectory(Node *parent, const char *name) {
  Node *child = CreateNode(parent, name, kDirectory);
  child->dir = {};
  return child;
}

Node *CreateFile(Node *parent, const char *name) {
  Node *child = CreateNode(parent, name, kFile);
  child->file.size = 0;
  child->file.buf = nullptr;
  child->file.cap = 0;
  return child;
}

Node *CreateSymlink(Node *parent, const char *name, const char *target) {
  Node *child = CreateNode(parent, name, kSymlink);
  child->symlink.target = std::string(target);
  return child;
}

void TruncateFile(Node *node) {
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

int ReadFile(Node *node, char *buf, size_t size, off_t offset) {
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

  return 0;
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
    node = CreateFile(parent, name);
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
    node = CreateFile(parent, name);
  }

  fi->fh = (uint64_t)node;

  return 0;
}

int FuseWrite(const char *path, const char *buf, size_t size, off_t offset,
              struct fuse_file_info *fi) {
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

int FuseFlush(const char *path, struct fuse_file_info *fi) { return 0; }

int FuseRelease(const char *path, struct fuse_file_info *fi) { return 0; }

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

  CreateDirectory(parent, name);

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

  CreateSymlink(parent, name, target);

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
    fs->deleted_nodes.push_back(RemoveNode(to_parent, to_name));
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

  fs->deleted_nodes.push_back(RemoveNode(parent, name));

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

  fs->deleted_nodes.push_back(RemoveNode(parent, name));

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
  const char *mount_point;
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

  std::cerr << "Mounting fuse at " << param->mount_point << std::endl;

  // TODO: In an incremental buidl, the output path might not be clean. How do
  // we handle that case? Delete the dir?
  int ret = mkdir(param->mount_point, 0755);
  if (!(ret == 0 || (ret == -1 && errno == EEXIST))) {
    std::cerr << "Failed to mount at " << param->mount_point << ": "
              << strerror(errno) << std::endl;
    return InitError(param);
  }

  if (fuse_mount(fs->fuse, param->mount_point) != 0) {
    std::cerr << "Failed to mount fuse" << std::endl;
    return InitError(param);
  }

  sem_post(&param->init_sem);

  if (!param->has_init_error) {
    fuse_loop(fs->fuse);
  }

  return nullptr;
}

FileSystem *CreateFileSystem() {
  FileSystem *fs = new FileSystem();
  fs->root.type = kDirectory;
  return fs;
}

int Mount(FileSystem *fs, const char *mount_point) {
  ASSERT(!fs->fuse);

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
      .getxattr = FuseGetxattr,
      .readdir = FuseReaddir,
      .init = FuseInit,
      .access = FuseAccess,
      .create = FuseCreate,
      .utimens = FuseUtime,
  };
  const char *argv[] = {
      "",
      "-d",  // enable fuse debug
  };
  fuse_args args = FUSE_ARGS_INIT(sizeof(argv) / sizeof(*argv), (char **)argv);
  std::cerr << "Initializing FUSE ..." << std::endl;
  fs->fuse = fuse_new(&args, &op, sizeof(op), nullptr);
  ASSERT(fs->fuse);

  std::unique_ptr<RunParam> param = std::make_unique<RunParam>();
  param->fs = fs;
  param->mount_point = mount_point;
  int ret = sem_init(&param->init_sem, 0, 0);
  ASSERT(ret == 0);
  ret = pthread_create(&fs->thread, nullptr, RunFuseEventLoop, param.get());
  ASSERT(ret == 0);
  sem_wait(&param->init_sem);
  if (param->has_init_error) {
    fuse_destroy(fs->fuse);
    return -1;
  }

  return 0;
}

void Unmount(FileSystem *fs) {
  if (!fs->fuse) {
    return;
  }

  // Unmount fuse will stop the event loop.
  fuse_unmount(fs->fuse);
  pthread_join(fs->thread, nullptr);
  fuse_destroy(fs->fuse);
  fs->thread = 0;
  fs->fuse = nullptr;
}