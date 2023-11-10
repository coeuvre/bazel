#include "src/tools/remote/src/main/cpp/output_service/fuse.h"

#include <semaphore.h>
#include <sys/stat.h>

#include <memory>
#include <string_view>

#include "third_party/fuse/fuse.h"

#define ASSERT(a) \
  do {            \
    if (!(a)) {   \
      abort();    \
    }             \
  } while (0)

struct FuseThreadParam {
  Workspace *workspace;
  sem_t init_sem;
  bool has_init_error;
};

static void *InitError(FuseThreadParam *param) {
  param->has_init_error = true;
  sem_post(&param->init_sem);
  return nullptr;
}

static void *FuseThreadMain(void *arg) {
  FuseThreadParam *param = (FuseThreadParam *)arg;
  Workspace *workspace = param->workspace;

  std::cerr << "Mounting fuse at " << workspace->mount_point << std::endl;

  struct stat st;
  if (stat(workspace->mount_point.c_str(), &st) != -1) {
    std::cerr << "Mountpoint is not free" << std::endl;
    return InitError(param);
  }

  if (errno != ENOENT) {
    std::cerr << "Invalid mountpoint: " << strerror(errno) << std::endl;
    return InitError(param);
  }

  // TODO: In an incremental buidl, the output path might not be clean. How do
  // we handle that case? Delete the dir?
  if (mkdir(workspace->mount_point.c_str(), 0755) != 0) {
    std::cerr << "Failed to mkdir at " << workspace->mount_point << std::endl;
    return InitError(param);
  }

  if (fuse_mount(workspace->fuse, workspace->mount_point.c_str()) != 0) {
    std::cerr << "Failed to mount fuse" << std::endl;
    return InitError(param);
  }

  sem_post(&param->init_sem);

  if (!param->has_init_error) {
    fuse_loop(workspace->fuse);
  }

  return nullptr;
}

static Workspace InvalidWorkspace() {
  Workspace workspace = {
      .valid = false,
  };
  return workspace;
}

static Build InvalidBuild() {
  Build build = {
      .valid = false,
  };
  return build;
}

struct Node;

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

static Workspace InitWorkspace(const std::string &workspace_id) {
  Workspace workspace = {
      .valid = true,
      .workspace_id = workspace_id,
  };

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
  std::cerr << "Creating FUSE ..." << std::endl;
  workspace.fuse = fuse_new(&args, &op, sizeof(op), nullptr);
  if (workspace.fuse == nullptr) {
    std::cerr << "Failed to create fuse" << std::endl;
    return InvalidWorkspace();
  }

  return workspace;
}

static Build StartBuild(Workspace *workspace, const std::string &build_id,
                        const std::string &output_path) {
  Build build = {
      .valid = true,
      .workspace_id = workspace->workspace_id,
      .output_path = output_path,
  };

  if (workspace->mount_point != output_path) {
    if (workspace->mount_point != "") {
      // Stop previous fuse thread by unmount which will stop the fuse event
      // loop.
      fuse_unmount(workspace->fuse);
      pthread_join(workspace->fuse_thread, nullptr);
    }

    workspace->mount_point = output_path;

    // Start a new fuse thread
    auto param = std::unique_ptr<FuseThreadParam>(new FuseThreadParam{
        .workspace = workspace,
    });

    if (sem_init(&param->init_sem, 0, 0) != 0) {
      std::cerr << "Failed to init semaphore" << std::endl;
      return InvalidBuild();
    }

    if (pthread_create(&workspace->fuse_thread, nullptr, FuseThreadMain,
                       param.get()) != 0) {
      std::cerr << "Failed to create new thread" << std::endl;
      return InvalidBuild();
    }

    sem_wait(&param->init_sem);
    if (param->has_init_error) {
      workspace->mount_point = "";
      return InvalidBuild();
    }
  }

  return build;
}

static void FinalizeBuild(Workspace *workspace, Build *build) {}

grpc::Status FuseRemoteOutputService::Clean(
    grpc::ServerContext *context,
    const remote_output_service::CleanRequest *request,
    google::protobuf::Empty *response) {
  std::cerr << "Clean("
            << "workspace_id = " << request->workspace_id() << ")" << std::endl;
  return grpc::Status::OK;
}

grpc::Status FuseRemoteOutputService::StartBuild(
    grpc::ServerContext *context,
    const remote_output_service::StartBuildRequest *request,
    remote_output_service::StartBuildResponse *response) {
  std::cerr << "StartBuild("
            << "workspace_id = " << request->workspace_id()
            << ", build_id = " << request->build_id()
            << ", output_path = " << request->output_path() << ")" << std::endl;
  auto workspace_id = request->workspace_id();
  if (workspaces_.find(workspace_id) == workspaces_.end()) {
    std::cerr << "Initializing workspace ..." << std::endl;
    auto workspace = InitWorkspace(workspace_id);
    if (!workspace.valid) {
      return grpc::Status(grpc::StatusCode::INTERNAL,
                          "Failed to initialize workspace");
    }
    workspaces_.emplace(workspace_id, workspace);
  }

  auto &workspace = workspaces_[workspace_id];
  if (workspace.active_build_id != "") {
    std::cerr << "Finalize previous build" << std::endl;
    auto &build = builds_[workspace.active_build_id];
    FinalizeBuild(&workspace, &build);
    builds_.erase(workspace.active_build_id);
    workspace.active_build_id = "";
  }

  std::cerr << "Starting a new build" << std::endl;
  Build build =
      ::StartBuild(&workspace, request->build_id(), request->output_path());
  if (!build.valid) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to start build");
  }
  builds_.emplace(build.build_id, build);

  return grpc::Status::OK;
}

grpc::Status FuseRemoteOutputService::BatchCreate(
    grpc::ServerContext *context,
    const remote_output_service::BatchCreateRequest *request,
    google::protobuf::Empty *response) {
  return grpc::Status::OK;
}
