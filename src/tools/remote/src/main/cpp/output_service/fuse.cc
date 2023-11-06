#include "src/tools/remote/src/main/cpp/output_service/fuse.h"

#include <semaphore.h>
#include <sys/stat.h>

#include <memory>

#include "third_party/fuse/fuse.h"

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
};

struct FileNode {};

struct DirectoryNode {
  std::map<std::string, Node *> children;
};

struct Node {
  NodeType type;
  union {
    FileNode file;
    DirectoryNode dir;
  };
};

struct FileSystem {
  Node root;
};

static void *FuseInit(struct fuse_conn_info *conn, struct fuse_config *cfg) {
  FileSystem *fs = (FileSystem *)malloc(sizeof(FileSystem));
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

  *out_child = iter->second;
  return 0;
}

static const char *LocateFirstChar(const char *str, int count, char ch) {
  for (int i = 0; i < count; ++i) {
    if (str[i] == ch) {
      return str + i;
    }
  }
  return nullptr;
}

static const char *LocateLastChar(const char *str, int count, char ch) {
  for (int i = count - 1; i >= 0; --i) {
    if (str[i] == ch) {
      return str + i;
    }
  }
  return nullptr;
}

static int GetNode(FileSystem *fs, const char *path, int count,
                   Node **out_node) {
  if (count <= 0 || path[0] != '/') {
    return -EBADF;
  }

  const char *end = path + count;

  path += 1;
  Node *node = &fs->root;

  while (true) {
    const char *s = LocateFirstChar(path, end - path, '/');
    if (s == nullptr) {
      break;
    }

    int ret = GetChild(node, path, s - path, &node);
    if (ret != 0) {
      return ret;
    }

    path = s + 1;
  }

  if (path < end) {
    return GetChild(node, path, end - path, out_node);
  }

  *out_node = node;
  return 0;
}

static int FuseGetattr(const char *path, struct stat *stbuf,
                       struct fuse_file_info *fi) {
  return -ENOENT;
}

static int FuseMkdir(const char *path, mode_t mode) {
  fuse_context *ctx = fuse_get_context();

  int count = strlen(path);
  const char *end = path + count;
  const char *s = LocateLastChar(path, count, '/');
  if (s == nullptr) {
    return -EBADF;
  }

  FileSystem *fs = (FileSystem *)ctx->private_data;
  Node *parent;
  int ret = GetNode(fs, path, s == path ? 1 : s - path, &parent);
  if (ret != 0) {
    return ret;
  }

  Node *child;
  const char *name = s + 1;
  int name_count = end - name;
  ret = GetChild(parent, name, name_count, &child);
  if (ret == 0) {
    return -EEXIST;
  } else if (ret != -ENOENT) {
    return ret;
  }

  child = (Node *)malloc(sizeof(Node));
  child->type = kDirectory;
  child->dir = {};
  auto key = std::string(name, name_count);
  parent->dir.children[key] = child;

  return 0;
}

static Workspace InitWorkspace(const std::string &workspace_id) {
  Workspace workspace = {
      .valid = true,
      .workspace_id = workspace_id,
  };

  fuse_operations op = {
      .getattr = FuseGetattr,
      .mkdir = FuseMkdir,
      .init = FuseInit,
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
