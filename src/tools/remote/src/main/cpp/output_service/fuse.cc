#include "src/tools/remote/src/main/cpp/output_service/fuse.h"

#include <signal.h>
#include <sys/stat.h>

#include <fstream>

#include "absl/strings/str_split.h"
#include "src/tools/remote/src/main/cpp/output_service/common.h"

static Build InvalidBuild() {
  Build build = {
      .valid = false,
  };
  return build;
}

static Workspace InitWorkspace(const std::string &workspace_id) {
  Workspace workspace = {
      .workspace_id = workspace_id,
      .fs = CreateFileSystem(),
  };
  return workspace;
}

static Build StartBuild(Workspace *workspace, const std::string &build_id,
                        const std::string &output_path) {
  Build build = {
      .valid = true,
      .workspace_id = workspace->workspace_id,
      .build_id = build_id,
      .output_path = output_path,
  };

  if (workspace->mount_point != output_path) {
    Unmount(workspace->fs);

    workspace->mount_point = output_path;
    int ret = Mount(workspace->fs, workspace->mount_point.c_str());
    if (ret != 0) {
      workspace->mount_point = "";
      return InvalidBuild();
    }
  }

  return build;
}

static void FinalizeBuild(Workspace *workspace, Build *build) {}

static void Clean(Workspace *workspace) {
  ASSERT(workspace->active_build_id == "");
  Unmount(workspace->fs);
  workspace->mount_point = "";
}

static FuseRemoteOutputService *INSTANCE;

static void OnExit(int sig) {
  INSTANCE->OnExit();
  exit(sig);
}

void FuseRemoteOutputService::InstallSignalHandlers() {
  ASSERT(!INSTANCE);

  INSTANCE = this;

  struct sigaction sa = {};
  sa.sa_handler = ::OnExit;
  sigemptyset(&(sa.sa_mask));
  sa.sa_flags = 0;

  if (sigaction(SIGHUP, &sa, nullptr) != 0) {
    ASSERT(false && "failed to install SIGHUP handler");
  }

  if (sigaction(SIGINT, &sa, nullptr) != 0) {
    ASSERT(false && "failed to install SIGINT handler");
  }

  if (sigaction(SIGTERM, &sa, nullptr) != 0) {
    ASSERT(false && "failed to install SIGTERM handler");
  }
}

void FuseRemoteOutputService::OnExit() {
  auto lock = std::lock_guard(this->mutex_);

  for (const auto &workspace : workspaces_) {
    Unmount(workspace.second.fs);
  }
}

grpc::Status FuseRemoteOutputService::Clean(
    grpc::ServerContext *context,
    const remote_output_service::CleanRequest *request,
    google::protobuf::Empty *response) {
  auto lock = std::lock_guard(this->mutex_);

  std::cerr << "Clean("
            << "workspace_id = " << request->workspace_id() << ")" << std::endl;

  auto &workspace_id = request->workspace_id();
  auto iter = workspaces_.find(workspace_id);
  if (iter != workspaces_.end()) {
    auto &workspace = (*iter).second;
    if (workspace.active_build_id != "") {
      std::cerr << "Finalize previous build" << std::endl;
      auto &build = builds_[workspace.active_build_id];
      FinalizeBuild(&workspace, &build);
      builds_.erase(workspace.active_build_id);
      workspace.active_build_id = "";
    }
    ::Clean(&workspace);
  }

  return grpc::Status::OK;
}

grpc::Status FuseRemoteOutputService::StartBuild(
    grpc::ServerContext *context,
    const remote_output_service::StartBuildRequest *request,
    remote_output_service::StartBuildResponse *response) {
  auto lock = std::lock_guard(this->mutex_);

  std::cerr << "StartBuild("
            << "workspace_id = " << request->workspace_id()
            << ", build_id = " << request->build_id()
            << ", output_path = " << request->output_path() << ")" << std::endl;
  auto &workspace_id = request->workspace_id();
  if (workspaces_.find(workspace_id) == workspaces_.end()) {
    std::cerr << "Initializing workspace " << workspace_id << " ..."
              << std::endl;
    workspaces_.emplace(workspace_id, InitWorkspace(workspace_id));
  }

  auto &workspace = workspaces_[workspace_id];
  if (workspace.active_build_id != "") {
    std::cerr << "Finalize previous build " << workspace.active_build_id
              << std::endl;
    auto &build = builds_[workspace.active_build_id];
    FinalizeBuild(&workspace, &build);
    builds_.erase(workspace.active_build_id);
    workspace.active_build_id = "";
  }

  std::cerr << "Starting a new build " << request->build_id() << std::endl;
  Build build =
      ::StartBuild(&workspace, request->build_id(), request->output_path());
  if (!build.valid) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to start build");
  }
  builds_.emplace(build.build_id, build);

  workspace.active_build_id = build.build_id;

  return grpc::Status::OK;
}

static bool CreateParentDirs(const std::string &output_path,
                             const std::string &path) {
  std::vector<std::string> components = absl::StrSplit(path, '/');
  std::string dir = output_path;
  for (size_t i = 0; i < components.size() - 1; ++i) {
    dir += "/" + components[i];
    if (mkdir(dir.c_str(), 0755) != 0) {
      // TODO(chiwang): Handle the case where the path exists but is not a
      // directory
      if (errno != EEXIST) {
        std::cerr << "Failed to create diretory at " << dir << ": "
                  << strerror(errno) << std::endl;
        return false;
      }
    }
  }
  return true;
}

static bool CopyFile(const std::string &dst, const std::string &src) {
  std::ofstream output(dst, std::ios::binary);
  std::ifstream input(src, std::ios::binary);
  output << input.rdbuf();
  return true;
}

static bool CreateFile(
    const std::string &output_path, const std::string &disk_cache,
    const std::string &path,
    const build::bazel::remote::execution::v2::Digest &digest, int mode) {
  auto blob_path =
      disk_cache + "/cas/" + digest.hash().substr(0, 2) + "/" + digest.hash();

  auto output = output_path + "/" + path;

  if (!CreateParentDirs(output_path, path)) {
    return false;
  }

  if (!CopyFile(output, blob_path)) {
    return false;
  }

  if (chmod(output.c_str(), mode) != 0) {
    std::cerr << "Failed to chmod " << output << ": " << strerror(errno)
              << std::endl;
    return false;
  }

  return true;
}

static bool CreateSymlink(const std::string &output_path,
                          const std::string &path, const std::string &target) {
  if (!CreateParentDirs(output_path, path)) {
    return false;
  }

  auto output = output_path + "/" + path;

  if (symlink(target.c_str(), output.c_str()) != 0) {
    std::cerr << "Failed to create symlink at " << output << ": "
              << strerror(errno) << std::endl;
    return false;
  }

  return true;
}

grpc::Status FuseRemoteOutputService::BatchCreate(
    grpc::ServerContext *context,
    const remote_output_service::BatchCreateRequest *request,
    google::protobuf::Empty *response) {
  auto lock = std::lock_guard(this->mutex_);

  std::cerr << "BatchCreate(build_id = " << request->build_id() << ", ...)"
            << std::endl;

  auto &build_id = request->build_id();
  if (builds_.find(build_id) == builds_.end()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Unknown build_id");
  }
  auto &build = builds_[request->build_id()];

  if (workspaces_.find(build.workspace_id) == workspaces_.end()) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Unknown workspace_id");
  }
  auto &workspace = workspaces_[build.workspace_id];

  if (workspace.active_build_id != build_id) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "build is not active");
  }

  for (auto &file : request->files()) {
    std::cerr << "  file: " << file.path()
              << ", digest = " << file.digest().DebugString() << std::endl;
    if (!CreateFile(build.output_path, disk_cache_, file.path(), file.digest(),
                    file.mode())) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to create file");
    }
  }

  for (auto &symlink : request->symlinks()) {
    std::cerr << "    symlink: " << symlink.path()
              << ", target = " << symlink.target_path() << std::endl;
    if (!CreateSymlink(build.output_path, symlink.path(),
                       symlink.target_path())) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to create file");
    }
  }

  return grpc::Status::OK;
}
