#include "src/tools/remote/src/main/cpp/output_service/impl.h"

#include <signal.h>
#include <sys/stat.h>

#include <fstream>

#include "absl/strings/str_split.h"
#include "impl.h"
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
  };
  return workspace;
}

static Build StartBuild(Workspace *workspace, const std::string &build_id,
                        const std::string &output_path,
                        const std::string &unix_digest_hash_attribute_name) {
  Build build = {
      .valid = true,
      .workspace_id = workspace->workspace_id,
      .build_id = build_id,
      .output_path = output_path,
  };

  if (workspace->mount_point != output_path ||
      workspace->unix_digest_hash_attribute_name !=
          unix_digest_hash_attribute_name) {
    if (workspace->fs) {
      Unmount(workspace->fs);
    }

    workspace->mount_point = output_path;
    workspace->unix_digest_hash_attribute_name =
        unix_digest_hash_attribute_name;
    workspace->fs = Mount(workspace->mount_point.c_str(),
                          unix_digest_hash_attribute_name.c_str());
    if (!workspace->fs) {
      workspace->mount_point = "";
      workspace->unix_digest_hash_attribute_name = "";
      return InvalidBuild();
    }
  }

  return build;
}

static void FinalizeBuild(Workspace *workspace, Build *build) {
  workspace->active_build_id = "";
}

static void Clean(Workspace *workspace) {
  ASSERT(workspace->active_build_id == "");
  if (workspace->fs) {
    Unmount(workspace->fs);
  }
  workspace->mount_point = "";
}

static RemoteOutputServiceImpl *INSTANCE;

static void OnExit(int sig) {
  INSTANCE->OnExit();
  exit(sig);
}

void RemoteOutputServiceImpl::InstallSignalHandlers() {
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

void RemoteOutputServiceImpl::OnExit() {
  auto lock = std::lock_guard(this->mutex_);

  for (const auto &workspace : workspaces_) {
    Unmount(workspace.second.fs);
  }
}

static grpc::Status GetActiveBuildAndWorkspace(
    std::unordered_map<std::string, Workspace> &workspaces,
    std::unordered_map<std::string, Build> &builds, const std::string &build_id,
    Build **out_build, Workspace **out_workspace) {
  if (builds.find(build_id) == builds.end()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Unknown build_id");
  }

  Build *build = &builds[build_id];

  if (workspaces.find(build->workspace_id) == workspaces.end()) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Unknown workspace_id");
  }
  Workspace *workspace = &workspaces[build->workspace_id];

  if (workspace->active_build_id != build_id) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "build is not active");
  }

  *out_build = build;
  *out_workspace = workspace;

  return grpc::Status::OK;
}

grpc::Status RemoteOutputServiceImpl::Clean(
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
      ::FinalizeBuild(&workspace, &build);
      builds_.erase(workspace.active_build_id);
      workspace.active_build_id = "";
    }
    ::Clean(&workspace);
  }

  return grpc::Status::OK;
}

grpc::Status RemoteOutputServiceImpl::StartBuild(
    grpc::ServerContext *context,
    const remote_output_service::StartBuildRequest *request,
    remote_output_service::StartBuildResponse *response) {
  auto lock = std::lock_guard(this->mutex_);

  std::cerr << "StartBuild("
            << "workspace_id = " << request->workspace_id()
            << ", build_id = " << request->build_id()
            << ", output_path = " << request->output_path()
            << ", digest_function = " << request->digest_function()
            << ", unix_digest_hash_attribute_name = "
            << request->unix_digest_hash_attribute_name() << ")" << std::endl;
  auto &workspace_id = request->workspace_id();
  if (workspaces_.find(workspace_id) == workspaces_.end()) {
    std::cerr << "Initializing workspace " << workspace_id << " ..."
              << std::endl;
    workspaces_.emplace(workspace_id, InitWorkspace(workspace_id));
  }

  auto &workspace = workspaces_[workspace_id];
  if (workspace.active_build_id != "") {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "another build is active");
  }

  if (request->digest_function() != "SHA-256") {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Unsupported digest function");
  }

  std::cerr << "Starting a new build " << request->build_id() << std::endl;
  Build build =
      ::StartBuild(&workspace, request->build_id(), request->output_path(),
                   request->unix_digest_hash_attribute_name());
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

grpc::Status RemoteOutputServiceImpl::BatchCreate(
    grpc::ServerContext *context,
    const remote_output_service::BatchCreateRequest *request,
    google::protobuf::Empty *response) {
  auto lock = std::lock_guard(this->mutex_);

  std::cerr << "BatchCreate(build_id = " << request->build_id() << ", ...)"
            << std::endl;

  auto &build_id = request->build_id();
  Build *build;
  Workspace *workspace;
  auto result = GetActiveBuildAndWorkspace(workspaces_, builds_, build_id,
                                           &build, &workspace);
  if (!result.ok()) {
    return result;
  }

  for (auto &file : request->files()) {
    std::cerr << "    file: " << file.path()
              << ", hash = " << file.digest().hash()
              << ", size = " << file.digest().size_bytes() << std::endl;
    if (!CreateFile(build->output_path, disk_cache_, file.path(), file.digest(),
                    file.mode())) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to create file");
    }

    auto path = "/" + file.path();
    auto hash = file.digest().hash();
    std::cerr << "        setting hash " << hash << " to xattr on path " << path
              << std::endl;
    MaybeSetDigestHashToXAttr(workspace->fs, path.c_str(), hash.c_str());
  }

  for (auto &symlink : request->symlinks()) {
    std::cerr << "    symlink: " << symlink.path()
              << ", target = " << symlink.target_path() << std::endl;
    if (!CreateSymlink(build->output_path, symlink.path(),
                       symlink.target_path())) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to create file");
    }
  }

  return grpc::Status::OK;
}

grpc::Status RemoteOutputServiceImpl::BatchStat(
    grpc::ServerContext *context,
    const remote_output_service::BatchStatRequest *request,
    remote_output_service::BatchStatResponse *response) {
  auto lock = std::lock_guard(this->mutex_);

  std::cerr << "BatchStat("
            << "build_id = " << request->build_id() << ", ...)" << std::endl;

  auto &build_id = request->build_id();
  Build *build;
  Workspace *workspace;
  auto result = GetActiveBuildAndWorkspace(workspaces_, builds_, build_id,
                                           &build, &workspace);
  if (!result.ok()) {
    return result;
  }

  for (auto &path : request->paths()) {
    std::cerr << "    " << path << std::endl;
    auto res = response->add_responses();

    auto fullpath = build->output_path + "/" + path;
    struct stat buf;
    if (stat(fullpath.c_str(), &buf) == 0) {
      switch (buf.st_mode & S_IFMT) {
        case S_IFREG: {
        } break;

        case S_IFLNK: {
        } break;

        case S_IFDIR: {
        } break;

        default: {
          // Ignore other types
        } break;
      }
    }
  }

  return grpc::Status::OK;
}

grpc::Status RemoteOutputServiceImpl::FinalizeBuild(
    grpc::ServerContext *context,
    const remote_output_service::FinalizeBuildRequest *request,
    google::protobuf::Empty *response) {
  auto lock = std::lock_guard(this->mutex_);

  std::cerr << "FinalizeBuild("
            << "build_id = " << request->build_id()
            << ", build_successful = " << request->build_successful() << ")"
            << std::endl;

  auto &build_id = request->build_id();
  Build *build;
  Workspace *workspace;
  auto result = GetActiveBuildAndWorkspace(workspaces_, builds_, build_id,
                                           &build, &workspace);
  if (!result.ok()) {
    return result;
  }

  ::FinalizeBuild(workspace, build);

  builds_.erase(build_id);

  return grpc::Status::OK;
}