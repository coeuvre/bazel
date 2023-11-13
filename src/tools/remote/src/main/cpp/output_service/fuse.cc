#include "src/tools/remote/src/main/cpp/output_service/fuse.h"

#include <signal.h>

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

  if (sigaction(SIGHUP, &sa, nullptr) !=  0) {
    ASSERT(false && "failed to install SIGHUP handler");
  }

  if (sigaction(SIGINT, &sa, nullptr) !=  0) {
    ASSERT(false && "failed to install SIGINT handler");
  }

  if (sigaction(SIGTERM, &sa, nullptr) !=  0) {
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

  return grpc::Status::OK;
}

grpc::Status FuseRemoteOutputService::BatchCreate(
    grpc::ServerContext *context,
    const remote_output_service::BatchCreateRequest *request,
    google::protobuf::Empty *response) {
  auto lock = std::lock_guard(this->mutex_);

  return grpc::Status::OK;
}
