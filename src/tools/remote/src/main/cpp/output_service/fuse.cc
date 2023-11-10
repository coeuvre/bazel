#include "src/tools/remote/src/main/cpp/output_service/fuse.h"

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
    workspaces_.emplace(workspace_id, InitWorkspace(workspace_id));
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
