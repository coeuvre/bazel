#include "src/tools/remote/src/main/cpp/output_service/fuse.h"

#include <semaphore.h>
#include <sys/stat.h>

#include "third_party/fuse/fuse.h"

struct FuseThreadParam {
  Workspace *workspace;
  sem_t init_sem;
  bool has_init_error;
};

static void *fuse_thread_main(void *arg) {
  FuseThreadParam *param = (FuseThreadParam *)arg;
  Workspace *workspace = param->workspace;

  std::cerr << "Mounting fuse at " << workspace->mount_point << std::endl;

  struct stat st;
  if (stat(workspace->mount_point.c_str(), &st) != -1) {
    std::cerr << "Mountpoint is not free" << std::endl;
    param->has_init_error = true;
  }

  if (errno != ENOENT) {
    std::cerr << "Invalid mountpoint: " << strerror(errno) << std::endl;
    param->has_init_error = true;
  }

  if (!param->has_init_error) {
    mkdir(workspace->mount_point.c_str(), 0755);

    if (fuse_mount(workspace->fuse, workspace->mount_point.c_str()) != 0) {
      std::cerr << "Failed to mount fuse" << std::endl;
      param->has_init_error = true;
    }
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

static Workspace InitWorkspace(const std::string &workspace_id) {
  Workspace workspace = {
      .valid = true,
      .workspace_id = workspace_id,
  };

  fuse_operations op = {};
  const char *argv[] = {
      "",
  };
  fuse_args args = FUSE_ARGS_INIT(1, (char **)argv);
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
    FuseThreadParam *param = new FuseThreadParam{
        .workspace = workspace,
    };

    if (sem_init(&param->init_sem, 0, 0) != 0) {
      std::cerr << "Failed to init semaphore" << std::endl;
      return InvalidBuild();
    }

    if (pthread_create(&workspace->fuse_thread, nullptr, fuse_thread_main,
                       param) != 0) {
      std::cerr << "Failed to create new thread" << std::endl;
      return InvalidBuild();
    }

    sem_wait(&param->init_sem);
    bool has_init_error = param->has_init_error;
    delete param;

    if (has_init_error) {
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
