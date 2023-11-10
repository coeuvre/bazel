#ifndef BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_FUSE_H_
#define BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_FUSE_H_

#include <string>

#include "src/main/protobuf/remote_output_service.grpc.pb.h"
#include "src/tools/remote/src/main/cpp/output_service/inmemory_fs.h"

struct Build {
  bool valid;
  std::string workspace_id;
  std::string build_id;
  std::string output_path;
};

struct Workspace {
  std::string workspace_id;
  std::string mount_point;
  FileSystem *fs;
  std::string active_build_id;
};

class FuseRemoteOutputService final
    : public remote_output_service::RemoteOutputService::Service {
 public:
  FuseRemoteOutputService(std::string disk_cache) : disk_cache_(disk_cache) {}

 private:
  grpc::Status Clean(grpc::ServerContext *context,
                     const remote_output_service::CleanRequest *request,
                     google::protobuf::Empty *response) override;

  grpc::Status StartBuild(
      grpc::ServerContext *context,
      const remote_output_service::StartBuildRequest *request,
      remote_output_service::StartBuildResponse *response) override;

  grpc::Status BatchCreate(
      grpc::ServerContext *context,
      const remote_output_service::BatchCreateRequest *request,
      google::protobuf::Empty *response) override;

  std::string disk_cache_;
  std::unordered_map<std::string, Workspace> workspaces_;
  std::unordered_map<std::string, Build> builds_;
};

#endif  // BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_FUSE_H_
