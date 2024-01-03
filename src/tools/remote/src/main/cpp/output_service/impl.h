#ifndef BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_IMPL_H_
#define BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_IMPL_H_

#include <mutex>
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
  std::string unix_digest_hash_attribute_name;
  FileSystem *fs;
  std::string active_build_id;
};

class RemoteOutputServiceImpl final
    : public remote_output_service::RemoteOutputService::Service {
 public:
  RemoteOutputServiceImpl(std::string disk_cache) : disk_cache_(disk_cache) {}

  void InstallSignalHandlers();
  void OnExit();

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

  grpc::Status FinalizeAction(
      grpc::ServerContext *context,
      const remote_output_service::FinalizeActionRequest *request,
      google::protobuf::Empty *response) override;

  grpc::Status BatchStat(
      grpc::ServerContext *context,
      const remote_output_service::BatchStatRequest *request,
      remote_output_service::BatchStatResponse *response) override;

  grpc::Status FinalizeBuild(
      grpc::ServerContext *context,
      const remote_output_service::FinalizeBuildRequest *request,
      google::protobuf::Empty *response) override;

  std::string disk_cache_;
  std::unordered_map<std::string, Workspace> workspaces_;
  std::unordered_map<std::string, Build> builds_;
  std::mutex mutex_;
};

#endif  // BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_IMPL_H_
