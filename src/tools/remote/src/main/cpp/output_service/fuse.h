#ifndef BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_FUSE_H_
#define BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_FUSE_H_

#include <string>

#include "src/main/protobuf/remote_output_service.grpc.pb.h"

class FuseRemoteOutputService final : public remote_output_service::RemoteOutputService::Service {
public:
  FuseRemoteOutputService(std::string disk_cache) : disk_cache_(disk_cache) {}

private:
  grpc::Status Clean(grpc::ServerContext *context,
                     const remote_output_service::CleanRequest *request,
                     google::protobuf::Empty *response) override;

  grpc::Status
  StartBuild(grpc::ServerContext *context,
             const remote_output_service::StartBuildRequest *request,
             remote_output_service::StartBuildResponse *response) override;

  grpc::Status
  BatchCreate(grpc::ServerContext *context,
              const remote_output_service::BatchCreateRequest *request,
              google::protobuf::Empty *response) override;

  std::string disk_cache_;
  std::string output_path_;
};

#endif // BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_FUSE_H_
