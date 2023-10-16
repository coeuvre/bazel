#include "src/tools/remote/src/main/cpp/output_service/fuse.h"

grpc::Status FuseRemoteOutputService::StartBuild(
    grpc::ServerContext *context,
    const remote_output_service::StartBuildRequest *request,
    remote_output_service::StartBuildResponse *response) {
  std::cerr << "StartBuild" << std::endl
            << "  workspace_id = " << request->workspace_id() << std::endl
            << "  build_id = " << request->build_id() << std::endl
            << "  output_path = " << request->output_path() << std::endl;
  output_path_ = request->output_path();
  return grpc::Status::OK;
}
