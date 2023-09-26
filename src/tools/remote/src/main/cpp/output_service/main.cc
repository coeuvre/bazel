#include "src/main/protobuf/remote_output_service.grpc.pb.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using remote_output_service::CleanRequest;
using remote_output_service::RemoteOutputService;
using remote_output_service::StartBuildRequest;
using remote_output_service::StartBuildResponse;

ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

class RemoteOutputServiceImpl final : public RemoteOutputService::Service {
  Status Clean(ServerContext *context, const CleanRequest *request,
               ::google::protobuf::Empty *response) {
    std::cerr << "Clean" << std::endl
              << "  workspace_id = " << request->workspace_id() << std::endl;
    return Status::OK;
  }

  Status StartBuild(ServerContext *context, const StartBuildRequest *request,
                    StartBuildResponse *response) override {
    std::cerr << "StartBuild" << std::endl
              << "  workspace_id = " << request->workspace_id() << std::endl
              << "  build_id = " << request->build_id() << std::endl
              << "  output_path = " << request->output_path() << std::endl;
    return Status::OK;
  }
};

static void RunServer(uint16_t port) {
  std::string server_address = absl::StrFormat("0.0.0.0:%d", port);

  RemoteOutputServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cerr << "Server listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  RunServer(absl::GetFlag(FLAGS_port));
  return 0;
}
