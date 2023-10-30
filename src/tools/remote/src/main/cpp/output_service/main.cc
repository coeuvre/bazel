#include <sys/stat.h>

#include <fstream>
#include <string_view>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/health_check_service_interface.h"
#include "src/tools/remote/src/main/cpp/output_service/copy.h"
#include "src/tools/remote/src/main/cpp/output_service/fuse.h"

ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");
ABSL_FLAG(std::string, disk_cache, "", "Location of the disk cache");

static void RunServer(uint16_t port, std::string disk_cache) {
  std::string server_address = absl::StrFormat("0.0.0.0:%d", port);

  FuseRemoteOutputService service(disk_cache);

  grpc::EnableDefaultHealthCheckService(true);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cerr << "Server listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  auto disk_cache = absl::GetFlag(FLAGS_disk_cache);

  if (disk_cache.empty()) {
    std::cerr << "--disk_cache must be set" << std::endl;
    return 1;
  }

  RunServer(absl::GetFlag(FLAGS_port), disk_cache);
  return 0;
}
