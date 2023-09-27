#include "src/main/protobuf/remote_output_service.grpc.pb.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"

#include "grpcpp/grpcpp.h"
#include "grpcpp/health_check_service_interface.h"

#include <fstream>
#include <string_view>
#include <sys/stat.h>

using ::build::bazel::remote::execution::v2::Digest;
using ::google::protobuf::Empty;
using ::grpc::Server;
using ::grpc::ServerBuilder;
using ::grpc::ServerContext;
using ::grpc::Status;
using ::grpc::StatusCode;
using ::remote_output_service::BatchCreateRequest;
using ::remote_output_service::CleanRequest;
using ::remote_output_service::RemoteOutputService;
using ::remote_output_service::StartBuildRequest;
using ::remote_output_service::StartBuildResponse;

ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");
ABSL_FLAG(std::string, disk_cache, "", "Location of the disk cache");

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

static bool CreateFile(const std::string &output_path,
                       const std::string &disk_cache, const std::string &path,
                       const Digest &digest, int mode) {

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

class RemoteOutputServiceImpl final : public RemoteOutputService::Service {
public:
  RemoteOutputServiceImpl(std::string disk_cache) : disk_cache_(disk_cache) {}

private:
  Status Clean(ServerContext *context, const CleanRequest *request,
               Empty *response) {
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
    output_path_ = request->output_path();
    return Status::OK;
  }

  Status BatchCreate(ServerContext *context, const BatchCreateRequest *request,
                     Empty *response) override {
    std::cerr << "BatchCreate" << std::endl;
    for (auto &file : request->files()) {
      std::cerr << "    file: " << file.path() << std::endl;
      if (!CreateFile(output_path_, disk_cache_, file.path(), file.digest(),
                      file.mode())) {
        return Status(StatusCode::INTERNAL, "Failed to create file");
      }
    }
    for (auto &symlink : request->symlinks()) {
      std::cerr << "    symlink: " << symlink.path() << std::endl;
      if (!CreateSymlink(output_path_, symlink.path(), symlink.target_path())) {
        return Status(StatusCode::INTERNAL, "Failed to create file");
      }
    }
    for (auto &directory : request->directories()) {
      std::cerr << "    directory: " << directory.path() << std::endl;
    }
    return Status::OK;
  }

  std::string disk_cache_;
  std::string output_path_;
};

static void RunServer(uint16_t port, std::string disk_cache) {
  std::string server_address = absl::StrFormat("0.0.0.0:%d", port);

  RemoteOutputServiceImpl service(disk_cache);

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
  auto disk_cache = absl::GetFlag(FLAGS_disk_cache);

  if (disk_cache.empty()) {
    std::cerr << "--disk_cache must be set" << std::endl;
    return 1;
  }

  RunServer(absl::GetFlag(FLAGS_port), disk_cache);
  return 0;
}
