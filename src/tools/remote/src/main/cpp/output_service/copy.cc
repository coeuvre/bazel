#include "src/tools/remote/src/main/cpp/output_service/copy.h"

#include <fstream>
#include <sys/stat.h>

#include "absl/strings/str_split.h"

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

static bool
CreateFile(const std::string &output_path, const std::string &disk_cache,
           const std::string &path,
           const build::bazel::remote::execution::v2::Digest &digest,
           int mode) {
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

grpc::Status CopyRemoteOutputService::Clean(
    grpc::ServerContext *context,
    const remote_output_service::CleanRequest *request,
    google::protobuf::Empty *response) {
  std::cerr << "Clean" << std::endl
            << "  workspace_id = " << request->workspace_id() << std::endl;
  return grpc::Status::OK;
}

grpc::Status CopyRemoteOutputService::StartBuild(
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

grpc::Status CopyRemoteOutputService::BatchCreate(
    grpc::ServerContext *context,
    const remote_output_service::BatchCreateRequest *request,
    google::protobuf::Empty *response) {
  std::cerr << "BatchCreate" << std::endl;
  for (auto &file : request->files()) {
    std::cerr << "    file: " << file.path() << std::endl;
    if (!CreateFile(output_path_, disk_cache_, file.path(), file.digest(),
                    file.mode())) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to create file");
    }
  }
  for (auto &symlink : request->symlinks()) {
    std::cerr << "    symlink: " << symlink.path() << std::endl;
    if (!CreateSymlink(output_path_, symlink.path(), symlink.target_path())) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to create file");
    }
  }
  for (auto &directory : request->directories()) {
    std::cerr << "    directory: " << directory.path() << std::endl;
  }
  return grpc::Status::OK;
}
