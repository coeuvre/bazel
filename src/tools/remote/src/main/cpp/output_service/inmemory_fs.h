#ifndef BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_INMEMORY_FS_H_
#define BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_INMEMORY_FS_H_

struct Node;
struct FileSystem;

FileSystem *CreateFileSystem();
void DeleteFileSystem(FileSystem *fs);

int Mount(FileSystem *fs, const char *mount_point);
void Unmount(FileSystem *fs);

#endif  // BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_INMEMORY_FS_H_
