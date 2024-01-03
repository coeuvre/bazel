#ifndef BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_INMEMORY_FS_H_
#define BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_INMEMORY_FS_H_

struct Node;
struct FileSystem;

FileSystem *Mount(const char *mount_point,
                  const char *unix_digest_hash_attribute_name);
void Unmount(FileSystem *fs);

void MaybeSetDigestHashToXAttr(FileSystem *fs, const char *path,
                                const char *hash);

#endif  // BAZEL_SRC_TOOLS_REMOTE_SRC_MAIN_CPP_OUTPUT_SERVICE_INMEMORY_FS_H_
