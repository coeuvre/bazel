cc_binary(
    name = "output_service",
    srcs = [
        "common.h",
        "copy.cc",
        "copy.h",
        "fuse.cc",
        "fuse.h",
        "inmemory_fs.cc",
        "inmemory_fs.h",
        "main.cc",
    ],
    deps = [
        "//src/main/protobuf:remote_output_service_cc_grpc",
        "//third_party/grpc:grpc++_unsecure",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/flags:parse",
        "@abseil-cpp//absl/strings",
        "@libfuse",
    ],
)
