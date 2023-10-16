cc_binary(
    name = "output_service",
    srcs = [
        "copy.cc",
        "copy.h",
        "fuse.cc",
        "fuse.h",
        "main.cc",
    ],
    deps = [
        "//src/main/protobuf:remote_output_service_cc_grpc",
        "//third_party/grpc:grpc++_unsecure",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
    ],
)
