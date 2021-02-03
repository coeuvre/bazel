load("@rules_java//java:defs.bzl", "java_test")

package(
    default_testonly = 1,
    default_visibility = ["//src:__subpackages__"],
)

filegroup(
    name = "srcs",
    testonly = 0,
    srcs = glob(["**"]),
    visibility = ["//src/test/java/com/google/devtools/build/lib/remote:__pkg__"],
)

java_test(
    name = "grpc",
    srcs = glob(["*.java"]),
    test_class = "com.google.devtools.build.lib.AllTests",
    tags = [
        "requires-network",
    ],
    deps = [
        "//src/main/java/com/google/devtools/build/lib/bazel/repository/cache",
        "//src/main/java/com/google/devtools/build/lib/bazel/repository/downloader",
        "//src/main/java/com/google/devtools/build/lib/events",
        "//src/main/java/com/google/devtools/build/lib/remote:ReferenceCountedChannel",
        "//src/main/java/com/google/devtools/build/lib/remote:Retrier",
        "//src/main/java/com/google/devtools/build/lib/remote/grpc",
        "//src/main/java/com/google/devtools/build/lib/remote/options",
        "//src/main/java/com/google/devtools/build/lib/remote/util",
        "//src/test/java/com/google/devtools/build/lib:test_runner",
        "//third_party:guava",
        "//third_party:junit4",
        "//third_party:mockito",
        "//third_party:truth",
        "//third_party:rxjava3",
        "//third_party/grpc:grpc-jar",
        "//third_party/protobuf:protobuf_java",
        "@remoteapis//:build_bazel_remote_asset_v1_remote_asset_java_grpc",
        "@remoteapis//:build_bazel_remote_asset_v1_remote_asset_java_proto",
        "@remoteapis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)
