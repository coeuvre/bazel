package com.google.devtools.build.lib.remote;

import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageStub;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import io.reactivex.rxjava3.core.Single;

public class RxContentAddressableStorageStub {
  private final Single<ContentAddressableStorageStub> contentAddressableStorageStubSingle;

  public RxContentAddressableStorageStub(
      Single<ContentAddressableStorageStub> contentAddressableStorageStubSingle) {
    this.contentAddressableStorageStubSingle = contentAddressableStorageStubSingle;
  }

  public Single<FindMissingBlobsResponse> findMissingBlobs(
      Single<FindMissingBlobsRequest> requestSingle) {
    return RxClientCalls.rxUnaryCall(
        contentAddressableStorageStubSingle.map(
            stub ->
                stub.getChannel()
                    .newCall(
                        ContentAddressableStorageGrpc.getFindMissingBlobsMethod(),
                        stub.getCallOptions())),
        requestSingle);
  }
}
