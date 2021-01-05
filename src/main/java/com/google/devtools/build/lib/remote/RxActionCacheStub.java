package com.google.devtools.build.lib.remote;

import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheStub;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import io.grpc.ClientCall;
import io.reactivex.rxjava3.core.Single;

public class RxActionCacheStub {
  private final Single<ActionCacheStub> actionCacheStubSingle;

  public RxActionCacheStub(Single<ActionCacheStub> actionCacheStubSingle) {
    this.actionCacheStubSingle = actionCacheStubSingle;
  }

  public Single<ActionResult> getActionResult(Single<GetActionResultRequest> requestSingle) {
    Single<ClientCall<GetActionResultRequest, ActionResult>> clientCallSingle =
        actionCacheStubSingle.map(
            stub ->
                stub.getChannel()
                    .newCall(ActionCacheGrpc.getGetActionResultMethod(), stub.getCallOptions()));
    return RxClientCalls.rxUnaryCall(clientCallSingle, requestSingle);
  }

  public Single<ActionResult> updateActionResult(Single<UpdateActionResultRequest> requestSingle) {
    return RxClientCalls.rxUnaryCall(
        actionCacheStubSingle.map(
            stub ->
                stub.getChannel()
                    .newCall(ActionCacheGrpc.getUpdateActionResultMethod(), stub.getCallOptions())),
        requestSingle);
  }
}
