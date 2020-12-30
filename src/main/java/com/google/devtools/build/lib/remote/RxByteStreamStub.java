package com.google.devtools.build.lib.remote;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import io.grpc.ClientCall;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;

public class RxByteStreamStub {
  private final Single<ByteStreamStub> byteStreamStubSingle;

  public RxByteStreamStub(
      Single<ByteStreamStub> byteStreamStubSingle) {
    this.byteStreamStubSingle = byteStreamStubSingle;
  }

  public Single<QueryWriteStatusResponse> queryWriteStatus(
      Single<QueryWriteStatusRequest> requestSingle) {
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle =
        byteStreamStubSingle.map(stub -> stub.getChannel()
            .newCall(ByteStreamGrpc.getQueryWriteStatusMethod(), stub.getCallOptions()));
    return RxClientCalls.rxUnaryCall(clientCallSingle, requestSingle);
  }

  public Single<WriteResponse> write(Flowable<WriteRequest> requestObservable) {
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle =
        byteStreamStubSingle.map(stub -> stub.getChannel()
            .newCall(ByteStreamGrpc.getWriteMethod(), stub.getCallOptions()));
    return RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);
  }
}
