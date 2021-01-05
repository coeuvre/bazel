package com.google.devtools.build.lib.remote;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

public class RxByteStreamStub {
  private final Single<ByteStreamStub> byteStreamStubSingle;

  public RxByteStreamStub(Single<ByteStreamStub> byteStreamStubSingle) {
    this.byteStreamStubSingle = byteStreamStubSingle;
  }

  public Single<QueryWriteStatusResponse> queryWriteStatus(
      Single<QueryWriteStatusRequest> requestSingle) {
    return RxClientCalls.rxUnaryCall(
        byteStreamStubSingle.map(
            stub ->
                stub.getChannel()
                    .newCall(ByteStreamGrpc.getQueryWriteStatusMethod(), stub.getCallOptions())),
        requestSingle);
  }

  public Single<WriteResponse> write(Flowable<WriteRequest> requestObservable) {
    return RxClientCalls.rxClientStreamingCall(
        byteStreamStubSingle.map(
            stub ->
                stub.getChannel().newCall(ByteStreamGrpc.getWriteMethod(), stub.getCallOptions())),
        requestObservable);
  }

  public Flowable<ReadResponse> read(Single<ReadRequest> requestSingle) {
    return RxClientCalls.rxServerStreamingCall(
        byteStreamStubSingle.map(
            stub ->
                stub.getChannel().newCall(ByteStreamGrpc.getReadMethod(), stub.getCallOptions())),
        requestSingle);
  }
}
