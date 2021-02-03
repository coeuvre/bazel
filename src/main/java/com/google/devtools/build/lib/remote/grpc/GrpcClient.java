package com.google.devtools.build.lib.remote.grpc;

import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

import java.io.Closeable;
import java.io.IOException;

public class GrpcClient implements Closeable {
  private final ConnectionPool factory;

  public static GrpcClient create(ConnectionPool factory) {
    return new GrpcClient(factory);
  }

  GrpcClient(ConnectionPool factory) {
    this.factory = factory;
  }

  public <ReqT, RespT> CallSpec<ReqT, RespT> call(MethodDescriptor<ReqT, RespT> method) {
    return new CallSpec<>(method);
  }

  @Override
  public void close() throws IOException {
    factory.close();
  }

  public class CallSpec<ReqT, RespT> {
    private final MethodDescriptor<ReqT, RespT> method;
    private Single<CallOptions> options = Single.just(CallOptions.DEFAULT);

    public CallSpec(MethodDescriptor<ReqT, RespT> method) {
      this.method = method;
    }

    public CallSpec<ReqT, RespT> options(CallOptions options) {
      this.options = Single.just(options);
      return this;
    }

    public CallSpec<ReqT, RespT> options(Single<CallOptions> options) {
      this.options = options;
      return this;
    }

    public Single<RespT> unary(Single<ReqT> request) {
      return factory
          .create()
          .flatMap(
              connection ->
                  Single.using(
                      () -> connection,
                      c ->
                          options.flatMap(o -> RxClientCalls.unaryCall(c.call(method, o), request)),
                      Connection::close,
                      /* eager= */ false));
    }

    public Single<RespT> clientStreaming(Flowable<ReqT> request) {
      return factory
          .create()
          .flatMap(
              connection ->
                  Single.using(
                      () -> connection,
                      c ->
                          options.flatMap(
                              o -> RxClientCalls.clientStreamingCall(c.call(method, o), request)),
                      Connection::close,
                      /* eager= */ false));
    }

    public Flowable<RespT> serverStreaming(Single<ReqT> request) {
      return factory
          .create()
          .flatMapPublisher(
              connection ->
                  Flowable.using(
                      () -> connection,
                      c ->
                          options.flatMapPublisher(
                              o -> RxClientCalls.serverStreamingCall(c.call(method, o), request)),
                      Connection::close,
                      /* eager= */ false));
    }
  }
}
