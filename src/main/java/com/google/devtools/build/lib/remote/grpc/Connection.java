package com.google.devtools.build.lib.remote.grpc;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;

import java.io.Closeable;
import java.io.IOException;

public interface Connection extends Closeable {
  <ReqT, RespT> ClientCall<ReqT, RespT> call(
      MethodDescriptor<ReqT, RespT> method, CallOptions options);

  @Override
  void close() throws IOException;
}
