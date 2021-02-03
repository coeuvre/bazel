package com.google.devtools.build.lib.remote.grpc;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;

import java.io.Closeable;
import java.io.IOException;

/**
 * A single connection to a server. RPCs are executed within the context of a connection. A {@link
 * Connection} object can consist of any number of transport connections.
 */
public interface Connection extends Closeable {

  /** Creates a new {@link ClientCall} for issuing RPC. */
  <ReqT, RespT> ClientCall<ReqT, RespT> call(
      MethodDescriptor<ReqT, RespT> method, CallOptions options);

  /** Releases any resources held by the {@link Connection}. */
  @Override
  void close() throws IOException;
}
