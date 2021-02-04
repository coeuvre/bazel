package com.google.devtools.build.lib.remote.grpc;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Single;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public interface ChannelConnectionFactory extends ConnectionFactory {
  @Override
  Single<? extends ChannelConnection> create();

  int maxConcurrency();

  class ChannelConnection implements Connection {
    private final ManagedChannel channel;

    public ChannelConnection(ManagedChannel channel) {
      this.channel = channel;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> call(
        MethodDescriptor<ReqT, RespT> method, CallOptions options) {
      return channel.newCall(method, options);
    }

    @Override
    public void close() throws IOException {
      channel.shutdown();
      try {
        channel.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new IOException(e.getMessage(), e);
      }
    }

    public ManagedChannel getChannel() {
      return channel;
    }
  }
}
