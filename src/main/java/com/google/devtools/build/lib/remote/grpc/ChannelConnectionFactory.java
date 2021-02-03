package com.google.devtools.build.lib.remote.grpc;

import com.google.devtools.build.lib.authandtls.GoogleAuthUtils;
import io.grpc.*;
import io.reactivex.rxjava3.core.Single;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ChannelConnectionFactory implements ConnectionFactory {
  private final ConnectionFactoryOptions options;

  public ChannelConnectionFactory(ConnectionFactoryOptions options) {
    this.options = options;
  }

  @Override
  public Single<? extends ChannelConnection> create() {
    List<ClientInterceptor> interceptors = options.interceptors();
    return Single.fromCallable(
        () -> {
          ManagedChannel channel =
              GoogleAuthUtils.newChannel(
                  options.target(),
                  options.proxy(),
                  options.options(),
                  interceptors.isEmpty() ? null : interceptors);
          return new ChannelConnection(channel);
        });
  }

  public static class ChannelConnection implements Connection {
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
  }
}
