package com.google.devtools.build.lib.remote;

import com.google.devtools.build.lib.authandtls.AuthAndTLSOptions;
import com.google.devtools.build.lib.remote.grpc.ChannelConnectionFactory;
import io.grpc.ClientInterceptor;
import io.reactivex.rxjava3.core.Single;

import java.util.List;

public class GoogleChannelConnectionFactory implements ChannelConnectionFactory {
  private final ChannelFactory channelFactory;
  private final String target;
  private final String proxy;
  private final AuthAndTLSOptions options;
  private final List<ClientInterceptor> interceptors;
  private final int maxConcurrency;

  public GoogleChannelConnectionFactory(
      ChannelFactory channelFactory,
      String target,
      String proxy,
      AuthAndTLSOptions options,
      List<ClientInterceptor> interceptors,
      int maxConcurrency) {
    this.channelFactory = channelFactory;
    this.target = target;
    this.proxy = proxy;
    this.options = options;
    this.interceptors = interceptors;
    this.maxConcurrency = maxConcurrency;
  }

  @Override
  public Single<ChannelConnection> create() {
    return Single.fromCallable(
        () ->
            new ChannelConnection(channelFactory.newChannel(target, proxy, options, interceptors)));
  }

  @Override
  public int maxConcurrency() {
    return maxConcurrency;
  }
}
