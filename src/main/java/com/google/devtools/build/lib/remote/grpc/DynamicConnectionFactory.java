package com.google.devtools.build.lib.remote.grpc;

import io.reactivex.rxjava3.core.Single;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;

public class DynamicConnectionFactory implements PooledConnectionFactory {
  private final ConnectionFactory connectionFactory;
  private final int maxConnectionsPerChannel;

  @GuardedBy("this")
  private final ArrayList<RateLimitedConnectionFactory> factories;

  @GuardedBy("this")
  private int indexTicker = 0;

  public DynamicConnectionFactory(
      ConnectionFactory connectionFactory, int maxConnectionsPerChannel) {
    this.connectionFactory = connectionFactory;
    this.maxConnectionsPerChannel = maxConnectionsPerChannel;
    this.factories = new ArrayList<>();
  }

  public int numAvailableConnections() {
    int result = 0;
    synchronized (this) {
      for (RateLimitedConnectionFactory factory : factories) {
        result += factory.numAvailableConnections();
      }
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      for (RateLimitedConnectionFactory factory : factories) {
        factory.close();
      }
      factories.clear();
    }
  }

  private synchronized RateLimitedConnectionFactory nextAvailableFactory() {
    for (int times = 0; times < factories.size(); ++times) {
      int index = Math.abs(indexTicker % factories.size());
      indexTicker += 1;

      RateLimitedConnectionFactory factory = factories.get(index);
      if (factory.numAvailableConnections() > 0) {
        return factory;
      }
    }

    RateLimitedConnectionFactory factory =
        new RateLimitedConnectionFactory(connectionFactory, maxConnectionsPerChannel);
    factories.add(factory);
    return factory;
  }

  @Override
  public Single<? extends Connection> create() {
    return Single.defer(
        () -> {
          RateLimitedConnectionFactory factory = nextAvailableFactory();
          return factory.create();
        });
  }
}
