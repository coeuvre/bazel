package com.google.devtools.build.lib.remote.grpc;

import io.reactivex.rxjava3.core.Single;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;

public class DynamicConnectionPool implements ConnectionPool {
  private final ConnectionFactory connectionFactory;
  private final int maxConcurrencyPerConnection;

  @GuardedBy("this")
  private final ArrayList<SharedConnectionFactory> factories;

  @GuardedBy("this")
  private int indexTicker = 0;

  public DynamicConnectionPool(
      ConnectionFactory connectionFactory, int maxConcurrencyPerConnection) {
    this.connectionFactory = connectionFactory;
    this.maxConcurrencyPerConnection = maxConcurrencyPerConnection;
    this.factories = new ArrayList<>();
  }

  public int numAvailableConnections() {
    int result = 0;
    synchronized (this) {
      for (SharedConnectionFactory factory : factories) {
        result += factory.numAvailableConnections();
      }
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      for (SharedConnectionFactory factory : factories) {
        factory.close();
      }
      factories.clear();
    }
  }

  private synchronized SharedConnectionFactory nextAvailableFactory() {
    for (int times = 0; times < factories.size(); ++times) {
      int index = Math.abs(indexTicker % factories.size());
      indexTicker += 1;

      SharedConnectionFactory factory = factories.get(index);
      if (factory.numAvailableConnections() > 0) {
        return factory;
      }
    }

    SharedConnectionFactory factory =
        new SharedConnectionFactory(connectionFactory, maxConcurrencyPerConnection);
    factories.add(factory);
    return factory;
  }

  @Override
  public Single<? extends Connection> create() {
    return Single.defer(
        () -> {
          SharedConnectionFactory factory = nextAvailableFactory();
          return factory.create();
        });
  }
}
