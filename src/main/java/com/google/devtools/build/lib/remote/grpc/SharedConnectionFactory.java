package com.google.devtools.build.lib.remote.grpc;

import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Single;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class SharedConnectionFactory implements ConnectionPool {
  private final TokenBucket<Integer> tokenBucket;
  private final ConnectionFactory factory;

  private final Lock connectionLock = new ReentrantLock();

  @GuardedBy("connectionLock")
  private @Nullable Connection connection = null;

  public SharedConnectionFactory(ConnectionFactory factory, int maxConcurrency) {
    this.factory = factory;

    List<Integer> initialTokens = new ArrayList<>(maxConcurrency);
    for (int i = 0; i < maxConcurrency; ++i) {
      initialTokens.add(i);
    }
    this.tokenBucket = new TokenBucket<>(initialTokens);
  }

  @Override
  public void close() throws IOException {
    tokenBucket.close();

    connectionLock.lock();
    try {
      if (connection != null) {
        connection.close();
        connection = null;
      }
    } finally {
      connectionLock.unlock();
    }
  }

  @SuppressWarnings("GuardedBy")
  private Single<? extends Connection> acquireConnection() {
    return Single.using(
        () -> {
          connectionLock.lock();
          return connectionLock;
        },
        ignored -> {
          if (connection == null) {
            return factory.create().doOnSuccess(conn -> connection = conn);
          }

          return Single.just(connection);
        },
        Lock::unlock,
        /* eager= */ true);
  }

  @Override
  public Single<SharedConnection> create() {
    return tokenBucket
        .acquireToken()
        .flatMap(
            token ->
                acquireConnection()
                    .map(conn -> new SharedConnection(conn, () -> tokenBucket.addToken(token))));
  }

  public int numAvailableConnections() {
    return tokenBucket.size();
  }

  public static class SharedConnection implements Connection {
    private final Connection connection;
    private final Closeable onClose;

    public SharedConnection(Connection connection, Closeable onClose) {
      this.connection = connection;
      this.onClose = onClose;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> call(
        MethodDescriptor<ReqT, RespT> method, CallOptions options) {
      return connection.call(method, options);
    }

    @Override
    public void close() throws IOException {
      onClose.close();
    }

    public Connection getUnderlyingConnection() {
      return connection;
    }
  }
}
