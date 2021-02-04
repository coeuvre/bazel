package com.google.devtools.build.lib.remote.grpc;

import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.AsyncSubject;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class SharedConnectionFactory implements ConnectionPool {
  private final TokenBucket<Integer> tokenBucket;
  private final ConnectionFactory factory;

  @GuardedBy("connectionLock")
  private @Nullable AsyncSubject<Connection> connectionAsyncSubject = null;
  private final ReentrantLock connectionLock = new ReentrantLock();
  private final AtomicReference<Disposable> connectionCreationDisposable =
      new AtomicReference<>(null);

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

    Disposable d = connectionCreationDisposable.getAndSet(null);
    if (d != null && !d.isDisposed()) {
      d.dispose();
    }

    try {
      connectionLock.lockInterruptibly();

      if (connectionAsyncSubject != null) {
        Connection connection = connectionAsyncSubject.getValue();
        if (connection != null) {
          connection.close();
        }

        if (!connectionAsyncSubject.hasComplete()) {
          connectionAsyncSubject.onError(new IllegalStateException("closed"));
        }
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      connectionLock.unlock();
    }
  }

  private AsyncSubject<Connection> createUnderlyingConnectionIfNot() throws InterruptedException {
    connectionLock.lockInterruptibly();
    try {
      if (connectionAsyncSubject == null || connectionAsyncSubject.hasThrowable()) {
        connectionAsyncSubject =
            factory
                .create()
                .doOnSubscribe(connectionCreationDisposable::set)
                .toObservable()
                .subscribeWith(AsyncSubject.create());
      }

      return connectionAsyncSubject;
    } finally {
      connectionLock.unlock();
    }
  }

  private Single<? extends Connection> acquireConnection() {
    return Single.fromCallable(this::createUnderlyingConnectionIfNot)
        .flatMap(Single::fromObservable);
  }

  @Override
  public Single<SharedConnection> create() {
    return tokenBucket
        .acquireToken()
        .flatMap(
            token ->
                acquireConnection()
                    .doOnError(ignored -> tokenBucket.addToken(token))
                    .doOnDispose(() -> tokenBucket.addToken(token))
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
