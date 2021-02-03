package com.google.devtools.build.lib.remote.grpc;

import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.core.SingleEmitter;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;

@ThreadSafe
public class TokenBucket<T> implements Closeable {
  @GuardedBy("this")
  private final Deque<T> tokens;

  private final PublishSubject<T> tokenSubject;

  public TokenBucket() {
    this(Collections.emptyList());
  }

  public TokenBucket(Collection<T> initialTokens) {
    tokens = new ArrayDeque<>(initialTokens);
    tokenSubject = PublishSubject.create();
  }

  public void addToken(T token) {
    synchronized (this) {
      tokens.addLast(token);
    }

    tokenSubject.onNext(token);
  }

  public synchronized int size() {
    return tokens.size();
  }

  public Single<T> acquireToken() {
    return Single.create(
        downstream -> {
          if (maybeEmitFirst(downstream)) {
            return;
          }

          tokenSubject.subscribe(
              new Observer<T>() {
                Disposable upstream;

                @Override
                public void onSubscribe(@NonNull Disposable d) {
                  upstream = d;
                  downstream.setDisposable(d);
                }

                @Override
                public void onNext(@NonNull T t) {
                  maybeEmitFirst(downstream);
                }

                @Override
                public void onError(@NonNull Throwable e) {
                  downstream.onError(e);
                }

                @Override
                public void onComplete() {
                  downstream.onError(new IllegalStateException("closed"));
                }
              });
        });
  }

  private synchronized @Nullable T takeFirst() {
    if (!tokens.isEmpty()) {
      return tokens.removeFirst();
    }
    return null;
  }

  private boolean maybeEmitFirst(SingleEmitter<T> emitter) {
    T token = takeFirst();
    if (token != null && !emitter.isDisposed()) {
      emitter.onSuccess(token);
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    tokenSubject.onComplete();
  }
}
