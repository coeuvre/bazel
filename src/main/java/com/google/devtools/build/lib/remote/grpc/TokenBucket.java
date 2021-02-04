package com.google.devtools.build.lib.remote.grpc;

import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;

/** A container for tokens which is used for rate limiting. */
@ThreadSafe
public class TokenBucket<T> implements Closeable {
  private final ConcurrentLinkedDeque<T> tokens;

  private final BehaviorSubject<T> tokenBehaviorSubject = BehaviorSubject.create();

  public TokenBucket() {
    tokens = new ConcurrentLinkedDeque<>();
  }

  public TokenBucket(Collection<T> initialTokens) {
    tokens = new ConcurrentLinkedDeque<>(initialTokens);

    if (!tokens.isEmpty()) {
      tokenBehaviorSubject.onNext(tokens.getFirst());
    }
  }

  /** Add a token to the bucket. */
  public void addToken(T token) {
    tokens.addLast(token);
    tokenBehaviorSubject.onNext(token);
  }

  /** Returns current number of tokens in the bucket. */
  public int size() {
    return tokens.size();
  }

  /**
   * Returns a cold {@link Single} which will start the token acquisition process upon subscription.
   */
  public Single<T> acquireToken() {
    return Single.create(
        downstream ->
            tokenBehaviorSubject.subscribe(
                new Observer<T>() {
                  Disposable upstream;

                  @Override
                  public void onSubscribe(@NonNull Disposable d) {
                    upstream = d;
                    downstream.setDisposable(d);
                  }

                  @Override
                  public void onNext(@NonNull T ignored) {
                    if (!downstream.isDisposed()) {
                      T token = tokens.pollFirst();
                      if (token != null) {
                        downstream.onSuccess(token);
                      }
                    }
                  }

                  @Override
                  public void onError(@NonNull Throwable e) {
                    downstream.onError(new IllegalStateException(e));
                  }

                  @Override
                  public void onComplete() {
                    if (!downstream.isDisposed()) {
                      downstream.onError(new IllegalStateException("closed"));
                    }
                  }
                }));
  }

  /**
   * Closes the bucket and release all the tokens.
   *
   * <p>Subscriptions after closed to the Single returned by {@link TokenBucket#acquireToken()} will
   * emit error.
   */
  @Override
  public void close() throws IOException {
    tokens.clear();
    tokenBehaviorSubject.onComplete();
  }
}
