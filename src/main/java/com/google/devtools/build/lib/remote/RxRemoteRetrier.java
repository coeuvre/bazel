package com.google.devtools.build.lib.remote;

import com.google.devtools.build.lib.authandtls.CallCredentialsProvider;
import com.google.devtools.build.lib.remote.Retrier.Backoff;
import io.grpc.CallCredentials;
import io.reactivex.rxjava3.core.Flowable;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;

public class RxRemoteRetrier {

  private final Supplier<Backoff> backoffSupplier;
  private final Predicate<? super Throwable> shouldRetry;
  private final CallCredentialsProvider callCredentialsProvider;

  public RxRemoteRetrier(
      Supplier<Backoff> backoffSupplier,
      Predicate<? super Throwable> shouldRetry,
      CallCredentialsProvider callCredentialsProvider) {
    this.backoffSupplier = backoffSupplier;
    this.shouldRetry = shouldRetry;
    this.callCredentialsProvider = callCredentialsProvider;
  }

  public Publisher<Long> retryWhen(Flowable<Throwable> errors) {
    return retryWhen(errors, newBackoff());
  }

  /**
   * <p>In case of an {@link #isUnauthenticated(Throwable)} error, the {@link * CallCredentials}
   * will be refreshed with {@link CallCredentialsProvider#refresh()}.
   */
  public Publisher<Long> retryWhen(Flowable<Throwable> errors, Backoff backoff) {
    return errors.flatMap(error -> {
      boolean isUnauthenticated = isUnauthenticated(error);
      if (isUnauthenticated || isRetriable(error)) {
        long waitMillis = backoff.nextDelayMillis();
        if (waitMillis >= 0) {
          if (isUnauthenticated) {
            return Flowable.fromCallable(() -> {
              callCredentialsProvider.refresh();
              return 0L;
            });
          } else {
            // TODO: scheduler?
            return Flowable.timer(waitMillis, TimeUnit.MILLISECONDS);
          }
        }
      }

      return Flowable.error(error);
    });
  }

  public Backoff newBackoff() {
    return backoffSupplier.get();
  }

  public boolean isRetriable(Throwable t) {
    return shouldRetry.test(t);
  }

  public CallCredentials getCallCredentials() {
    return callCredentialsProvider.getCallCredentials();
  }

  public static boolean isUnauthenticated(Throwable t) {
    io.grpc.Status status = io.grpc.Status.fromThrowable(t);
    return (status != null
        && (status.getCode() == io.grpc.Status.Code.UNAUTHENTICATED
        || status.getCode() == io.grpc.Status.Code.PERMISSION_DENIED));
  }
}
