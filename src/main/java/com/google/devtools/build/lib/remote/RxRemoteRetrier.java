package com.google.devtools.build.lib.remote;

import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.devtools.build.lib.authandtls.CallCredentialsProvider;
import com.google.devtools.build.lib.remote.Retrier.Backoff;
import io.grpc.CallCredentials;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;

public class RxRemoteRetrier {

  private final GoogleLogger logger = GoogleLogger.forEnclosingClass();

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
   * <p>In case of an {@link #isUnauthenticated(Throwable)} error, the {@link CallCredentials}
   * will be refreshed with {@link CallCredentialsProvider#refresh()}.
   */
  public Publisher<Long> retryWhen(Flowable<Throwable> errors, Backoff backoff) {
    return Flowable.defer(() -> {
      AtomicBoolean isUnauthenticatedRef = new AtomicBoolean(false);
      return errors.flatMap(error -> {
        boolean isUnauthenticated = isUnauthenticated(error);
        boolean lastIsUnauthenticated = isUnauthenticatedRef.getAndSet(isUnauthenticated);
        if (isUnauthenticated) {
          // Don't retry if we see consecutive unauthenticated errors
          if (!lastIsUnauthenticated) {
            logger.atWarning().withCause(error)
                .log("Received unauthenticated error, refreshing CallCredentials");
            return Flowable.fromCallable(() -> {
              callCredentialsProvider.refresh();
              return 0L;
            });
          }
        } else if (isRetriable(error)) {
          long waitMillis = backoff.nextDelayMillis();
          if (waitMillis >= 0) {
            logger.atWarning().withCause(error).log("Received retriable error, retrying");
            return Flowable.timer(waitMillis, TimeUnit.MILLISECONDS,
                Schedulers.from(MoreExecutors.directExecutor()));
          }
        }
        return Flowable.error(error);
      });
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
