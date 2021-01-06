package com.google.devtools.build.lib.remote.util;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class RxFutures {

  private RxFutures() {}

  public static ListenableFuture<Void> toListenableFuture(
      Completable completable, Executor executor) {
    SettableFuture<Void> future = SettableFuture.create();
    completable
        .subscribeOn(Schedulers.from(executor))
        .subscribe(
            new CompletableObserver() {
              @Override
              public void onSubscribe(@NonNull Disposable d) {}

              @Override
              public void onComplete() {
                future.set(null);
              }

              @Override
              public void onError(@NonNull Throwable e) {
                future.setException(e);
              }
            });
    return future;
  }

  public static <T> ListenableFuture<T> toListenableFuture(Single<T> single, Executor executor) {
    SettableFuture<T> future = SettableFuture.create();
    single
        .subscribeOn(Schedulers.from(executor))
        .subscribe(
            new SingleObserver<T>() {
              @Override
              public void onSubscribe(@NonNull Disposable d) {}

              @Override
              public void onSuccess(@NonNull T value) {
                future.set(value);
              }

              @Override
              public void onError(@NonNull Throwable e) {
                future.setException(e);
              }
            });
    return future;
  }

  public static <T> ListenableFuture<T> toListenableFuture(Maybe<T> maybe, Executor executor) {
    SettableFuture<T> future = SettableFuture.create();
    maybe
        .subscribeOn(Schedulers.from(executor))
        .subscribe(
            new MaybeObserver<T>() {
              @Override
              public void onSubscribe(@NonNull Disposable d) {}

              @Override
              public void onSuccess(@NonNull T t) {
                future.set(t);
              }

              @Override
              public void onError(@NonNull Throwable e) {
                future.setException(e);
              }

              @Override
              public void onComplete() {
                future.set(null);
              }
            });
    return future;
  }

  public static Completable toCompletable(
      Callable<ListenableFuture<Void>> futureSupplier, Executor executor) {
    AtomicBoolean subscribed = new AtomicBoolean(false);
    return Completable.create(
        emitter -> {
          boolean wasSubscribed = subscribed.getAndSet(true);
          Preconditions.checkState(
              !wasSubscribed, "This Completable cannot be subscribed to twice");
          ListenableFuture<Void> future = futureSupplier.call();
          Futures.addCallback(
              future,
              new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void v) {
                  emitter.onComplete();
                }

                @Override
                public void onFailure(Throwable t) {
                  emitter.onError(t);
                }
              },
              executor);
          emitter.setCancellable(() -> future.cancel(false));
        });
  }

  public static <T> Maybe<T> toMaybe(
      Callable<ListenableFuture<T>> futureSupplier, Executor executor) {
    AtomicBoolean subscribed = new AtomicBoolean(false);
    return Maybe.create(
        emitter -> {
          boolean wasSubscribed = subscribed.getAndSet(true);
          Preconditions.checkState(!wasSubscribed, "This Maybe cannot be subscribed to twice");
          ListenableFuture<T> future = futureSupplier.call();
          Futures.addCallback(
              future,
              new FutureCallback<T>() {
                @Override
                public void onSuccess(T v) {
                  if (v != null) {
                    emitter.onSuccess(v);
                  } else {
                    emitter.onComplete();
                  }
                }

                @Override
                public void onFailure(Throwable t) {
                  emitter.onError(t);
                }
              },
              executor);
          emitter.setCancellable(() -> future.cancel(false));
        });
  }

  public static <T> Single<T> toSingle(
      Callable<ListenableFuture<T>> futureSupplier, Executor executor) {
    AtomicBoolean subscribed = new AtomicBoolean(false);
    return Single.create(
        emitter -> {
          boolean wasSubscribed = subscribed.getAndSet(true);
          Preconditions.checkState(!wasSubscribed, "This Single cannot be subscribed to twice");
          ListenableFuture<T> future = futureSupplier.call();
          Futures.addCallback(
              future,
              new FutureCallback<T>() {
                @Override
                public void onSuccess(T v) {
                  emitter.onSuccess(v);
                }

                @Override
                public void onFailure(Throwable t) {
                  emitter.onError(t);
                }
              },
              executor);
          emitter.setCancellable(() -> future.cancel(false));
        });
  }
}
