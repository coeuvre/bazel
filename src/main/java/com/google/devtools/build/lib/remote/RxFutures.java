package com.google.devtools.build.lib.remote;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableObserver;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.core.SingleObserver;
import io.reactivex.rxjava3.disposables.Disposable;

public class RxFutures {
  private RxFutures() {}

  public static ListenableFuture<Void> toListenableFuture(Completable completable) {
    SettableFuture<Void> future = SettableFuture.create();
    completable.subscribe(new CompletableObserver() {
      @Override
      public void onSubscribe(@NonNull Disposable d) {
      }

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

  public static <T> ListenableFuture<T> toListenableFuture(Single<T> single) {
    SettableFuture<T> future = SettableFuture.create();
    single.subscribe(new SingleObserver<T>() {
      @Override
      public void onSubscribe(@NonNull Disposable d) {
      }

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
}
