package com.google.devtools.build.lib.remote;

import io.grpc.ClientCall;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.annotations.CheckReturnValue;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import java.util.function.Consumer;

public class RxClientCalls {

  private RxClientCalls() {}

  @CheckReturnValue
  public static <ReqT, RespT> Single<RespT> rxUnaryCall(
      Single<ClientCall<ReqT, RespT>> clientCallSingle, Single<ReqT> requestSingle) {
    return clientCallSingle.flatMap(clientCall -> requestSingle.flatMapObservable(
        request -> RxClientCalls.<RespT>responseObservable((responseObserver) -> {
          ClientCalls.asyncUnaryCall(clientCall, request, responseObserver);
        })).singleOrError());
  }

  @CheckReturnValue
  public static <ReqT, RespT> Single<RespT> rxClientStreamingCall(
      Single<ClientCall<ReqT, RespT>> clientCallSingle, Observable<ReqT> requestObservable) {
    return clientCallSingle
        .flatMap(clientCall -> RxClientCalls.<RespT>responseObservable((responseObserver) -> {
          StreamObserver<ReqT> requestObserver = ClientCalls
              .asyncClientStreamingCall(clientCall, responseObserver);
          requestObservable.subscribe(new StreamObserverDelegate<>(requestObserver));
        }).singleOrError());
  }

  public static class StreamObserverDelegate<T> implements Observer<T> {

    private final StreamObserver<T> streamObserver;

    public StreamObserverDelegate(StreamObserver<T> streamObserver) {
      this.streamObserver = streamObserver;
    }

    @Override
    public void onSubscribe(@NonNull Disposable d) {
    }

    @Override
    public void onNext(@NonNull T t) {
      streamObserver.onNext(t);
    }

    @Override
    public void onError(@NonNull Throwable e) {
      streamObserver.onError(e);
    }

    @Override
    public void onComplete() {
      streamObserver.onCompleted();
    }
  }

  private static <T> Observable<T> responseObservable(Consumer<StreamObserver<T>> consumer) {
    return Observable.create(emitter -> consumer.accept(new StreamObserver<T>() {
      @Override
      public void onNext(T value) {
        emitter.onNext(value);
      }

      @Override
      public void onError(Throwable error) {
        emitter.onError(error);
      }

      @Override
      public void onCompleted() {
        emitter.onComplete();
      }
    }));
  }
}
