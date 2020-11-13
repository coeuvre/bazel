package com.google.devtools.build.lib.remote;

import io.grpc.ClientCall;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.annotations.CheckReturnValue;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;

public class RxClientCalls {

  private RxClientCalls() {
  }

  /**
   * Returns a {@link Single} which will initiate the unary gRPC call on subscription and emit
   * either a response message from server or an error.
   *
   * <p>The {@link Single} is *cold* which means no request will be made until subscription.
   * A resubscription triggers a new call.
   *
   * <p>When the stream is disposed and the underlying RPC hasn't terminated,
   * {@link ClientCall#cancel(String, Throwable)} will be called.
   *
   * @param clientCallSingle a {@link Single} which will return the {@link ClientCall} on
   * subscription.
   * @param requestSingle a {@link Single} which will return the request message of type
   * {@link ReqT} on subscription.
   * @param <ReqT> type of message sent one or more times to the server.
   * @param <RespT> type of message received one or more times from the server.
   */
  @CheckReturnValue
  public static <ReqT, RespT> Single<RespT> rxUnaryCall(
      Single<ClientCall<ReqT, RespT>> clientCallSingle, Single<ReqT> requestSingle) {
    return clientCallSingle
        .flatMap(clientCall -> requestSingle.flatMap(request -> rxUnaryCall(clientCall, request)));
  }

  private static <ReqT, RespT> Single<RespT> rxUnaryCall(ClientCall<ReqT, RespT> clientCall,
      ReqT request) {
    Observable<RespT> responseObservable = Observable.create(emitter -> {
      StreamObserverEmitter<RespT> responseObserver = new StreamObserverEmitter<>(clientCall,
          emitter);
      ClientCalls.asyncUnaryCall(clientCall, request, responseObserver);
    });

    return responseObservable
        .singleOrError();
  }

  @CheckReturnValue
  public static <ReqT, RespT> Single<RespT> rxClientStreamingCall(
      Single<ClientCall<ReqT, RespT>> clientCallSingle, Observable<ReqT> requestObservable) {
    return clientCallSingle
        .flatMap(clientCall -> rxClientStreamingCall(clientCall, requestObservable));
  }

  private static <ReqT, RespT> Single<RespT> rxClientStreamingCall(
      ClientCall<ReqT, RespT> clientCall, Observable<ReqT> requestObservable) {
    Observable<RespT> responseObservable = Observable.create(emitter -> {
      StreamObserverEmitter<RespT> responseObserver = new StreamObserverEmitter<>(clientCall,
          emitter);
      StreamObserver<ReqT> requestObserver = ClientCalls
          .asyncClientStreamingCall(clientCall, responseObserver);
      requestObservable.subscribe(new StreamObserverDelegate<>(requestObserver));
    });

    return responseObservable
        .singleOrError();
  }

  private static class StreamObserverDelegate<T> implements Observer<T> {

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

  private static class StreamObserverEmitter<T> implements StreamObserver<T>, Disposable {

    private final ObservableEmitter<T> emitter;

    private boolean disposed;
    private boolean terminated;

    private StreamObserverEmitter(ClientCall<?, T> clientCall, ObservableEmitter<T> emitter) {
      this.emitter = emitter;

      emitter.setCancellable(() -> {
        dispose();

        if (!isTerminated()) {
          clientCall.cancel(/* message */ "disposed", /* cause */ null);
        }
      });
    }

    @Override
    public void dispose() {
      this.disposed = true;
    }

    @Override
    public boolean isDisposed() {
      return disposed;
    }

    @Override
    public void onNext(T value) {
      if (!isDisposed()) {
        emitter.onNext(value);
      }
    }

    @Override
    public void onError(Throwable t) {
      terminated = true;

      if (!isDisposed()) {
        emitter.onError(t);
      }
    }

    @Override
    public void onCompleted() {
      terminated = true;

      if (!isDisposed()) {
        emitter.onComplete();
      }
    }

    public boolean isTerminated() {
      return terminated;
    }
  }
}
