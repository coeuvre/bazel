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
import io.reactivex.rxjava3.functions.Cancellable;
import io.reactivex.rxjava3.functions.Consumer;
import java.util.concurrent.atomic.AtomicReference;

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
   * @param requestSingle a {@link Single} which will return the request message of type {@link
   * ReqT} on subscription.
   * @param <ReqT> type of message sent one time to the server.
   * @param <RespT> type of message received one time from the server.
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
      ResponseObservableEmitter<RespT> responseObserver = new ResponseObservableEmitter<>(emitter,
          isUpstreamTerminated -> {
            if (!isUpstreamTerminated) {
              clientCall.cancel(/* message */"disposed", /* cause */ null);
            }
          });
      ClientCalls.asyncUnaryCall(clientCall, request, responseObserver);
    });

    return responseObservable.singleOrError();
  }

  /**
   * Returns a {@link Single} which will initiate the client streaming gRPC call on subscription and
   * emit either a response message from server or an error.
   *
   * <p>The {@link Single} is *cold* which means no request will be made until subscription.
   * A resubscription triggers a new call.
   *
   * <p>When the stream is disposed and the underlying RPC hasn't terminated,
   * {@link ClientCall#cancel(String, Throwable)} will be called.
   *
   * @param clientCallSingle a {@link Single} which will return the {@link ClientCall} on
   * subscription.
   * @param requestObservable a {@link Observable} which will return a stream of request messages of
   * type {@link ReqT} on subscription.
   * @param <ReqT> type of message sent one or more times to the server.
   * @param <RespT> type of message received one or more times from the server.
   */
  @CheckReturnValue
  public static <ReqT, RespT> Single<RespT> rxClientStreamingCall(
      Single<ClientCall<ReqT, RespT>> clientCallSingle, Observable<ReqT> requestObservable) {
    return clientCallSingle
        .flatMap(clientCall -> rxClientStreamingCall(clientCall, requestObservable));
  }

  private static <ReqT, RespT> Single<RespT> rxClientStreamingCall(
      ClientCall<ReqT, RespT> clientCall, Observable<ReqT> requestObservable) {
    Observable<RespT> responseObservable = Observable.create(emitter -> {
      final AtomicReference<RequestObserver<ReqT, RespT>> requestObserverRef =
          new AtomicReference<>(null);

      ResponseObservableEmitter<RespT> responseObserver = new ResponseObservableEmitter<>(emitter,
          isUpstreamTerminated -> {
            RequestObserver<ReqT, RespT> requestObserver;
            // Get requestObserver with a spinlock
            do {
              requestObserver = requestObserverRef.get();
            } while (requestObserver == null);

            requestObserver.stopObserveUpstream();

            if (!isUpstreamTerminated) {
              clientCall.cancel(/* message */"disposed", /* cause */ null);
            }
          });

      RequestObserver<ReqT, RespT> requestObserver = new RequestObserver<>(
          ClientCalls.asyncClientStreamingCall(clientCall, responseObserver), responseObserver);
      requestObserverRef.set(requestObserver);
      requestObservable.subscribe(requestObserverRef.get());
    });

    return responseObservable.singleOrError();
  }

  /**
   * A {@link Observer} which delegates request messages from upstream to {@link StreamObserver}.
   */
  private static class RequestObserver<ReqT, RespT> implements Observer<ReqT> {

    private final StreamObserver<ReqT> requestStreamObserver;
    private final ResponseObservableEmitter<RespT> responseObservableEmitter;

    private Disposable disposable;
    private boolean isUpstreamTerminated;

    public RequestObserver(StreamObserver<ReqT> requestStreamObserver,
        ResponseObservableEmitter<RespT> responseObservableEmitter) {
      this.requestStreamObserver = requestStreamObserver;
      this.responseObservableEmitter = responseObservableEmitter;
    }

    public void stopObserveUpstream() {
      if (!isUpstreamTerminated && disposable != null && !disposable.isDisposed()) {
        disposable.dispose();
      }
    }

    @Override
    public void onSubscribe(@NonNull Disposable d) {
      this.disposable = d;
    }

    @Override
    public void onNext(@NonNull ReqT t) {
      requestStreamObserver.onNext(t);
    }

    @Override
    public void onError(@NonNull Throwable e) {
      isUpstreamTerminated = true;

      // Propagate the error to downstream observer
      responseObservableEmitter.onError(e);

      // Send error the server and cancel the call
      requestStreamObserver.onError(e);
    }

    @Override
    public void onComplete() {
      isUpstreamTerminated = true;

      requestStreamObserver.onCompleted();
    }
  }

  /**
   * A {@link StreamObserver} which emits response messages from upstream to downstream observer
   * using {@link ObservableEmitter}.
   */
  private static class ResponseObservableEmitter<RespT> implements StreamObserver<RespT> {

    private final ObservableEmitter<RespT> emitter;

    private boolean isUpstreamTerminated;
    private boolean isDownstreamDisposed;

    private ResponseObservableEmitter(ObservableEmitter<RespT> emitter,
        Consumer<Boolean> cancellable) {
      this.emitter = emitter;

      emitter.setCancellable(() -> {
        stopEmitToDownstream();

        cancellable.accept(isUpstreamTerminated);
      });
    }

    /* Stop emitting to downstream since it is disposed */
    private void stopEmitToDownstream() {
      isDownstreamDisposed = true;
    }

    private boolean shouldEmitToDownstream() {
      return !isDownstreamDisposed;
    }

    @Override
    public void onNext(RespT value) {
      if (shouldEmitToDownstream()) {
        emitter.onNext(value);
      }
    }

    @Override
    public void onError(Throwable t) {
      isUpstreamTerminated = true;

      if (shouldEmitToDownstream()) {
        emitter.onError(t);
        stopEmitToDownstream();
      }
    }

    @Override
    public void onCompleted() {
      isUpstreamTerminated = true;

      if (shouldEmitToDownstream()) {
        emitter.onComplete();
        stopEmitToDownstream();
      }
    }
  }
}
