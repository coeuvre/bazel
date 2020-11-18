package com.google.devtools.build.lib.remote;

import static com.google.common.base.Preconditions.checkState;

import io.grpc.ClientCall;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.annotations.CheckReturnValue;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.annotations.Nullable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import java.util.concurrent.atomic.AtomicBoolean;
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
      ResponseObservableEmitter<ReqT, RespT> responseObserver = new ResponseObservableEmitter<>(
          emitter,
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

      ResponseObservableEmitter<ReqT, RespT> responseObserver = new ResponseObservableEmitter<>(
          emitter,
          isUpstreamTerminated -> {
            RequestObserver<ReqT, RespT> requestObserver = requestObserverRef.get();
            checkState(requestObserver != null);
            requestObserver.stopObserveUpstream();

            if (!isUpstreamTerminated) {
              clientCall.cancel(/* message */"disposed", /* cause */ null);
            }
          });

      RequestObserver<ReqT, RespT> requestObserver = new RequestObserver<>(responseObserver);

      AtomicBoolean subscribed = new AtomicBoolean(false);
      responseObserver.setBeforeStartHandler(requestStreamObserver -> {
        requestStreamObserver.setOnReadyHandler(() -> {
          if (!subscribed.getAndSet(true)) {
            // Only subscribe request-upstream when the RPC stream is ready
            requestObserver
                .startObserverUpstreamAndDelegate(requestObservable, requestStreamObserver);
          }
        });
      });
      requestObserverRef.set(requestObserver);

      // Starting the call
      ClientCalls.asyncClientStreamingCall(clientCall, responseObserver);
    });

    return responseObservable.singleOrError();
  }

  /**
   * A {@link Observer} which delegates request messages from upstream to {@link StreamObserver}.
   */
  private static class RequestObserver<ReqT, RespT> implements Observer<ReqT> {

    @Nullable
    private StreamObserver<ReqT> requestStreamObserver;
    private final ResponseObservableEmitter<ReqT, RespT> responseObservableEmitter;

    private Disposable upstreamDisposable;
    private boolean isUpstreamTerminated;

    public RequestObserver(
        ResponseObservableEmitter<ReqT, RespT> responseObservableEmitter) {
      this.responseObservableEmitter = responseObservableEmitter;
    }

    public void startObserverUpstreamAndDelegate(Observable<ReqT> requestObservable,
        StreamObserver<ReqT> requestStreamObserver) {
      this.requestStreamObserver = requestStreamObserver;
      requestObservable.subscribe(this);
    }

    public void stopObserveUpstream() {
      if (!isUpstreamTerminated && upstreamDisposable != null && !upstreamDisposable.isDisposed()) {
        upstreamDisposable.dispose();
      }
    }

    @Override
    public void onSubscribe(@NonNull Disposable d) {
      this.upstreamDisposable = d;
    }

    @Override
    public void onNext(@NonNull ReqT t) {
      checkState(requestStreamObserver != null);
      requestStreamObserver.onNext(t);
    }

    @Override
    public void onError(@NonNull Throwable e) {
      checkState(requestStreamObserver != null);

      isUpstreamTerminated = true;

      // Early propagate the error to downstream observer
      responseObservableEmitter.onError(e);

      // Send error the server and cancel the call
      requestStreamObserver.onError(e);
    }

    @Override
    public void onComplete() {
      checkState(requestStreamObserver != null);

      isUpstreamTerminated = true;

      requestStreamObserver.onCompleted();
    }
  }

  /**
   * A {@link StreamObserver} which emits response messages from upstream to downstream observer
   * using {@link ObservableEmitter}.
   */
  private static class ResponseObservableEmitter<ReqT, RespT> implements
      ClientResponseObserver<ReqT, RespT> {

    private final ObservableEmitter<RespT> emitter;

    private Consumer<ClientCallStreamObserver<ReqT>> beforeStartHandler;
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

    public void setBeforeStartHandler(Consumer<ClientCallStreamObserver<ReqT>> beforeStartHandler) {
      this.beforeStartHandler = beforeStartHandler;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<ReqT> requestStream) {
      if (beforeStartHandler != null) {
        try {
          beforeStartHandler.accept(requestStream);
        } catch (Throwable t) {
          onError(t);
        }
      }
    }
  }
}
