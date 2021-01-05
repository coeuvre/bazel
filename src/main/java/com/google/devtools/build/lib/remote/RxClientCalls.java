package com.google.devtools.build.lib.remote;

import static com.google.common.base.Preconditions.checkState;

import io.grpc.ClientCall;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.annotations.CheckReturnValue;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.subscribers.DisposableSubscriber;
import org.reactivestreams.Subscriber;

import javax.annotation.Nullable;
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
   * @param requestFlowable a {@link Flowable} which will return a stream of request messages of
   * type {@link ReqT} on subscription.
   * @param <ReqT> type of message sent one or more times to the server.
   * @param <RespT> type of message received one or more times from the server.
   */
  @CheckReturnValue
  public static <ReqT, RespT> Single<RespT> rxClientStreamingCall(
      Single<ClientCall<ReqT, RespT>> clientCallSingle, Flowable<ReqT> requestFlowable) {
    return clientCallSingle
        .flatMap(clientCall -> rxClientStreamingCall(clientCall, requestFlowable));
  }

  private static <ReqT, RespT> Single<RespT> rxClientStreamingCall(
      ClientCall<ReqT, RespT> clientCall, Flowable<ReqT> requestFlowable) {
    Observable<RespT> responseObservable = Observable.create(emitter -> {
      final AtomicReference<RequestSubscriber<ReqT, RespT>> requestObserverRef =
          new AtomicReference<>(null);

      ResponseObservableEmitter<ReqT, RespT> responseObserver = new ResponseObservableEmitter<>(
          emitter,
          isUpstreamTerminated -> {
            RequestSubscriber<ReqT, RespT> requestSubscriber = requestObserverRef.get();
            checkState(requestSubscriber != null);
            requestSubscriber.stopObserveUpstream();

            if (!isUpstreamTerminated) {
              clientCall.cancel(/* message */"disposed", /* cause */ null);
            }
          });

      RequestSubscriber<ReqT, RespT> requestSubscriber = new RequestSubscriber<>(responseObserver);

      AtomicBoolean subscribed = new AtomicBoolean(false);
      responseObserver.setBeforeStartHandler(requestStreamObserver -> requestStreamObserver.setOnReadyHandler(() -> {
        if (!subscribed.getAndSet(true)) {
          // Only subscribe request-upstream when the RPC stream is ready
          requestSubscriber
              .subscribe(requestFlowable, requestStreamObserver);
        }
        requestSubscriber.onDownstreamReady();
      }));
      requestObserverRef.set(requestSubscriber);

      // Starting the call
      ClientCalls.asyncClientStreamingCall(clientCall, responseObserver);
    });

    return responseObservable.singleOrError();
  }

  public static <ReqT, RespT> Flowable<RespT> rxServerStreamingCall(
      Single<ClientCall<ReqT, RespT>> clientCallSingle, Single<ReqT> requestSingle) {
    return clientCallSingle.flatMapPublisher(
        clientCall ->
            requestSingle.flatMapPublisher(request -> rxServerStreamingCall(clientCall, request)));
  }

  private static <ReqT, RespT> Flowable<RespT> rxServerStreamingCall(
      ClientCall<ReqT, RespT> clientCall, ReqT request) {
    Observable<RespT> responseObservable = Observable.create(emitter -> {
      ResponseObservableEmitter<ReqT, RespT> responseObserver = new ResponseObservableEmitter<>(
          emitter,
          isUpstreamTerminated -> {
            if (!isUpstreamTerminated) {
              clientCall.cancel(/* message */"disposed", /* cause */ null);
            }
          });
      ClientCalls.asyncServerStreamingCall(clientCall, request, responseObserver);
    });

    return responseObservable.toFlowable(BackpressureStrategy.BUFFER);
  }

  /**
   * A {@link Subscriber} which delegates request messages from upstream to {@link StreamObserver}.
   */
  private static class RequestSubscriber<ReqT, RespT> extends DisposableSubscriber<ReqT> {

    @Nullable
    private ClientCallStreamObserver<ReqT> downstream;
    private final ResponseObservableEmitter<ReqT, RespT> emitter;

    private boolean isUpstreamTerminated;

    public RequestSubscriber(
        ResponseObservableEmitter<ReqT, RespT> emitter) {
      this.emitter = emitter;
    }

    public void subscribe(Flowable<ReqT> requestFlowable,
                          ClientCallStreamObserver<ReqT> requestStreamObserver) {
      this.downstream = requestStreamObserver;
      requestFlowable.subscribe(this);
    }

    public void stopObserveUpstream() {
      if (!isUpstreamTerminated && !isDisposed()) {
          dispose();
      }
    }

    public void onDownstreamReady() {
      while (!isUpstreamTerminated && downstream != null && downstream.isReady()) {
        request(1);
      }
    }

    @Override
    protected void onStart() {
      // no-op
    }

    @Override
    public void onNext(@NonNull ReqT t) {
      checkState(downstream != null);
      downstream.onNext(t);
    }

    @Override
    public void onError(@NonNull Throwable e) {
      checkState(downstream != null);

      isUpstreamTerminated = true;

      // Early propagate the error to downstream observer
      emitter.onError(e);

      // Send error to the server and cancel the call
      downstream.onError(e);
    }

    @Override
    public void onComplete() {
      checkState(downstream != null);

      isUpstreamTerminated = true;

      downstream.onCompleted();
    }
  }

  /**
   * A {@link StreamObserver} which emits response messages from upstream to downstream observer
   * using {@link ObservableEmitter}.
   */
  private static class ResponseObservableEmitter<ReqT, RespT> implements
      ClientResponseObserver<ReqT, RespT> {

    private final ObservableEmitter<RespT> downstream;

    private Consumer<ClientCallStreamObserver<ReqT>> beforeStartHandler;
    private boolean isUpstreamTerminated;
    private boolean isDownstreamDisposed;

    private ResponseObservableEmitter(ObservableEmitter<RespT> emitter,
        Consumer<Boolean> cancellable) {
      this.downstream = emitter;

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
        downstream.onNext(value);
      }
    }

    @Override
    public void onError(Throwable t) {
      isUpstreamTerminated = true;

      if (shouldEmitToDownstream()) {
        downstream.onError(t);
        stopEmitToDownstream();
      }
    }

    @Override
    public void onCompleted() {
      isUpstreamTerminated = true;

      if (shouldEmitToDownstream()) {
        downstream.onComplete();
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
