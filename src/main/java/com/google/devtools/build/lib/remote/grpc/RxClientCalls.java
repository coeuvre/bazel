package com.google.devtools.build.lib.remote.grpc;

import io.grpc.ClientCall;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.subscribers.DisposableSubscriber;
import java.util.concurrent.atomic.AtomicBoolean;

/** Utility methods for Rx. with gRPC. */
public class RxClientCalls {

  private RxClientCalls() {}

  /**
   * Returns a {@link Single} which will initiate the client streaming gRPC call on subscription and
   * emit either a response message from server or an error.
   *
   * <p>The {@link Single} is *cold* which means no request will be made until subscription. A
   * re-subscription triggers a new call.
   *
   * <p>When the {@link Single} is disposed and the underlying RPC hasn't terminated, {@link
   * ClientCall#cancel(String, Throwable)} will be called, the upstream is also disposed.
   *
   * @param clientCallSingle a {@link Single} which will return the {@link ClientCall} on
   *     subscription.
   * @param requestFlowable a {@link Flowable} which will return a stream of request messages of
   *     type {@link ReqT} on subscription.
   * @param <ReqT> type of message sent one or more times to the server.
   * @param <RespT> type of message received one or more times from the server.
   */
  public static <ReqT, RespT> Single<RespT> clientStreamingCall(
      Single<ClientCall<ReqT, RespT>> clientCallSingle, Flowable<ReqT> requestFlowable) {
    return clientCallSingle.flatMap(
        clientCall ->
            Observable.<RespT>create(
                    emitter ->
                        ClientCalls.asyncClientStreamingCall(
                            clientCall,
                            new ClientStreamingCallResponseObserver<>(requestFlowable, emitter)))
                .singleOrError());
  }

  private static class FlowableRequestSubscriber<ReqT> extends DisposableSubscriber<ReqT> {
    private final ClientCallStreamObserver<ReqT> requestStream;
    private volatile boolean terminated;

    FlowableRequestSubscriber(ClientCallStreamObserver<ReqT> requestStream) {
      this.requestStream = requestStream;
    }

    @Override
    protected void onStart() {
      // no-op
    }

    @Override
    public void onNext(ReqT value) {
      requestStream.onNext(value);
      requestOne();
    }

    @Override
    public void onError(Throwable throwable) {
      terminated = true;
      requestStream.onError(throwable);
    }

    @Override
    public void onComplete() {
      terminated = true;
      requestStream.onCompleted();
    }

    void onReady() {
      requestOne();
    }

    void requestOne() {
      if (!terminated && !isDisposed() && requestStream.isReady()) {
        request(1);
      }
    }
  }

  private static class ClientStreamingCallResponseObserver<ReqT, RespT>
      implements ClientResponseObserver<ReqT, RespT> {
    private final Flowable<ReqT> requestFlowable;
    private final ObservableEmitter<RespT> emitter;
    private volatile boolean terminated;
    private volatile boolean cancelled;

    private ClientStreamingCallResponseObserver(
        Flowable<ReqT> requestFlowable, ObservableEmitter<RespT> emitter) {
      this.requestFlowable = requestFlowable;
      this.emitter = emitter;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<ReqT> requestStream) {
      if (emitter.isDisposed()) {
        requestStream.cancel("disposed", null);
        return;
      }

      FlowableRequestSubscriber<ReqT> subscriber = new FlowableRequestSubscriber<>(requestStream);
      emitter.setCancellable(() -> {
        if (!subscriber.terminated) {
          subscriber.dispose();
        }

        if (!terminated) {
          cancelled = true;
          requestStream.cancel("disposed", null);
        }
      });

      AtomicBoolean subscribed = new AtomicBoolean(false);
      requestStream.setOnReadyHandler(
          () -> {
            if (!subscribed.getAndSet(true)) {
              requestFlowable.subscribe(subscriber);
            }

            subscriber.onReady();
          });
    }

    @Override
    public void onNext(RespT value) {
      emitter.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
      terminated = true;

      if (t instanceof StatusRuntimeException) {
        Status status = ((StatusRuntimeException) t).getStatus();
        // In case of a client-side error, propagate the cause.
        if (status.getCause() != null) {
          t = status.getCause();
        }
      }

      if (!cancelled) {
        emitter.onError(t);
      }
    }

    @Override
    public void onCompleted() {
      terminated = true;
      emitter.onComplete();
    }
  }
}
