package com.google.devtools.build.lib.remote.grpc;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.devtools.build.lib.remote.util.RxNoGlobalErrorsRule;
import com.google.protobuf.ByteString;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RxClientCalls}. */
@RunWith(JUnit4.class)
public class RxClientCallsTest {
  @Rule
  public final RxNoGlobalErrorsRule rxNoGlobalErrorsRule = new RxNoGlobalErrorsRule();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_1 =
      QueryWriteStatusRequest.newBuilder().setResourceName("request1").build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_2 =
      QueryWriteStatusRequest.newBuilder().setResourceName("request2").build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_3 =
      QueryWriteStatusRequest.newBuilder().setResourceName("request3").build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_4 =
      QueryWriteStatusRequest.newBuilder().setResourceName("request4").build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_5 =
      QueryWriteStatusRequest.newBuilder().setResourceName("request5").build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_6 =
      QueryWriteStatusRequest.newBuilder().setResourceName("request6").build();

  private static final QueryWriteStatusResponse QUERY_WRITE_STATUS_RESPONSE_1 =
      QueryWriteStatusResponse.newBuilder().setCommittedSize(1).setComplete(true).build();

  private static final WriteRequest[] WRITE_REQUESTS_1 =
      newWriteRequests("request1", "chunk1", "chunk2", "chunk3");

  private static final WriteRequest[] WRITE_REQUESTS_2 =
      newWriteRequests("request2", "chunk1", "chunk2");

  private static final WriteRequest[] WRITE_REQUESTS_3 =
      newWriteRequests("request3", "chunk1", "chunk2");

  private static final WriteRequest[] WRITE_REQUESTS_4 =
      newWriteRequests("request4", "chunk1", "chunk2");

  private static final WriteRequest[] WRITE_REQUESTS_5 =
      newWriteRequests("request5", "chunk1", "chunk2");

  private static final WriteRequest[] WRITE_REQUESTS_6 =
      newWriteRequests("request6", "chunk1", "chunk2");

  private static WriteRequest[] newWriteRequests(String resourceName, String... strings) {
    WriteRequest[] requests = new WriteRequest[strings.length];
    long offset = 0;
    for (int i = 0; i < requests.length; ++i) {
      String string = strings[i];
      ByteString data = ByteString.copyFrom(string, UTF_8);
      requests[i] =
          WriteRequest.newBuilder()
              .setResourceName(resourceName)
              .setWriteOffset(offset)
              .setFinishWrite(i + 1 == requests.length)
              .setData(data)
              .build();
      offset += data.size();
    }
    return requests;
  }

  private static WriteResponse newWriteResponse(long committedSize) {
    return WriteResponse.newBuilder().setCommittedSize(committedSize).build();
  }

  private static long totalSize(WriteRequest[] requests) {
    long result = 0;
    for (WriteRequest request : requests) {
      result += request.getData().size();
    }
    return result;
  }

  private final String fakeServerName = "fake server for " + getClass();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final AtomicInteger queryWriteStatusTimes = new AtomicInteger(0);
  private final AtomicInteger writeTimes = new AtomicInteger(0);
  private Server fakeServer;
  private ManagedChannel channel;

  @Before
  public final void setUp() throws Exception {
    // Use a mutable service registry for later registering the service impl for each test case.
    fakeServer =
        InProcessServerBuilder.forName(fakeServerName)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
            .build()
            .start();

    queryWriteStatusTimes.set(0);
    writeTimes.set(0);

    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public void queryWriteStatus(
              QueryWriteStatusRequest request,
              StreamObserver<QueryWriteStatusResponse> responseObserver) {
            queryWriteStatusTimes.addAndGet(1);

            if (request.equals(QUERY_WRITE_STATUS_REQUEST_1)) {
              responseObserver.onNext(QUERY_WRITE_STATUS_RESPONSE_1);
              responseObserver.onCompleted();
            } else if (request.equals(QUERY_WRITE_STATUS_REQUEST_2)) {
              responseObserver.onError(Status.NOT_FOUND.asException());
            } else if (request.equals(QUERY_WRITE_STATUS_REQUEST_3)) {
              throw new RuntimeException("error");
            } else if (request.equals(QUERY_WRITE_STATUS_REQUEST_4)) {
              responseObserver.onNext(QUERY_WRITE_STATUS_RESPONSE_1);
              responseObserver.onNext(QUERY_WRITE_STATUS_RESPONSE_1);
              responseObserver.onCompleted();
            } else if (request.equals(QUERY_WRITE_STATUS_REQUEST_5)) {
              responseObserver.onCompleted();
            } else if (request.equals(QUERY_WRITE_STATUS_REQUEST_6)) {
              responseObserver.onNext(QUERY_WRITE_STATUS_RESPONSE_1);
            } else {
              super.queryWriteStatus(request, responseObserver);
            }
          }

          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            writeTimes.addAndGet(1);

            return new StreamObserver<WriteRequest>() {
              @Override
              public void onNext(WriteRequest value) {
                if (value.getResourceName().equals(WRITE_REQUESTS_1[0].getResourceName())) {
                  if (value.getFinishWrite()) {
                    responseObserver.onNext(
                        newWriteResponse(value.getWriteOffset() + value.getData().size()));
                    responseObserver.onCompleted();
                  }
                } else if (value.getResourceName().equals(WRITE_REQUESTS_2[0].getResourceName())) {
                  responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
                } else if (value.getResourceName().equals(WRITE_REQUESTS_3[0].getResourceName())) {
                  throw new RuntimeException("error");
                } else if (value.getResourceName().equals(WRITE_REQUESTS_4[0].getResourceName())) {
                  if (value.getFinishWrite()) {
                    responseObserver.onNext(
                        newWriteResponse(value.getWriteOffset() + value.getData().size()));
                    responseObserver.onNext(
                        newWriteResponse(value.getWriteOffset() + value.getData().size()));
                    responseObserver.onCompleted();
                  }
                } else if (value.getResourceName().equals(WRITE_REQUESTS_5[0].getResourceName())) {
                  if (value.getFinishWrite()) {
                    responseObserver.onCompleted();
                  }
                } else if (value.getResourceName().equals(WRITE_REQUESTS_6[0].getResourceName())) {
                  if (value.getFinishWrite()) {
                    responseObserver.onNext(
                        newWriteResponse(value.getWriteOffset() + value.getData().size()));
                  }
                } else {
                  responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
                }
              }

              @Override
              public void onError(Throwable t) {
                responseObserver.onError(t);
              }

              @Override
              public void onCompleted() {}
            };
          }
        });

    channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
  }

  @After
  public void tearDown() throws Throwable {
    channel.shutdown();

    fakeServer.shutdownNow();
    fakeServer.awaitTermination();
  }

  public static class ClientCallDelegate<ReqT, RespT> extends ClientCall<ReqT, RespT> {

    private final ClientCall<ReqT, RespT> delegate;
    private int cancelTimes = 0;

    public ClientCallDelegate(ClientCall<ReqT, RespT> delegate) {
      this.delegate = delegate;
    }

    public int getCancelTimes() {
      return cancelTimes;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      delegate.start(responseListener, headers);
    }

    @Override
    public void request(int numMessages) {
      delegate.request(numMessages);
    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
      ++cancelTimes;
      delegate.cancel(message, cause);
    }

    @Override
    public void halfClose() {
      delegate.halfClose();
    }

    @Override
    public void sendMessage(ReqT message) {
      delegate.sendMessage(message);
    }

    @Override
    public boolean isReady() {
      return delegate.isReady();
    }

    @Override
    public void setMessageCompression(boolean enabled) {
      delegate.setMessageCompression(enabled);
    }

    @Override
    public Attributes getAttributes() {
      return delegate.getAttributes();
    }
  }

  private ClientCallDelegate<WriteRequest, WriteResponse> newWriteClientCall() {
    return new ClientCallDelegate<>(
        channel.newCall(ByteStreamGrpc.getWriteMethod(), CallOptions.DEFAULT));
  }

  @Test
  public void clientStreamingCall_smoke() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Flowable<WriteRequest> requestFlowable = Flowable.fromArray(WRITE_REQUESTS_1);

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle
        .test()
        .assertValue(newWriteResponse(totalSize(WRITE_REQUESTS_1)))
        .assertComplete();
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @SuppressWarnings("CheckReturnValue")
  @Test
  public void clientStreamingCall_noSubscription() {
    AtomicInteger clientCallSingleTimes = new AtomicInteger(0);
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle =
        Single.fromCallable(
            () -> {
              clientCallSingleTimes.addAndGet(1);
              return newWriteClientCall();
            });
    AtomicInteger requestFlowableTimes = new AtomicInteger(0);
    Flowable<WriteRequest> requestFlowable =
        Flowable.create(
            emitter -> {
              requestFlowableTimes.addAndGet(1);
              for (WriteRequest request : WRITE_REQUESTS_1) {
                emitter.onNext(request);
              }
              emitter.onComplete();
            },
            BackpressureStrategy.BUFFER);

    RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    assertThat(clientCallSingleTimes.get()).isEqualTo(0);
    assertThat(requestFlowableTimes.get()).isEqualTo(0);
    assertThat(writeTimes.get()).isEqualTo(0);
  }

  @Test
  public void clientStreamingCall_multipleSubscriptions() {
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle =
        Single.fromCallable(this::newWriteClientCall);
    Flowable<WriteRequest> requestFlowable = Flowable.fromArray(WRITE_REQUESTS_1);

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle.blockingSubscribe();
    responseSingle.blockingSubscribe();
    assertThat(writeTimes.get()).isEqualTo(2);
  }

  @Test
  public void clientStreamingCall_clientCallSingleOnError() {
    Exception error = new IOException("test error");
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.error(error);
    Flowable<WriteRequest> requestFlowable = Flowable.fromArray(WRITE_REQUESTS_1);

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle.test().assertError(error.getClass());
    assertThat(writeTimes.get()).isEqualTo(0);
  }

  @Test
  public void clientStreamingCall_requestFlowableOnError_firstIsError() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Exception error = new IOException("test error");
    Flowable<WriteRequest> requestFlowable = Flowable.error(error);

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle.test().assertError(error.getClass());
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(1);
  }

  @Test
  public void clientStreamingCall_requestFlowableOnError_errorAfterFirst() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Exception error = new IOException("test error");
    Flowable<WriteRequest> requestFlowable =
        Flowable.create(
            emitter -> {
              emitter.onNext(WRITE_REQUESTS_1[0]);
              emitter.onError(error);
            },
            BackpressureStrategy.BUFFER);

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle.test().assertError(error.getClass());
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(1);
  }

  @Test
  public void clientStreamingCall_serverReturnsError() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Flowable<WriteRequest> requestFlowable = Flowable.fromArray(WRITE_REQUESTS_2);

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle
        .test()
        .assertError(
            e -> {
              assertThat(e).isInstanceOf(StatusRuntimeException.class);
              Status status = Status.fromThrowable(e);
              assertThat(status.getCode()).isEqualTo(Code.NOT_FOUND);
              return true;
            });
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void clientStreamingCall_serverThrowsRuntimeException() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Flowable<WriteRequest> requestFlowable = Flowable.fromArray(WRITE_REQUESTS_3);

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle
        .test()
        .assertError(
            e -> {
              assertThat(e).isInstanceOf(StatusRuntimeException.class);
              Status status = Status.fromThrowable(e);
              assertThat(status.getCode()).isEqualTo(Code.UNKNOWN);
              return true;
            });
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void clientStreamingCall_serverCompletedWithMultipleValues() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Flowable<WriteRequest> requestFlowable = Flowable.fromArray(WRITE_REQUESTS_4);

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle
        .test()
        .assertError(
            e -> {
              assertThat(e).isInstanceOf(StatusRuntimeException.class);
              Status status = Status.fromThrowable(e);
              assertThat(status.getCode()).isEqualTo(Code.CANCELLED);
              return true;
            });
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void clientStreamingCall_serverCompletedWithoutValue() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Flowable<WriteRequest> requestFlowable = Flowable.fromArray(WRITE_REQUESTS_5);

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle
        .test()
        .assertError(
            e -> {
              assertThat(e).isInstanceOf(StatusRuntimeException.class);
              Status status = Status.fromThrowable(e);
              assertThat(status.getCode()).isEqualTo(Code.CANCELLED);
              return true;
            });
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void clientStreamingCall_serverReturnsValueNotCompleted() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Flowable<WriteRequest> requestFlowable = Flowable.fromArray(WRITE_REQUESTS_6);

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle.test().assertNoValues().assertNoErrors().assertNotComplete();
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void clientStreamingCall_dispose_cancelCall() throws Exception {
    AtomicBoolean requestDisposed = new AtomicBoolean(false);
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Flowable<WriteRequest> requestFlowable =
        Flowable.<WriteRequest>create(
                emitter -> {
                  for (WriteRequest request : WRITE_REQUESTS_6) {
                    emitter.onNext(request);
                  }
                },
                BackpressureStrategy.BUFFER)
            .doOnCancel(
                () -> requestDisposed.set(true));

    RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable).test().dispose();

    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(1);
    assertThat(requestDisposed.get()).isTrue();
  }

  @Test
  public void clientStreamingCall_serverReturnErrorsBeforeRequestComplete_disposeRequest() {
    AtomicBoolean requestDisposed = new AtomicBoolean(false);
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Flowable<WriteRequest> requestFlowable =
        Flowable.<WriteRequest>create(
                emitter -> {
                  for (WriteRequest request : WRITE_REQUESTS_2) {
                    emitter.onNext(request);
                  }
                },
                BackpressureStrategy.BUFFER)
            .doOnCancel(
                () -> requestDisposed.set(true));

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle
        .test()
        .assertError(
            e -> {
              assertThat(e).isInstanceOf(StatusRuntimeException.class);
              Status status = Status.fromThrowable(e);
              assertThat(status.getCode()).isEqualTo(Code.NOT_FOUND);
              return true;
            });
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
    assertThat(requestDisposed.get()).isTrue();
  }

  @Test
  public void clientStreamingCall_serverCompleteBeforeRequestComplete_disposeRequest() {
    AtomicBoolean requestDisposed = new AtomicBoolean(false);
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Flowable<WriteRequest> requestFlowable =
        Flowable.<WriteRequest>create(
                emitter -> {
                  for (WriteRequest request : WRITE_REQUESTS_1) {
                    emitter.onNext(request);
                  }
                },
                BackpressureStrategy.BUFFER)
            .doOnCancel(
                () -> requestDisposed.set(true));

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle
        .test()
        .assertValue(newWriteResponse(totalSize(WRITE_REQUESTS_1)))
        .assertComplete();
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
    assertThat(requestDisposed.get()).isTrue();
  }

  @Test
  public void clientStreamingCall_serverCompleteBeforeRequestSubscription_requestNotSubscribed() {
    AtomicBoolean requestSubscribed = new AtomicBoolean(false);
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall = newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single.just(clientCall);
    Flowable<WriteRequest> requestFlowable =
        Flowable.<WriteRequest>create(
                emitter -> {
                  for (WriteRequest request : WRITE_REQUESTS_1) {
                    emitter.onNext(request);
                  }
                },
                BackpressureStrategy.BUFFER)
            .doOnSubscribe(
                d -> requestSubscribed.set(true));
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            writeTimes.getAndIncrement();
            responseObserver.onNext(
                WriteResponse.newBuilder().setCommittedSize(totalSize(WRITE_REQUESTS_1)).build());
            responseObserver.onCompleted();
            return new StreamObserver<WriteRequest>() {
              @Override
              public void onNext(WriteRequest writeRequest) {}

              @Override
              public void onError(Throwable throwable) {}

              @Override
              public void onCompleted() {}
            };
          }
        });

    Single<WriteResponse> responseSingle =
        RxClientCalls.clientStreamingCall(clientCallSingle, requestFlowable);

    responseSingle
        .test()
        .assertValue(newWriteResponse(totalSize(WRITE_REQUESTS_1)))
        .assertComplete();
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
    assertThat(requestSubscribed.get()).isFalse();
  }
}
