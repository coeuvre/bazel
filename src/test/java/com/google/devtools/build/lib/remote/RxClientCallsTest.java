package com.google.devtools.build.lib.remote;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
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
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link RxClientCalls}.
 */
@RunWith(JUnit4.class)
public class RxClientCallsTest {

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_1 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request1")
      .build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_2 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request2")
      .build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_3 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request3")
      .build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_4 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request4")
      .build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_5 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request5")
      .build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_6 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request6")
      .build();

  private static final QueryWriteStatusResponse QUERY_WRITE_STATUS_RESPONSE_1 = QueryWriteStatusResponse
      .newBuilder()
      .setCommittedSize(1)
      .setComplete(true)
      .build();

  private static final WriteRequest[] WRITE_REQUESTS_1 = newWriteRequests("request1", "chunk1",
      "chunk2");

  private static final WriteRequest[] WRITE_REQUESTS_2 = newWriteRequests("request2", "chunk1",
      "chunk2");

  private static final WriteRequest[] WRITE_REQUESTS_3 = newWriteRequests("request3", "chunk1",
      "chunk2");

  private static final WriteRequest[] WRITE_REQUESTS_4 = newWriteRequests("request4", "chunk1",
      "chunk2");

  private static final WriteRequest[] WRITE_REQUESTS_5 = newWriteRequests("request5", "chunk1",
      "chunk2");

  private static final WriteRequest[] WRITE_REQUESTS_6 = newWriteRequests("request6", "chunk1",
      "chunk2");

  private static WriteRequest[] newWriteRequests(String resourceName, String... strings) {
    WriteRequest[] requests = new WriteRequest[strings.length];
    long offset = 0;
    for (int i = 0; i < requests.length; ++i) {
      String string = strings[i];
      ByteString data = ByteString.copyFrom(string, UTF_8);
      requests[i] = WriteRequest.newBuilder()
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
    return WriteResponse
        .newBuilder()
        .setCommittedSize(committedSize)
        .build();
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
  private final AtomicReference<Throwable> rxGlobalThrowable = new AtomicReference<>(null);

  @Before
  public final void setUp() throws Exception {
    RxJavaPlugins.setErrorHandler(rxGlobalThrowable::set);

    // Use a mutable service registry for later registering the service impl for each test case.
    fakeServer =
        InProcessServerBuilder.forName(fakeServerName)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
            .build()
            .start();

    queryWriteStatusTimes.set(0);
    writeTimes.set(0);

    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public void queryWriteStatus(QueryWriteStatusRequest request,
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
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> responseObserver) {
        writeTimes.addAndGet(1);

        return new StreamObserver<WriteRequest>() {
          @Override
          public void onNext(WriteRequest value) {
            if (value.getResourceName().equals(WRITE_REQUESTS_1[0].getResourceName())) {
              if (value.getFinishWrite()) {
                responseObserver
                    .onNext(newWriteResponse(value.getWriteOffset() + value.getData().size()));
                responseObserver.onCompleted();
              }
            } else if (value.getResourceName().equals(WRITE_REQUESTS_2[0].getResourceName())) {
              responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            } else if (value.getResourceName().equals(WRITE_REQUESTS_3[0].getResourceName())) {
              throw new RuntimeException("error");
            } else if (value.getResourceName().equals(WRITE_REQUESTS_4[0].getResourceName())) {
              if (value.getFinishWrite()) {
                responseObserver
                    .onNext(newWriteResponse(value.getWriteOffset() + value.getData().size()));
                responseObserver
                    .onNext(newWriteResponse(value.getWriteOffset() + value.getData().size()));
                responseObserver.onCompleted();
              }
            } else if (value.getResourceName().equals(WRITE_REQUESTS_5[0].getResourceName())) {
              if (value.getFinishWrite()) {
                responseObserver.onCompleted();
              }
            } else if (value.getResourceName().equals(WRITE_REQUESTS_6[0].getResourceName())) {
              if (value.getFinishWrite()) {
                responseObserver
                    .onNext(newWriteResponse(value.getWriteOffset() + value.getData().size()));
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
          public void onCompleted() {
          }
        };
      }
    });

    channel = InProcessChannelBuilder.forName(fakeServerName)
        .directExecutor()
        .build();
  }

  @After
  public void tearDown() throws Throwable {
    channel.shutdown();

    fakeServer.shutdownNow();
    fakeServer.awaitTermination();

    // Make sure rxjava didn't receive global errors
    Throwable t = rxGlobalThrowable.getAndSet(null);
    if (t != null) {
      throw t;
    }
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

  private ClientCallDelegate<QueryWriteStatusRequest, QueryWriteStatusResponse> newQueryWriteStatusClientCall() {
    return new ClientCallDelegate<>(
        channel.newCall(ByteStreamGrpc.getQueryWriteStatusMethod(), CallOptions.DEFAULT));
  }

  private ClientCallDelegate<WriteRequest, WriteResponse> newWriteClientCall() {
    return new ClientCallDelegate<>(
        channel.newCall(ByteStreamGrpc.getWriteMethod(), CallOptions.DEFAULT));
  }

  @Test
  public void rxUnaryCall_smoke() {
    ClientCallDelegate<QueryWriteStatusRequest, QueryWriteStatusResponse> clientCall =
        newQueryWriteStatusClientCall();
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .just(clientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_1);

    Single<QueryWriteStatusResponse> responseSingle =
        RxClientCalls.rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertValue(QUERY_WRITE_STATUS_RESPONSE_1)
        .assertComplete();
    assertThat(queryWriteStatusTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @SuppressWarnings("CheckReturnValue")
  @Test
  public void rxUnaryCall_noSubscription() {
    AtomicInteger clientCallSingleTimes = new AtomicInteger(0);
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .fromCallable(() -> {
          clientCallSingleTimes.addAndGet(1);
          return newQueryWriteStatusClientCall();
        });
    AtomicInteger requestSingleTimes = new AtomicInteger(0);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .fromCallable(() -> {
          requestSingleTimes.addAndGet(0);
          return QUERY_WRITE_STATUS_REQUEST_1;
        });

    RxClientCalls.rxUnaryCall(clientCallSingle, requestSingle);

    assertThat(clientCallSingleTimes.get()).isEqualTo(0);
    assertThat(requestSingleTimes.get()).isEqualTo(0);
    assertThat(queryWriteStatusTimes.get()).isEqualTo(0);
  }

  @Test
  public void rxUnaryCall_multipleSubscriptions() {
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .fromCallable(this::newQueryWriteStatusClientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_1);

    Single<QueryWriteStatusResponse> responseSingle =
        RxClientCalls.rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle.blockingSubscribe();
    responseSingle.blockingSubscribe();
    assertThat(queryWriteStatusTimes.get()).isEqualTo(2);
  }

  @Test
  public void rxUnaryCall_clientCallSingleOnError() {
    Exception error = new IOException("test error");
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .error(error);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_1);

    Single<QueryWriteStatusResponse> responseSingle = RxClientCalls
        .rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertError(error.getClass());
    assertThat(queryWriteStatusTimes.get()).isEqualTo(0);
  }

  @Test
  public void rxUnaryCall_requestSingleOnError() {
    ClientCallDelegate<QueryWriteStatusRequest, QueryWriteStatusResponse> clientCall =
        newQueryWriteStatusClientCall();
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .just(clientCall);
    Exception error = new IOException("test error");
    Single<QueryWriteStatusRequest> requestSingle = Single
        .error(error);

    Single<QueryWriteStatusResponse> responseSingle = RxClientCalls
        .rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertError(error.getClass());
    assertThat(queryWriteStatusTimes.get()).isEqualTo(0);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxUnaryCall_serverReturnsError() {
    ClientCallDelegate<QueryWriteStatusRequest, QueryWriteStatusResponse> clientCall =
        newQueryWriteStatusClientCall();
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .just(clientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_2);

    Single<QueryWriteStatusResponse> responseSingle = RxClientCalls
        .rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertError(e -> {
          assertThat(e).isInstanceOf(StatusRuntimeException.class);
          Status status = Status.fromThrowable(e);
          assertThat(status.getCode()).isEqualTo(Code.NOT_FOUND);
          return true;
        });
    assertThat(queryWriteStatusTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxUnaryCall_serverThrowsRuntimeException() {
    ClientCallDelegate<QueryWriteStatusRequest, QueryWriteStatusResponse> clientCall =
        newQueryWriteStatusClientCall();
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .just(clientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_3);

    Single<QueryWriteStatusResponse> responseSingle = RxClientCalls
        .rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertError(e -> {
          assertThat(e).isInstanceOf(StatusRuntimeException.class);
          Status status = Status.fromThrowable(e);
          assertThat(status.getCode()).isEqualTo(Code.UNKNOWN);
          return true;
        });
    assertThat(queryWriteStatusTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxUnaryCall_serverCompletedWithMultipleValues() {
    ClientCallDelegate<QueryWriteStatusRequest, QueryWriteStatusResponse> clientCall =
        newQueryWriteStatusClientCall();
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .just(clientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_4);

    Single<QueryWriteStatusResponse> responseSingle = RxClientCalls
        .rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertError(e -> {
          assertThat(e).isInstanceOf(StatusRuntimeException.class);
          Status status = Status.fromThrowable(e);
          assertThat(status.getCode()).isEqualTo(Code.CANCELLED);
          return true;
        });
    assertThat(queryWriteStatusTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxUnaryCall_serverCompletedWithoutValue() {
    ClientCallDelegate<QueryWriteStatusRequest, QueryWriteStatusResponse> clientCall =
        newQueryWriteStatusClientCall();
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .just(clientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_5);

    Single<QueryWriteStatusResponse> responseSingle = RxClientCalls
        .rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertError(e -> {
          assertThat(e).isInstanceOf(StatusRuntimeException.class);
          Status status = Status.fromThrowable(e);
          assertThat(status.getCode()).isEqualTo(Code.CANCELLED);
          return true;
        });
    assertThat(queryWriteStatusTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxUnaryCall_serverReturnsValueNotCompleted() {
    ClientCallDelegate<QueryWriteStatusRequest, QueryWriteStatusResponse> clientCall =
        newQueryWriteStatusClientCall();
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .just(clientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_6);

    Single<QueryWriteStatusResponse> responseSingle = RxClientCalls
        .rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertNoValues()
        .assertNotComplete()
        .assertNoErrors();
    assertThat(queryWriteStatusTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxUnaryCall_dispose_cancelCall() {
    ClientCallDelegate<QueryWriteStatusRequest, QueryWriteStatusResponse> clientCall =
        newQueryWriteStatusClientCall();
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .just(clientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_6);

    Single<QueryWriteStatusResponse> responseSingle = RxClientCalls
        .rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .dispose();

    assertThat(queryWriteStatusTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(1);
  }

  @Test
  public void rxStreamingClientCall_smoke() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Observable<WriteRequest> requestObservable = Observable.fromArray(WRITE_REQUESTS_1);

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertValue(newWriteResponse(totalSize(WRITE_REQUESTS_1)))
        .assertComplete();
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @SuppressWarnings("CheckReturnValue")
  @Test
  public void rxStreamingClientCall_noSubscription() {
    AtomicInteger clientCallSingleTimes = new AtomicInteger(0);
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .fromCallable(() -> {
          clientCallSingleTimes.addAndGet(1);
          return newWriteClientCall();
        });
    AtomicInteger requestObservableTimes = new AtomicInteger(0);
    Observable<WriteRequest> requestObservable = Observable.create(emitter -> {
      requestObservableTimes.addAndGet(1);
      for (WriteRequest request : WRITE_REQUESTS_1) {
        emitter.onNext(request);
      }
      emitter.onComplete();
    });

    RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    assertThat(clientCallSingleTimes.get()).isEqualTo(0);
    assertThat(requestObservableTimes.get()).isEqualTo(0);
    assertThat(writeTimes.get()).isEqualTo(0);
  }

  @Test
  public void rxStreamingClientCall_multipleSubscriptions() {
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .fromCallable(this::newWriteClientCall);
    Observable<WriteRequest> requestObservable = Observable.fromArray(WRITE_REQUESTS_1);

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle.blockingSubscribe();
    responseSingle.blockingSubscribe();
    assertThat(writeTimes.get()).isEqualTo(2);
  }

  @Test
  public void rxStreamingClientCall_clientCallSingleOnError() {
    Exception error = new IOException("test error");
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .error(error);
    Observable<WriteRequest> requestObservable = Observable.fromArray(WRITE_REQUESTS_1);

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertError(error.getClass());
    assertThat(writeTimes.get()).isEqualTo(0);
  }

  @Test
  public void rxStreamingClientCall_requestObservableOnError_firstIsError() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Exception error = new IOException("test error");
    Observable<WriteRequest> requestObservable = Observable.error(error);

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertError(error.getClass());
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(1);
  }

  @Test
  public void rxStreamingClientCall_requestObservableOnError_errorAfterFirst() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Exception error = new IOException("test error");
    Observable<WriteRequest> requestObservable = Observable.create(emitter -> {
      emitter.onNext(WRITE_REQUESTS_1[0]);
      emitter.onError(error);
    });

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertError(error.getClass());
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(1);
  }

  @Test
  public void rxStreamingClientCall_serverReturnsError() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Observable<WriteRequest> requestObservable = Observable.fromArray(WRITE_REQUESTS_2);

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertError(e -> {
          assertThat(e).isInstanceOf(StatusRuntimeException.class);
          Status status = Status.fromThrowable(e);
          assertThat(status.getCode()).isEqualTo(Code.NOT_FOUND);
          return true;
        });
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxStreamingClientCall_serverThrowsRuntimeException() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Observable<WriteRequest> requestObservable = Observable.fromArray(WRITE_REQUESTS_3);

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertError(e -> {
          assertThat(e).isInstanceOf(StatusRuntimeException.class);
          Status status = Status.fromThrowable(e);
          assertThat(status.getCode()).isEqualTo(Code.UNKNOWN);
          return true;
        });
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxStreamingClientCall_serverCompletedWithMultipleValues() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Observable<WriteRequest> requestObservable = Observable.fromArray(WRITE_REQUESTS_4);

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertError(e -> {
          assertThat(e).isInstanceOf(StatusRuntimeException.class);
          Status status = Status.fromThrowable(e);
          assertThat(status.getCode()).isEqualTo(Code.CANCELLED);
          return true;
        });
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxStreamingClientCall_serverCompletedWithoutValue() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Observable<WriteRequest> requestObservable = Observable.fromArray(WRITE_REQUESTS_5);

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertError(e -> {
          assertThat(e).isInstanceOf(StatusRuntimeException.class);
          Status status = Status.fromThrowable(e);
          assertThat(status.getCode()).isEqualTo(Code.CANCELLED);
          return true;
        });
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxStreamingClientCall_serverReturnsValueNotCompleted() {
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Observable<WriteRequest> requestObservable = Observable.fromArray(WRITE_REQUESTS_6);

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertNoValues()
        .assertNoErrors()
        .assertNotComplete();
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
  }

  @Test
  public void rxStreamingClientCall_dispose_cancelCall() {
    AtomicBoolean requestDisposed = new AtomicBoolean(false);
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Observable<WriteRequest> requestObservable = Observable.<WriteRequest>create(emitter -> {
      for (WriteRequest request : WRITE_REQUESTS_6) {
        emitter.onNext(request);
      }
    }).doOnDispose(() -> {
      requestDisposed.set(true);
    });

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .dispose();
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(1);
    assertThat(requestDisposed.get()).isTrue();
  }

  @Test
  public void rxStreamingClientCall_serverReturnErrorsBeforeRequestComplete_disposeRequest() {
    AtomicBoolean requestDisposed = new AtomicBoolean(false);
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Observable<WriteRequest> requestObservable = Observable.<WriteRequest>create(emitter -> {
      for (WriteRequest request : WRITE_REQUESTS_2) {
        emitter.onNext(request);
      }
    }).doOnDispose(() -> {
      requestDisposed.set(true);
    });

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertError(e -> {
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
  public void rxStreamingClientCall_serverCompleteBeforeRequestComplete_disposeRequest() {
    AtomicBoolean requestDisposed = new AtomicBoolean(false);
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Observable<WriteRequest> requestObservable = Observable.<WriteRequest>create(emitter -> {
      for (WriteRequest request : WRITE_REQUESTS_1) {
        emitter.onNext(request);
      }
    }).doOnDispose(() -> {
      requestDisposed.set(true);
    });

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertValue(newWriteResponse(totalSize(WRITE_REQUESTS_1)))
        .assertComplete();
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
    assertThat(requestDisposed.get()).isTrue();
  }

  @Test
  public void rxStreamingClientCall_serverCompleteBeforeRequestSubscription_requestNotSubscribed() {
    AtomicBoolean requestSubscribed = new AtomicBoolean(false);
    ClientCallDelegate<WriteRequest, WriteResponse> clientCall =
        newWriteClientCall();
    Single<ClientCall<WriteRequest, WriteResponse>> clientCallSingle = Single
        .just(clientCall);
    Observable<WriteRequest> requestObservable = Observable.<WriteRequest>create(emitter -> {
      for (WriteRequest request : WRITE_REQUESTS_1) {
        emitter.onNext(request);
      }
    }).doOnSubscribe(d -> {
      requestSubscribed.set(true);
    });
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> responseObserver) {
        writeTimes.getAndIncrement();
        responseObserver.onNext(
            WriteResponse.newBuilder().setCommittedSize(totalSize(WRITE_REQUESTS_1)).build());
        responseObserver.onCompleted();
        return new StreamObserver<WriteRequest>() {
          @Override
          public void onNext(WriteRequest writeRequest) {
          }

          @Override
          public void onError(Throwable throwable) {
          }

          @Override
          public void onCompleted() {
          }
        };
      }
    });

    Single<WriteResponse> responseSingle =
        RxClientCalls.rxClientStreamingCall(clientCallSingle, requestObservable);

    responseSingle
        .test()
        .assertValue(newWriteResponse(totalSize(WRITE_REQUESTS_1)))
        .assertComplete();
    assertThat(writeTimes.get()).isEqualTo(1);
    assertThat(clientCall.getCancelTimes()).isEqualTo(0);
    assertThat(requestSubscribed.get()).isFalse();
  }
}
