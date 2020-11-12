package com.google.devtools.build.lib.remote;

import static com.google.common.truth.Truth.assertThat;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import io.reactivex.rxjava3.core.Single;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
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
      .setResourceName("request 1")
      .build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_2 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request 2")
      .build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_3 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request 3")
      .build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_4 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request 4")
      .build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_5 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request 5")
      .build();

  private static final QueryWriteStatusRequest QUERY_WRITE_STATUS_REQUEST_6 = QueryWriteStatusRequest
      .newBuilder()
      .setResourceName("request 6")
      .build();

  private static final QueryWriteStatusResponse QUERY_WRITE_STATUS_RESPONSE_1 = QueryWriteStatusResponse
      .newBuilder()
      .setCommittedSize(1)
      .setComplete(true)
      .build();

  private final String fakeServerName = "fake server for " + getClass();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final AtomicInteger queryWriteStatusTimes = new AtomicInteger(0);
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
    });

    channel = InProcessChannelBuilder.forName(fakeServerName)
        .directExecutor()
        .build();
  }

  @After
  public void tearDown() throws Exception {
    channel.shutdown();

    fakeServer.shutdownNow();
    fakeServer.awaitTermination();
  }

  private ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse> newQueryWriteStatusClientCall() {
    return channel.newCall(ByteStreamGrpc.getQueryWriteStatusMethod(), CallOptions.DEFAULT);
  }

  @Test
  public void rxUnaryCall_smoke() {
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .fromCallable(this::newQueryWriteStatusClientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_1);

    Single<QueryWriteStatusResponse> responseSingle =
        RxClientCalls.rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertValue(QUERY_WRITE_STATUS_RESPONSE_1)
        .assertComplete();
    assertThat(queryWriteStatusTimes.get()).isEqualTo(1);
  }

  @SuppressWarnings("CheckReturnValue")
  @Test
  public void rxUnaryCall_noSubscription() {
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .fromCallable(this::newQueryWriteStatusClientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_1);

    RxClientCalls.rxUnaryCall(clientCallSingle, requestSingle);

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
    Exception error = new IOException("test error");
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .fromCallable(this::newQueryWriteStatusClientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .error(error);

    Single<QueryWriteStatusResponse> responseSingle = RxClientCalls
        .rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertError(error.getClass());
    assertThat(queryWriteStatusTimes.get()).isEqualTo(0);
  }

  @Test
  public void rxUnaryCall_serverReturnsError() {
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .fromCallable(this::newQueryWriteStatusClientCall);
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
  }

  @Test
  public void rxUnaryCall_serverThrowsRuntimeException() {
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .fromCallable(this::newQueryWriteStatusClientCall);
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
  }

  @Test
  public void rxUnaryCall_serverCompletedWithMultipleValues() {
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .fromCallable(this::newQueryWriteStatusClientCall);
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
  }

  @Test
  public void rxUnaryCall_serverCompletedWithoutValue() {
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .fromCallable(this::newQueryWriteStatusClientCall);
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
  }

  @Test
  public void rxUnaryCall_serverReturnsValueNotCompleted() {
    Single<ClientCall<QueryWriteStatusRequest, QueryWriteStatusResponse>> clientCallSingle = Single
        .fromCallable(this::newQueryWriteStatusClientCall);
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QUERY_WRITE_STATUS_REQUEST_6);

    Single<QueryWriteStatusResponse> responseSingle = RxClientCalls
        .rxUnaryCall(clientCallSingle, requestSingle);

    responseSingle
        .test()
        .assertNoErrors()
        .assertNotComplete();
    assertThat(queryWriteStatusTimes.get()).isEqualTo(1);
  }
}
