package com.google.devtools.build.lib.remote;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Digest;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.hash.HashCode;
import com.google.devtools.build.lib.authandtls.CallCredentialsProvider;
import com.google.devtools.build.lib.remote.Retrier.Backoff;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import com.google.devtools.build.lib.vfs.DigestHashFunction;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import io.reactivex.rxjava3.core.Completable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link RxByteStreamUploader}. Test cases migrated from {@link ByteStreamUploaderTest}.
 */
@RunWith(JUnit4.class)
public class RxByteStreamUploaderTest {

  private static final DigestUtil DIGEST_UTIL = new DigestUtil(DigestHashFunction.SHA256);

  private static final int CHUNK_SIZE = 10;
  private static final String INSTANCE_NAME = "foo";

  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();

  private Server server;
  private ManagedChannel channel;
  private Context withEmptyMetadata;

  @Mock
  private Retrier.Backoff mockBackoff;

  @Before
  public final void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    String serverName = "Server for " + this.getClass();
    server = InProcessServerBuilder.forName(serverName).fallbackHandlerRegistry(serviceRegistry)
        .build().start();
    channel = InProcessChannelBuilder.forName(serverName).build();
    withEmptyMetadata =
        TracingMetadataUtils.contextWithMetadata(
            "none", "none", DIGEST_UTIL.asActionKey(Digest.getDefaultInstance()));
  }

  private static RxRemoteRetrier newRetrier(
      Supplier<Backoff> backoff,
      Predicate<? super Throwable> shouldRetry) {
    return new RxRemoteRetrier(
        backoff,
        shouldRetry, CallCredentialsProvider.NO_CREDENTIALS);
  }

  private RxByteStreamUploader newUploader(RxRemoteRetrier retrier) {
    return new RxByteStreamUploader(INSTANCE_NAME,
        new ReferenceCountedChannel(channel), /* callTimeoutSecs= */ 1, retrier);
  }

  @After
  public void tearDown() throws Exception {
    channel.shutdownNow();
    channel.awaitTermination(5, TimeUnit.SECONDS);
    server.shutdownNow();
    server.awaitTermination();
  }

  @Test
  public void uploadBlob_smoke() throws Exception {
    RxRemoteRetrier retrier = newRetrier(() -> mockBackoff, (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 2 + 1];
    new Random().nextBytes(blob);
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> streamObserver) {
        return new StreamObserver<WriteRequest>() {

          final byte[] receivedData = new byte[blob.length];
          long nextOffset = 0;

          @Override
          public void onNext(WriteRequest writeRequest) {
            if (nextOffset == 0) {
              assertThat(writeRequest.getResourceName()).isNotEmpty();
              assertThat(writeRequest.getResourceName()).startsWith(INSTANCE_NAME + "/uploads");
              assertThat(writeRequest.getResourceName()).endsWith(String.valueOf(blob.length));
            } else {
              assertThat(writeRequest.getResourceName()).isEmpty();
            }

            assertThat(writeRequest.getWriteOffset()).isEqualTo(nextOffset);

            ByteString data = writeRequest.getData();

            System.arraycopy(data.toByteArray(), 0, receivedData, (int) nextOffset,
                data.size());

            nextOffset += data.size();
            boolean lastWrite = blob.length == nextOffset;
            assertThat(writeRequest.getFinishWrite()).isEqualTo(lastWrite);
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should never be called.");
          }

          @Override
          public void onCompleted() {
            assertThat(nextOffset).isEqualTo(blob.length);
            assertThat(receivedData).isEqualTo(blob);

            WriteResponse response =
                WriteResponse.newBuilder().setCommittedSize(nextOffset).build();
            streamObserver.onNext(response);
            streamObserver.onCompleted();
          }
        };
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.uploadBlob(hash, chunker, true));

    upload.test()
        .await()
        .assertComplete();
    // This test should not have triggered any retries.
    verifyZeroInteractions(mockBackoff);
  }

  @Test
  public void uploadBlob_makingProgress_resetBackoff() throws Exception {
    when(mockBackoff.getRetryAttempts()).thenReturn(0);
    RxRemoteRetrier retrier = newRetrier(() -> mockBackoff, (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 2 + 1];
    new Random().nextBytes(blob);
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    serviceRegistry.addService(new ByteStreamImplBase() {
      final byte[] receivedData = new byte[blob.length];
      String receivedResourceName = null;
      boolean receivedComplete = false;
      long nextOffset = 0;
      long initialOffset = 0;
      boolean mustQueryWriteStatus = false;

      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> streamObserver) {
        return new StreamObserver<WriteRequest>() {
          @Override
          public void onNext(WriteRequest writeRequest) {
            assertThat(mustQueryWriteStatus).isFalse();

            String resourceName = writeRequest.getResourceName();
            if (nextOffset == initialOffset) {
              if (initialOffset == 0) {
                receivedResourceName = resourceName;
              }
              assertThat(resourceName).startsWith(INSTANCE_NAME + "/uploads");
              assertThat(resourceName).endsWith(String.valueOf(blob.length));
            } else {
              assertThat(resourceName).isEmpty();
            }

            assertThat(writeRequest.getWriteOffset()).isEqualTo(nextOffset);

            ByteString data = writeRequest.getData();

            System.arraycopy(
                data.toByteArray(), 0, receivedData, (int) nextOffset, data.size());

            nextOffset += data.size();
            receivedComplete = blob.length == nextOffset;
            assertThat(writeRequest.getFinishWrite()).isEqualTo(receivedComplete);

            if (initialOffset == 0) {
              streamObserver.onError(Status.DEADLINE_EXCEEDED.asException());
              mustQueryWriteStatus = true;
              initialOffset = nextOffset;
            }
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should never be called.");
          }

          @Override
          public void onCompleted() {
            assertThat(nextOffset).isEqualTo(blob.length);
            assertThat(receivedData).isEqualTo(blob);

            WriteResponse response =
                WriteResponse.newBuilder().setCommittedSize(nextOffset).build();
            streamObserver.onNext(response);
            streamObserver.onCompleted();
          }
        };
      }

      @Override
      public void queryWriteStatus(
          QueryWriteStatusRequest request, StreamObserver<QueryWriteStatusResponse> response) {
        String resourceName = request.getResourceName();
        final long committedSize;
        final boolean complete;
        if (receivedResourceName != null && receivedResourceName.equals(resourceName)) {
          assertThat(mustQueryWriteStatus).isTrue();
          mustQueryWriteStatus = false;
          committedSize = nextOffset;
          complete = receivedComplete;
        } else {
          committedSize = 0;
          complete = false;
        }
        response.onNext(
            QueryWriteStatusResponse.newBuilder()
                .setCommittedSize(committedSize)
                .setComplete(complete)
                .build());
        response.onCompleted();
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.uploadBlob(hash, chunker, true));

    upload.test()
        .await()
        .assertComplete();
    // This test should not have triggered any retries.
    verify(mockBackoff, never()).nextDelayMillis();
    verify(mockBackoff, times(1)).getRetryAttempts();
  }

  @Test
  public void uploadBlob_uploadFailedButQueryCompleted_notRetried() throws Exception {
    // Test that after an upload has failed and the QueryWriteStatus call returns
    // that the upload has completed that we'll not retry the upload.
    RxRemoteRetrier retrier = newRetrier(() -> new FixedBackoff(1, 0), (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 2 + 1];
    new Random().nextBytes(blob);
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    AtomicInteger numWriteCalls = new AtomicInteger(0);
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> streamObserver) {
        numWriteCalls.getAndIncrement();
        streamObserver.onError(Status.DEADLINE_EXCEEDED.asException());
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

      @Override
      public void queryWriteStatus(
          QueryWriteStatusRequest request, StreamObserver<QueryWriteStatusResponse> response) {
        response.onNext(
            QueryWriteStatusResponse.newBuilder()
                .setCommittedSize(blob.length)
                .setComplete(true)
                .build());
        response.onCompleted();
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.uploadBlob(hash, chunker, true));

    upload.test()
        .await()
        .assertComplete();
    // This test should not have triggered any retries.
    assertThat(numWriteCalls.get()).isEqualTo(1);
  }

  @Test
  public void uploadBlob_unimplementedQuery_restartUpload() throws Exception {
    when(mockBackoff.getRetryAttempts()).thenReturn(0);
    RxRemoteRetrier retrier = newRetrier(() -> mockBackoff, (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 2 + 1];
    new Random().nextBytes(blob);
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    serviceRegistry.addService(new ByteStreamImplBase() {
      boolean expireCall = true;
      boolean sawReset = false;

      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> streamObserver) {
        return new StreamObserver<WriteRequest>() {
          @Override
          public void onNext(WriteRequest writeRequest) {
            if (expireCall) {
              streamObserver.onError(Status.DEADLINE_EXCEEDED.asException());
              expireCall = false;
            } else if (!sawReset && writeRequest.getWriteOffset() != 0) {
              streamObserver.onError(Status.INVALID_ARGUMENT.asException());
            } else {
              sawReset = true;
              if (writeRequest.getFinishWrite()) {
                long committedSize =
                    writeRequest.getWriteOffset() + writeRequest.getData().size();
                streamObserver.onNext(
                    WriteResponse.newBuilder().setCommittedSize(committedSize).build());
                streamObserver.onCompleted();
              }
            }
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should never be called.");
          }

          @Override
          public void onCompleted() {
          }
        };
      }

      @Override
      public void queryWriteStatus(
          QueryWriteStatusRequest request, StreamObserver<QueryWriteStatusResponse> response) {
        response.onError(Status.UNIMPLEMENTED.asException());
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.uploadBlob(hash, chunker, true));

    upload.test()
        .await()
        .assertComplete();
    // This test should have triggered a single retry, because it made
    // no progress.
    verify(mockBackoff, times(1)).nextDelayMillis();
  }

  @Test
  public void uploadBlob_earlyWriteResponse_completeUpload() throws Exception {
    RxRemoteRetrier retrier = newRetrier(() -> mockBackoff, (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 2 + 1];
    new Random().nextBytes(blob);
    // provide only enough data to write a single chunk
    InputStream in = new ByteArrayInputStream(blob, 0, CHUNK_SIZE);
    Chunker chunker = Chunker.builder().setInput(blob.length, in).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> streamObserver) {
        streamObserver.onNext(WriteResponse.newBuilder().setCommittedSize(blob.length).build());
        streamObserver.onCompleted();
        return new NoopStreamObserver();
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.uploadBlob(hash, chunker, true));

    upload.test()
        .await()
        .assertComplete();
    // This test should not have triggered any retries.
    verifyZeroInteractions(mockBackoff);
  }

  // Dispose in the middle, should stop asking next chunks

  private static class NoopStreamObserver implements StreamObserver<WriteRequest> {

    @Override
    public void onNext(WriteRequest writeRequest) {
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onCompleted() {
    }
  }

  static class FixedBackoff implements Retrier.Backoff {

    private final int maxRetries;
    private final int delayMillis;

    private int retries;

    public FixedBackoff(int maxRetries, int delayMillis) {
      this.maxRetries = maxRetries;
      this.delayMillis = delayMillis;
    }

    @Override
    public long nextDelayMillis() {
      if (retries < maxRetries) {
        retries++;
        return delayMillis;
      }
      return -1;
    }

    @Override
    public int getRetryAttempts() {
      return retries;
    }
  }
}
