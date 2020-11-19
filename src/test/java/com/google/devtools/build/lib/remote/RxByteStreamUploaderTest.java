package com.google.devtools.build.lib.remote;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.devtools.build.lib.analysis.BlazeVersionInfo;
import com.google.devtools.build.lib.authandtls.CallCredentialsProvider;
import com.google.devtools.build.lib.remote.ByteStreamUploaderTest.FixedBackoff;
import com.google.devtools.build.lib.remote.ByteStreamUploaderTest.MaybeFailOnceUploadService;
import com.google.devtools.build.lib.remote.ByteStreamUploaderTest.NoopStreamObserver;
import com.google.devtools.build.lib.remote.Retrier.Backoff;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import com.google.devtools.build.lib.vfs.DigestHashFunction;
import com.google.protobuf.ByteString;
import io.grpc.BindableService;
import io.grpc.CallCredentials;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
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
  private final AtomicReference<Throwable> rxGlobalThrowable = new AtomicReference<>(null);

  @Before
  public final void setUp() throws Exception {
    RxJavaPlugins.setErrorHandler(rxGlobalThrowable::set);
    MockitoAnnotations.initMocks(this);

    String serverName = "Server for " + this.getClass();
    server = InProcessServerBuilder.forName(serverName).fallbackHandlerRegistry(serviceRegistry)
        .build().start();
    channel = InProcessChannelBuilder.forName(serverName).build();
    withEmptyMetadata =
        TracingMetadataUtils.contextWithMetadata(
            "none", "none", DIGEST_UTIL.asActionKey(Digest.getDefaultInstance()));
  }

  @After
  public void tearDown() throws Throwable {
    channel.shutdownNow();
    channel.awaitTermination(5, TimeUnit.SECONDS);
    server.shutdownNow();
    server.awaitTermination();

    // Make sure rxjava didn't receive global errors
    Throwable t = rxGlobalThrowable.getAndSet(null);
    if (t != null) {
      throw t;
    }
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
        new ReferenceCountedChannel(channel), /* callTimeoutSecs= */ 60, retrier);
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

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertComplete();
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

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertComplete();
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

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertComplete();
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

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertComplete();
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

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertComplete();
    // This test should not have triggered any retries.
    verifyZeroInteractions(mockBackoff);
  }

  @Test
  public void uploadBlob_incorrectCommittedSize_uploadFailed() throws Exception {
    RxRemoteRetrier retrier = newRetrier(() -> mockBackoff, (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 2 + 1];
    new Random().nextBytes(blob);
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> streamObserver) {
        streamObserver.onNext(WriteResponse.newBuilder().setCommittedSize(blob.length + 1).build());
        streamObserver.onCompleted();
        return new NoopStreamObserver();
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertError(IOException.class);
    // This test should not have triggered any retries.
    verifyZeroInteractions(mockBackoff);
  }

  @Test
  public void uploadBlob_multipleUploads() throws Exception {
    RxRemoteRetrier retrier = newRetrier(() -> new FixedBackoff(1, 0), (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    int numUploads = 10;
    HashCode[] hashCodes = new HashCode[numUploads];
    Chunker[] chunkers = new Chunker[numUploads];
    Map<HashCode, byte[]> blobsByHash = Maps.newHashMap();
    Random rand = new Random();
    for (int i = 0; i < numUploads; i++) {
      int blobSize = rand.nextInt(CHUNK_SIZE * 10) + CHUNK_SIZE;
      byte[] blob = new byte[blobSize];
      rand.nextBytes(blob);
      Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
      HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
      hashCodes[i] = hash;
      chunkers[i] = chunker;
      blobsByHash.put(hash, blob);
    }
    serviceRegistry.addService(new MaybeFailOnceUploadService(blobsByHash));

    Completable[] uploads = new Completable[numUploads];
    for (int i = 0; i < numUploads; i++) {
      final int index = i;
      uploads[i] = withEmptyMetadata
          .call(() -> uploader.upload(hashCodes[index], chunkers[index], true));
    }

    TestObserver[] uploadTests = new TestObserver[numUploads];
    for (int i = 0; i < numUploads; i++) {
      uploadTests[i] = uploads[i].test();
    }
    for (int i = 0; i < numUploads; i++) {
      uploadTests[i].await().assertComplete();
    }
  }

  @Test
  public void uploadBlob_multipleUploadsWithDifferentContext_preservedUponRetries()
      throws Exception {
    // We upload blobs with different context, and retry 3 times for each upload.
    // We verify that the correct metadata is passed to the server with every blob.
    RxRemoteRetrier retrier = newRetrier(() -> new FixedBackoff(5, 0), (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    List<String> toUpload = ImmutableList.of("aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc");
    Map<Digest, Chunker> chunkers = Maps.newHashMapWithExpectedSize(toUpload.size());
    Map<String, Integer> uploadsFailed = Maps.newHashMap();
    for (String s : toUpload) {
      Chunker chunker = Chunker.builder().setInput(s.getBytes(UTF_8)).setChunkSize(3).build();
      Digest digest = DIGEST_UTIL.computeAsUtf8(s);
      chunkers.put(digest, chunker);
      uploadsFailed.put(digest.getHash(), 0);
    }

    BindableService bsService = new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> response) {
        return new StreamObserver<WriteRequest>() {

          private String digestHash;

          @Override
          public void onNext(WriteRequest writeRequest) {
            String resourceName = writeRequest.getResourceName();
            if (!resourceName.isEmpty()) {
              String[] components = resourceName.split("/");
              assertThat(components).hasLength(6);
              digestHash = components[4];
            }
            assertThat(digestHash).isNotNull();
            RequestMetadata meta = TracingMetadataUtils.fromCurrentContext();
            assertThat(meta.getCorrelatedInvocationsId()).isEqualTo("build-req-id");
            assertThat(meta.getToolInvocationId()).isEqualTo("command-id");
            assertThat(meta.getActionId()).isEqualTo(digestHash);
            assertThat(meta.getToolDetails().getToolName()).isEqualTo("bazel");
            assertThat(meta.getToolDetails().getToolVersion())
                .isEqualTo(BlazeVersionInfo.instance().getVersion());
            synchronized (this) {
              Integer numFailures = uploadsFailed.get(digestHash);
              if (numFailures < 3) {
                uploadsFailed.put(digestHash, numFailures + 1);
                response.onError(Status.INTERNAL.asException());
              }
            }
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should never be called.");
          }

          @Override
          public void onCompleted() {
            response.onNext(WriteResponse.newBuilder().setCommittedSize(10).build());
            response.onCompleted();
          }
        };
      }

      @Override
      public void queryWriteStatus(
          QueryWriteStatusRequest request, StreamObserver<QueryWriteStatusResponse> response) {
        response.onNext(
            QueryWriteStatusResponse.newBuilder()
                .setCommittedSize(0)
                .setComplete(false)
                .build());
        response.onCompleted();
      }
    };
    serviceRegistry.addService(ServerInterceptors
        .intercept(bsService, new TracingMetadataUtils.ServerHeadersInterceptor()));

    List<Completable> uploads = new ArrayList<>();
    for (Map.Entry<Digest, Chunker> chunkerEntry : chunkers.entrySet()) {
      Digest actionDigest = chunkerEntry.getKey();
      Context ctx = TracingMetadataUtils
          .contextWithMetadata("build-req-id", "command-id", DIGEST_UTIL.asActionKey(actionDigest));
      uploads.add(ctx.call(() -> uploader.upload(HashCode.fromString(actionDigest.getHash()),
          chunkerEntry.getValue(), /* forceUpload=*/ true)));
    }

    List<TestObserver<Void>> uploadTests = new ArrayList<>();
    for (Completable upload : uploads) {
      uploadTests.add(upload.test());
    }
    for (TestObserver<Void> uploadTest : uploadTests) {
      uploadTest.await().assertComplete();
    }
  }

  @Test
  public void uploadBlob_customHeaders_attachedToRequest() throws Exception {
    RxRemoteRetrier retrier = newRetrier(() -> new FixedBackoff(1, 0), (e) -> true);
    Metadata metadata = new Metadata();
    metadata.put(Metadata.Key.of("Key1", Metadata.ASCII_STRING_MARSHALLER), "Value1");
    metadata.put(Metadata.Key.of("Key2", Metadata.ASCII_STRING_MARSHALLER), "Value2");
    RxByteStreamUploader uploader =
        new RxByteStreamUploader(
            INSTANCE_NAME,
            new ReferenceCountedChannel(
                InProcessChannelBuilder.forName("Server for " + this.getClass())
                    .intercept(MetadataUtils.newAttachHeadersInterceptor(metadata))
                    .build()),
            /* callTimeoutSecs= */ 1,
            retrier);
    byte[] blob = new byte[CHUNK_SIZE];
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    serviceRegistry.addService(ServerInterceptors.intercept(
        new ByteStreamImplBase() {
          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> streamObserver) {
            return new StreamObserver<WriteRequest>() {
              @Override
              public void onNext(WriteRequest writeRequest) {
              }

              @Override
              public void onError(Throwable throwable) {
                fail("onError should never be called.");
              }

              @Override
              public void onCompleted() {
                WriteResponse response =
                    WriteResponse.newBuilder().setCommittedSize(blob.length).build();
                streamObserver.onNext(response);
                streamObserver.onCompleted();
              }
            };
          }
        },
        new ServerInterceptor() {
          @Override
          public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
              ServerCall<ReqT, RespT> call,
              Metadata metadata,
              ServerCallHandler<ReqT, RespT> next) {
            assertThat(metadata.get(Metadata.Key.of("Key1", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("Value1");
            assertThat(metadata.get(Metadata.Key.of("Key2", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("Value2");
            assertThat(metadata.get(Metadata.Key.of("Key3", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo(null);
            return next.startCall(call, metadata);
          }
        }));

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertComplete();
  }

  @Test
  public void uploadBlob_sameBlob_notUploadedTwice() throws Exception {
    // Test that uploading the same file concurrently triggers only one file upload.
    RxRemoteRetrier retrier = newRetrier(() -> mockBackoff, (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 10];
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    AtomicInteger numWriteCalls = new AtomicInteger();
    CountDownLatch blocker = new CountDownLatch(1);
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> response) {
        numWriteCalls.incrementAndGet();
        try {
          // Ensures that the first upload does not finish, before the second upload is started.
          blocker.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        return new StreamObserver<WriteRequest>() {

          private long bytesReceived;

          @Override
          public void onNext(WriteRequest writeRequest) {
            bytesReceived += writeRequest.getData().size();
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should never be called.");
          }

          @Override
          public void onCompleted() {
            response.onNext(WriteResponse.newBuilder().setCommittedSize(bytesReceived).build());
            response.onCompleted();
          }
        };
      }
    });

    Completable upload1 = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));
    Completable upload2 = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    TestObserver<Void> uploadTest1 = upload1.test();
    TestObserver<Void> uploadTest1Dup = upload1.test();
    TestObserver<Void> uploadTest2 = upload2.test();

    blocker.countDown();

    uploadTest1.await().assertComplete();
    uploadTest1Dup.await().assertComplete();
    uploadTest2.await().assertComplete();

    assertThat(numWriteCalls.get()).isEqualTo(1);
  }

  @Test
  public void uploadBlob_errors_reported() throws Exception {
    RxRemoteRetrier retrier = newRetrier(() -> new FixedBackoff(1, 10), (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE];
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> response) {
        response.onError(Status.INTERNAL.asException());
        return new NoopStreamObserver();
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertError(e -> {
      assertThat(RemoteRetrierUtils.causedByStatus(e, Code.INTERNAL)).isTrue();
      return true;
    });
  }

  @Test
  public void uploadBlob_noSubscriptions_cancelOngoingUploads() throws Exception {
    Context prevContext = withEmptyMetadata.attach();
    RxRemoteRetrier retrier = newRetrier(() -> new FixedBackoff(1, 10), (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    CountDownLatch cancellations = new CountDownLatch(2);
    ServerServiceDefinition service = ServerServiceDefinition.builder(ByteStreamGrpc.SERVICE_NAME)
        .addMethod(ByteStreamGrpc.getWriteMethod(), (call, headers) -> {
          // Don't request() any messages from the client, so that the client will be
          // blocked
          // on flow control and thus the call will sit there idle long enough to receive
          // the
          // cancellation.
          return new Listener<WriteRequest>() {
            @Override
            public void onCancel() {
              cancellations.countDown();
            }
          };
        })
        .build();
    serviceRegistry.addService(service);
    byte[] blob1 = new byte[CHUNK_SIZE];
    Chunker chunker1 = Chunker.builder().setInput(blob1).setChunkSize(CHUNK_SIZE).build();
    HashCode hash1 = HashCode.fromString(DIGEST_UTIL.compute(blob1).getHash());
    byte[] blob2 = new byte[CHUNK_SIZE + 1];
    Chunker chunker2 = Chunker.builder().setInput(blob2).setChunkSize(CHUNK_SIZE).build();
    HashCode hash2 = HashCode.fromString(DIGEST_UTIL.compute(blob2).getHash());

    Completable upload1 = uploader.upload(hash1, chunker1, true);
    Completable upload1Dup = uploader.upload(hash1, chunker1, true);
    Completable upload2 = uploader.upload(hash2, chunker2, true);

    TestObserver<Void> uploadTest1 = upload1.test();
    TestObserver<Void> uploadTest1Dup = upload1Dup.test();
    TestObserver<Void> uploadTest2 = upload2.test();

    assertThat(uploader.uploadsInProgress()).isTrue();

    uploadTest1.dispose();
    assertThat(uploader.uploadsInProgress(hash1)).isTrue();

    uploadTest2.dispose();
    assertThat(uploader.uploadsInProgress(hash2)).isFalse();

    uploadTest1Dup.dispose();
    assertThat(uploader.uploadsInProgress(hash1)).isFalse();

    assertThat(uploader.uploadsInProgress()).isFalse();

    cancellations.await();

    withEmptyMetadata.detach(prevContext);
  }

  @Test
  public void uploadBlob_noInstanceName() throws Exception {
    RxRemoteRetrier retrier = newRetrier(() -> mockBackoff, (e) -> true);
    RxByteStreamUploader uploader = new RxByteStreamUploader( /* instanceName= */ null,
        new ReferenceCountedChannel(channel), /* callTimeoutSecs= */ 1, retrier);

    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> response) {
        return new StreamObserver<WriteRequest>() {
          @Override
          public void onNext(WriteRequest writeRequest) {
            // Test that the resource name doesn't start with an instance name.
            assertThat(writeRequest.getResourceName()).startsWith("uploads/");
          }

          @Override
          public void onError(Throwable throwable) {

          }

          @Override
          public void onCompleted() {
            response.onNext(WriteResponse.newBuilder().setCommittedSize(1).build());
            response.onCompleted();
          }
        };
      }
    });

    byte[] blob = new byte[1];
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertComplete();
  }

  @Test
  public void uploadBlob_nonRetryableStatus_notBeRetried() throws Exception {
    RxRemoteRetrier retrier = newRetrier(() -> new FixedBackoff(1, 0), /* No Status is retriable. */
        (e) -> false);
    RxByteStreamUploader uploader = newUploader(retrier);
    AtomicInteger numCalls = new AtomicInteger();
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> response) {
        numCalls.incrementAndGet();
        response.onError(Status.INTERNAL.asException());
        return new NoopStreamObserver();
      }
    });
    byte[] blob = new byte[1];
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertError(IOException.class);
    assertThat(numCalls.get()).isEqualTo(1);
  }

  @Test
  public void uploadBlob_failedUploads_notDeduplicate() throws Exception {
    RxRemoteRetrier retrier = newRetrier(() -> Retrier.RETRIES_DISABLED, (e) -> false);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 2 + 1];
    new Random().nextBytes(blob);
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    AtomicInteger numUploads = new AtomicInteger();
    serviceRegistry.addService(new ByteStreamImplBase() {
      boolean failRequest = true;

      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> streamObserver) {
        numUploads.incrementAndGet();
        return new StreamObserver<WriteRequest>() {
          long nextOffset = 0;

          @Override
          public void onNext(WriteRequest writeRequest) {
            if (failRequest) {
              streamObserver.onError(Status.UNKNOWN.asException());
              failRequest = false;
            } else {
              nextOffset += writeRequest.getData().size();
              boolean lastWrite = blob.length == nextOffset;
              assertThat(writeRequest.getFinishWrite()).isEqualTo(lastWrite);
            }
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should never be called.");
          }

          @Override
          public void onCompleted() {
            assertThat(nextOffset).isEqualTo(blob.length);

            WriteResponse response =
                WriteResponse.newBuilder().setCommittedSize(nextOffset).build();
            streamObserver.onNext(response);
            streamObserver.onCompleted();
          }
        };
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    // This should fail
    upload.test().await().assertError(e -> {
      assertThat(e).hasCauseThat().isInstanceOf(StatusRuntimeException.class);
      assertThat(Status.fromThrowable(e.getCause()).getCode()).isEqualTo(Code.UNKNOWN);
      return true;
    });

    upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, false));

    // This should trigger an upload.
    upload.test().await().assertComplete();

    assertThat(numUploads.get()).isEqualTo(2);
  }

  @Test
  public void uploadBlob_deduplicationOfUploads() throws Exception {
    RxRemoteRetrier retrier = newRetrier(() -> mockBackoff, (e) -> true);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 2 + 1];
    new Random().nextBytes(blob);
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    AtomicInteger numUploads = new AtomicInteger();
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> streamObserver) {
        numUploads.incrementAndGet();
        return new StreamObserver<WriteRequest>() {

          long nextOffset = 0;

          @Override
          public void onNext(WriteRequest writeRequest) {
            nextOffset += writeRequest.getData().size();
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

            WriteResponse response =
                WriteResponse.newBuilder().setCommittedSize(nextOffset).build();
            streamObserver.onNext(response);
            streamObserver.onCompleted();
          }
        };
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));
    upload.test().await().assertComplete();

    upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, false));
    // This should not trigger an upload.
    upload.test().await().assertComplete();

    assertThat(numUploads.get()).isEqualTo(1);

    // This test should not have triggered any retries.
    verifyZeroInteractions(mockBackoff);
  }

  @Test
  public void uploadBlob_unauthenticatedError_notRetriedTwice() throws Exception {
    AtomicInteger refreshTimes = new AtomicInteger();
    CallCredentialsProvider callCredentialsProvider =
        new CallCredentialsProvider() {
          @Nullable
          @Override
          public CallCredentials getCallCredentials() {
            return null;
          }

          @Override
          public void refresh() throws IOException {
            refreshTimes.incrementAndGet();
          }
        };
    RxRemoteRetrier retrier = new RxRemoteRetrier(() -> mockBackoff,
        RemoteRetrier.RETRIABLE_GRPC_ERRORS, callCredentialsProvider);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 2 + 1];
    new Random().nextBytes(blob);
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    AtomicInteger numUploads = new AtomicInteger();
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> streamObserver) {
        numUploads.incrementAndGet();

        streamObserver.onError(Status.UNAUTHENTICATED.asException());
        return new NoopStreamObserver();
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertError(IOException.class);
    assertThat(refreshTimes.get()).isEqualTo(1);
    assertThat(numUploads.get()).isEqualTo(2);
    // This test should not have triggered any retries.
    verifyZeroInteractions(mockBackoff);
  }

  @Test
  public void uploadBlob_authenticationError_refresh() throws Exception {
    AtomicInteger refreshTimes = new AtomicInteger();
    CallCredentialsProvider callCredentialsProvider =
        new CallCredentialsProvider() {
          @Nullable
          @Override
          public CallCredentials getCallCredentials() {
            return null;
          }

          @Override
          public void refresh() throws IOException {
            refreshTimes.incrementAndGet();
          }
        };
    RxRemoteRetrier retrier = new RxRemoteRetrier(() -> mockBackoff,
        RemoteRetrier.RETRIABLE_GRPC_ERRORS, callCredentialsProvider);
    RxByteStreamUploader uploader = newUploader(retrier);
    byte[] blob = new byte[CHUNK_SIZE * 2 + 1];
    new Random().nextBytes(blob);
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    HashCode hash = HashCode.fromString(DIGEST_UTIL.compute(blob).getHash());
    AtomicInteger numUploads = new AtomicInteger();
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> streamObserver) {
        numUploads.incrementAndGet();

        if (refreshTimes.get() == 0) {
          streamObserver.onError(Status.UNAUTHENTICATED.asException());
          return new NoopStreamObserver();
        }

        return new StreamObserver<WriteRequest>() {
          long nextOffset = 0;

          @Override
          public void onNext(WriteRequest writeRequest) {
            nextOffset += writeRequest.getData().size();
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

            WriteResponse response =
                WriteResponse.newBuilder().setCommittedSize(nextOffset).build();
            streamObserver.onNext(response);
            streamObserver.onCompleted();
          }
        };
      }
    });

    Completable upload = withEmptyMetadata.call(() -> uploader.upload(hash, chunker, true));

    upload.test().await().assertComplete();
    assertThat(refreshTimes.get()).isEqualTo(1);
    assertThat(numUploads.get()).isEqualTo(2);
    // This test should not have triggered any retries.
    verifyZeroInteractions(mockBackoff);
  }

  @Test
  public void newRequestObservable_dispose_notAskRemainingChunks() throws Exception {
    byte[] blob = new byte[CHUNK_SIZE * 10];
    Chunker chunker = Chunker.builder().setInput(blob).setChunkSize(CHUNK_SIZE).build();
    AtomicInteger nextTimes = new AtomicInteger(0);
    Chunker mockChunker = mock(Chunker.class);
    when(mockChunker.hasNext()).thenAnswer(invocation -> chunker.hasNext());
    when(mockChunker.next()).thenAnswer(invocation -> {
      if (nextTimes.getAndIncrement() != 0) {
        fail("next() should on be called once");
      }
      return chunker.next();
    });
    Observable<WriteRequest> requestObservable = RxByteStreamUploader
        .newRequestObservable(INSTANCE_NAME, mockChunker);

    requestObservable.subscribe(new Observer<WriteRequest>() {
      private Disposable disposable;

      @Override
      public void onSubscribe(@NonNull Disposable d) {
        disposable = d;
      }

      @Override
      public void onNext(@NonNull WriteRequest writeRequest) {
        assertThat(writeRequest.getResourceName()).isEqualTo(INSTANCE_NAME);
        assertThat(writeRequest.getWriteOffset()).isEqualTo(0);
        assertThat(writeRequest.getData().size()).isEqualTo(CHUNK_SIZE);
        disposable.dispose();
      }

      @Override
      public void onError(@NonNull Throwable e) {
        fail("onError should never be called");
      }

      @Override
      public void onComplete() {
        fail("onComplete should never be called");
      }
    });
  }
}
