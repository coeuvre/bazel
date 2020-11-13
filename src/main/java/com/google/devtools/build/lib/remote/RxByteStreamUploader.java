package com.google.devtools.build.lib.remote;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.base.Strings;
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.HashCode;
import com.google.devtools.build.lib.authandtls.CallCredentialsProvider;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import io.grpc.Context;
import io.grpc.StatusRuntimeException;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.concurrent.GuardedBy;

public class RxByteStreamUploader extends AbstractReferenceCounted {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final String instanceName;
  private final ReferenceCountedChannel channel;
  private final CallCredentialsProvider callCredentialsProvider;
  private final long callTimeoutSecs;

  private final Object lock = new Object();

  /**
   * Contains the hash codes of already uploaded blobs.
   **/
  @GuardedBy("lock")
  private final Set<HashCode> uploadedBlobs = new HashSet<>();

  @GuardedBy("lock")
  private final Map<HashCode, ConnectableObservable<Object>> uploadsInProgress = new HashMap<>();

  @GuardedBy("lock")
  private boolean isShutdown;

  public RxByteStreamUploader(String instanceName, ReferenceCountedChannel channel,
      CallCredentialsProvider callCredentialsProvider, long callTimeoutSecs) {
    this.instanceName = instanceName;
    this.channel = channel;
    this.callCredentialsProvider = callCredentialsProvider;
    this.callTimeoutSecs = callTimeoutSecs;
  }

  @Override
  public RxByteStreamUploader retain() {
    return (RxByteStreamUploader) super.retain();
  }

  @Override
  public RxByteStreamUploader retain(int increment) {
    return (RxByteStreamUploader) super.retain(increment);
  }

  @Override
  protected void deallocate() {
    shutdown();
    channel.release();
  }

  @Override
  public ReferenceCounted touch(Object o) {
    return this;
  }

  private RxByteStreamStub newByteStreamStub() {
    Context ctx = Context.current();
    return new RxByteStreamStub(
        Single.fromCallable(() -> ctx.call(() -> ByteStreamGrpc.newStub(channel)
            .withInterceptors(TracingMetadataUtils.attachMetadataFromContextInterceptor())
            .withCallCredentials(callCredentialsProvider.getCallCredentials())
            .withDeadlineAfter(callTimeoutSecs, SECONDS))));
  }

  private static String buildUploadResourceName(
      String instanceName, UUID uuid, HashCode hash, long size) {
    String resourceName = format("uploads/%s/blobs/%s/%d", uuid, hash, size);
    if (!Strings.isNullOrEmpty(instanceName)) {
      resourceName = instanceName + "/" + resourceName;
    }
    return resourceName;
  }

  void shutdown() {
    synchronized (lock) {
      if (isShutdown) {
        return;
      }

      isShutdown = true;
      // TODO: Cancel uploadsInProgress
    }
  }

  /*
     TODO: Retry

     Test cases:
        - Retry
        - Cancel
        - gRPC metadata
  */
  public Completable uploadBlob(HashCode hash, Chunker chunker, boolean forceUpload) {
    // Only start the upload on subscription
    return Completable.defer(() -> {
      synchronized (lock) {
        checkState(!isShutdown, "Must not call uploadBlobs after shutdown.");

        if (!forceUpload && uploadedBlobs.contains(hash)) {
          return Completable.complete();
        }

        ConnectableObservable<Object> upload = uploadsInProgress.get(hash);
        if (upload == null) {
          upload = uploadBlob(hash, chunker)
              .onErrorResumeNext(e -> {
                if (e instanceof StatusRuntimeException) {
                  return Completable.error(new IOException(e));
                } else {
                  return Completable.error(e);
                }
              })
              .doFinally(() -> {
                // TODO: If subscribe before this lock and after completion, what happens? NEED TEST
                // TODO: RACE: before this doFinally, after downstream.onComplete
                synchronized (lock) {
                  uploadsInProgress.remove(hash);
                  uploadedBlobs.add(hash);
                }
              })
              .toObservable()
              .publish();

          uploadsInProgress.put(hash, upload);

          upload.connect();
        }

        return Completable.fromObservable(upload);
      }
    });
  }

  private Completable uploadBlob(HashCode hash, Chunker chunker) {
    UUID uploadId = UUID.randomUUID();
    String resourceName = buildUploadResourceName(instanceName, uploadId, hash, chunker.getSize());

    Single<Long> writeResult = writeAndQueryOnFailure(chunker, resourceName)
        .doOnSuccess(committedSize -> {
          long expected = chunker.getSize();
          if (committedSize != expected) {
            String message =
                format(
                    "write incomplete: committed_size %d for %d total", committedSize, expected);
            throw new IOException(message);
          }
        });

    return Completable.fromSingle(writeResult);
  }

  private Single<Long> writeAndQueryOnFailure(Chunker chunker, String resourceName) {
    return write(resourceName, chunker)
        .map(WriteResponse::getCommittedSize)
        .onErrorResumeNext(e -> queryWriteStatus(resourceName)
            .map(QueryWriteStatusResponse::getCommittedSize));
  }

  private Single<WriteResponse> write(String resourceName, Chunker chunker) {
    Observable<WriteRequest> requestObservable = Observable.create(emitter -> {
      boolean isFirst = true;
      while (chunker.hasNext()) {
        WriteRequest.Builder requestBuilder = WriteRequest.newBuilder();
        Chunker.Chunk chunk = chunker.next();

        if (isFirst) {
          // Resource name only needs to be set on the first write for each file.
          requestBuilder.setResourceName(resourceName);
          isFirst = false;
        }

        boolean isLastChunk = !chunker.hasNext();
        WriteRequest request =
            requestBuilder
                .setData(chunk.getData())
                .setWriteOffset(chunk.getOffset())
                .setFinishWrite(isLastChunk)
                .build();

        emitter.onNext(request);
      }

      emitter.onComplete();
    });

    return newByteStreamStub().write(requestObservable);
  }

  private Single<QueryWriteStatusResponse> queryWriteStatus(String resourceName) {
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QueryWriteStatusRequest.newBuilder().setResourceName(resourceName).build());
    return newByteStreamStub().queryWriteStatus(requestSingle);
  }
}
