package com.google.devtools.build.lib.remote;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.Digest;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.Futures;
import com.google.devtools.build.lib.remote.RemoteRetrier.ProgressiveBackoff;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.reactivex.rxjava3.core.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;

/**
 * A implementation for {@link RxByteStreamClient}
 */
public class RxByteStreamUploader extends AbstractReferenceCounted implements RxByteStreamClient {

  private final String instanceName;
  private final ReferenceCountedChannel channel;
  private final long callTimeoutSecs;
  private final RxRemoteRetrier retrier;

  private final Object lock = new Object();

  /**
   * Contains the hash codes of already uploaded blobs.
   **/
  @GuardedBy("lock")
  private final Set<HashCode> uploadedBlobs = new HashSet<>();

  @GuardedBy("lock")
  private final Map<HashCode, Observable<Void>> uploadsInProgress = new HashMap<>();

  public RxByteStreamUploader(String instanceName, ReferenceCountedChannel channel,
      long callTimeoutSecs,
      RxRemoteRetrier retrier) {
    this.instanceName = instanceName;
    this.channel = channel;
    this.callTimeoutSecs = callTimeoutSecs;
    this.retrier = retrier;
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
    channel.release();
  }

  @Override
  public ReferenceCounted touch(Object o) {
    return this;
  }

  private RxByteStreamStub newByteStreamStub(Context ctx) {
    return new RxByteStreamStub(
        Single.fromCallable(() -> ctx.call(() -> ByteStreamGrpc.newStub(channel)
            .withInterceptors(TracingMetadataUtils.attachMetadataFromContextInterceptor())
            .withCallCredentials(retrier.getCallCredentials())
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

  @VisibleForTesting
  boolean uploadsInProgress() {
    synchronized (lock) {
      return !uploadsInProgress.isEmpty();
    }
  }

  @VisibleForTesting
  boolean uploadsInProgress(Digest digest) {
    synchronized (lock) {
      return uploadsInProgress.get(HashCode.fromString(digest.getHash())) != null;
    }
  }

  @Override
  public Completable upload(Digest digest, Chunker chunker, boolean forceUpload) {
    Context ctx = Context.current();
    RxByteStreamStub stub = newByteStreamStub(ctx);

    return Completable.defer(() -> {
      synchronized (lock) {
        checkState(chunker.getSize() == digest.getSizeBytes(), String.format(
            "Expected chunker size of %d, got %d",
            digest.getSizeBytes(), chunker.getSize()));
        HashCode hash = HashCode.fromString(digest.getHash());
        Observable<Void> upload = uploadsInProgress.computeIfAbsent(hash, key -> {
          return upload(stub, key, chunker, forceUpload)
              .onErrorResumeNext(e -> {
                if (e instanceof StatusRuntimeException) {
                  return Completable.error(new IOException(e));
                } else {
                  return Completable.error(e);
                }
              })
              .doOnComplete(() -> {
                synchronized (lock) {
                  uploadedBlobs.add(hash);
                }
              })
              .doFinally(() -> {
                synchronized (lock) {
                  uploadsInProgress.remove(hash);
                }
              })
              .<Void>toObservable()
              .publish()
              // Convert to RefCount so the upload is automatically started on subscription and
              // shared among multiple subscriptions. It will also automatically dispose if no one
              // subscribed.
              .refCount();
        });

        // Upload could complete before downstream subscribe the returned Completable. If it then
        // subscribe, the RefCount will be increased from 0 to 1 resulting in a resubscription to
        // Completable returned by {@link #upload}. So we check the uploadedBlobs with a lock
        // inside {@link #upload}
        return Completable.fromObservable(upload);
      }
    });
  }

  private Completable upload(RxByteStreamStub stub, HashCode hash, Chunker chunker,
      boolean forceUpload) {
    return Completable.defer(() -> {
      synchronized (lock) {
        if (!forceUpload && uploadedBlobs.contains(hash)) {
          return Completable.complete();
        }
      }

      UUID uploadId = UUID.randomUUID();
      long totalSize = chunker.getSize();
      String resourceName = buildUploadResourceName(instanceName, uploadId, hash, totalSize);

      AtomicLong lastCommittedSize = new AtomicLong(0);
      ProgressiveBackoff backoff = new ProgressiveBackoff(retrier::newBackoff);

      // Using Single.just here is fine since we ensured that only one upload exists at one
      // time, we won't access chunker concurrently.
      Single<Long> writeResult = Single.just(chunker)
          .doOnSuccess(ch -> {
            if (ch.getOffset() != lastCommittedSize.get()) {
              ch.seek(lastCommittedSize.get());
            }
          })
          .flatMap(ch -> writeAndQueryOnFailure(stub, resourceName, ch, lastCommittedSize, backoff))
          .onErrorResumeNext(error -> {
            // If the lastCommittedSize we queried from server is equals to totalSize, we assume
            // the upload is completed
            long committedSize = lastCommittedSize.get();
            if (committedSize == totalSize) {
              return Single.just(committedSize);
            } else {
              return Single.error(error);
            }
          })
          .retryWhen(errors -> retrier.retryWhen(errors, backoff))
          .flatMap(committedSize -> {
            if (committedSize != totalSize) {
              String message = format("write incomplete: committed_size %d for %d total",
                  committedSize, totalSize);
              return Single.error(new IOException(message));
            }
            return Single.just(committedSize);
          });

      return Completable.fromSingle(writeResult);
    });
  }

  /**
   * Uploads chunks for data from {@link Chunker} using it's current offset and returns the {@code
   * committedSize}.
   *
   * <p>In case a write failed, query the server for the last {@code committedSize} and update
   * {@code lastCommittedSize} accordingly.
   */
  private Single<Long> writeAndQueryOnFailure(RxByteStreamStub stub, String resourceName,
      Chunker chunker, AtomicLong lastCommittedSize, ProgressiveBackoff backoff) {
    return write(stub, resourceName, chunker)
        .map(WriteResponse::getCommittedSize)
        .doOnSuccess(lastCommittedSize::set)
        .onErrorResumeNext(writeError ->
            query(stub, resourceName, lastCommittedSize, backoff, writeError));
  }

  private Single<Long> query(RxByteStreamStub stub, String resourceName,
      AtomicLong lastCommittedSize, ProgressiveBackoff backoff, Throwable writeError) {
    return queryWriteStatus(stub, resourceName)
        .map(QueryWriteStatusResponse::getCommittedSize)
        .onErrorResumeNext(queryError -> {
          Status status = Status.fromThrowable(queryError);
          if (status.getCode() == Code.UNIMPLEMENTED) {
            // if the bytestream server does not implement the query, insist
            // that we should reset the upload
            return Single.just(0L);
          } else {
            writeError.addSuppressed(queryError);
            return Single.error(writeError);
          }
        })
        .doOnSuccess(committedSize -> {
          if (committedSize > lastCommittedSize.getAndSet(committedSize)) {
            // we have made progress on this upload in the last request,
            // reset the backoff so that this request has a full deck of retries
            backoff.reset();
          }
        })
        // Returns the fact that the write is failed
        .flatMap(committedSize -> Single.error(writeError));
  }

  static class WriteRequestGenerator {
    private final String resourceName;
    private final Chunker chunker;
    private boolean isFirst = true;

    WriteRequestGenerator(String resourceName, Chunker chunker) {
      this.resourceName = resourceName;
      this.chunker = chunker;
    }

    void generate(Emitter<WriteRequest> emitter) throws IOException {
      if (chunker.hasNext()) {
        WriteRequest.Builder requestBuilder = WriteRequest.newBuilder();
        if (isFirst) {
          requestBuilder.setResourceName(resourceName);
          isFirst = false;
        }
        Chunker.Chunk chunk = chunker.next();
        WriteRequest request =
            requestBuilder
                .setWriteOffset(chunk.getOffset())
                .setData(chunk.getData())
                .setFinishWrite(!chunker.hasNext())
                .build();
        emitter.onNext(request);
      } else {
        emitter.onComplete();
      }
    }
  }

  @VisibleForTesting
  static Flowable<WriteRequest> newRequestFlowable(String resourceName, Chunker chunker) {
    return Flowable.generate(
        () -> new WriteRequestGenerator(resourceName, chunker), WriteRequestGenerator::generate);
  }

  private Single<WriteResponse> write(RxByteStreamStub stub, String resourceName, Chunker chunker) {
    return stub.write(newRequestFlowable(resourceName, chunker));
  }

  private Single<QueryWriteStatusResponse> queryWriteStatus(RxByteStreamStub stub,
      String resourceName) {
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QueryWriteStatusRequest.newBuilder().setResourceName(resourceName).build());
    return stub.queryWriteStatus(requestSingle);
  }
}
