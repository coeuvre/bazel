package com.google.devtools.build.lib.remote;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.HashCode;
import com.google.devtools.build.lib.remote.RemoteRetrier.ProgressiveBackoff;
import com.google.devtools.build.lib.remote.Retrier.Backoff;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import io.grpc.Context;
import io.grpc.StatusRuntimeException;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;

public class RxByteStreamUploader extends AbstractReferenceCounted {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

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
  boolean uploadsInProgress(HashCode hash) {
    synchronized (lock) {
      return uploadsInProgress.get(hash) != null;
    }
  }

  /**
   * Uploads a BLOB asynchronously to the remote {@code ByteStream} service. The call returns a
   * {@link Completable} and only start the upload on subscription.
   *
   * <p>Upload is retried in case of retriable error. Retrying is transparent to the user of this
   * API.
   *
   * <p>Trying to upload the same BLOB multiple times concurrently, results in only one upload
   * being performed. This is transparent to the user of this API.
   *
   * @param hash the hash of the data to upload.
   * @param chunker the data to upload.
   * @param forceUpload if {@code false} the blob is not uploaded if it has previously been
   * uploaded, if {@code true} the blob is uploaded.
   */
  public Completable uploadBlob(HashCode hash, Chunker chunker, boolean forceUpload) {
    Context ctx = Context.current();
    RxByteStreamStub stub = newByteStreamStub(ctx);

    return Completable.defer(() -> {
      synchronized (lock) {
        Observable<Void> upload = uploadsInProgress.computeIfAbsent(hash, key -> {
          return uploadBlob(stub, key, chunker, forceUpload)
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
              // shared among multiple subscriptions. It will also automatically disposed if no one
              // subscribed.
              .refCount();
        });

        // Upload could complete before downstream subscribe the returned Completable. If it then
        // subscribe, the RefCount will be increased from 0 to 1 resulting in a resubscription to
        // Completable returned by {@link #uploadBlob}. So we check the uploadedBlobs with a lock
        // inside {@link #uploadBlob}.
        return Completable.fromObservable(upload);
      }
    });
  }

  private Completable uploadBlob(RxByteStreamStub stub, HashCode hash, Chunker chunker,
      boolean forceUpload) {
    return Completable.defer(() -> {
      synchronized (lock) {
        if (!forceUpload && uploadedBlobs.contains(hash)) {
          return Completable.complete();
        }
      }

      UUID uploadId = UUID.randomUUID();
      long blobSize = chunker.getSize();
      String resourceName = buildUploadResourceName(instanceName, uploadId, hash, blobSize);

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
            // If the lastCommittedSize we queried from server is equals to blobSize, we assume
            // the upload is completed
            long committedSize = lastCommittedSize.get();
            if (committedSize == blobSize) {
              return Single.just(committedSize);
            } else {
              return Single.error(error);
            }
          })
          .retryWhen(errors -> retrier.retryWhen(errors, backoff))
          .flatMap(committedSize -> {
            if (committedSize != blobSize) {
              String message = format("write incomplete: committed_size %d for %d total",
                  committedSize, blobSize);
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
        .onErrorResumeNext(writeError -> {
          return queryWriteStatus(stub, resourceName)
              .map(QueryWriteStatusResponse::getCommittedSize)
              .doOnSuccess(committedSize -> {
                if (committedSize > lastCommittedSize.getAndSet(committedSize)) {
                  // we have made progress on this upload in the last request,
                  // reset the backoff so that this request has a full deck of retries
                  backoff.reset();
                }
              })
              .onErrorResumeNext(queryError -> {
                writeError.addSuppressed(queryError);
                return Single.error(writeError);
              })
              // Returns the fact that the write is failed
              .flatMap(committedSize -> Single.error(writeError));
        });
  }

  @VisibleForTesting
  static Observable<WriteRequest> newRequestObservable(String resourceName, Chunker chunker) {
    return Observable.create(emitter -> {
      AtomicBoolean cancelled = new AtomicBoolean(false);
      emitter.setCancellable(() -> cancelled.set(true));

      boolean isFirst = true;
      while (!cancelled.get()) {
        if (chunker.hasNext()) {
          WriteRequest.Builder requestBuilder = WriteRequest.newBuilder();
          if (isFirst) {
            requestBuilder.setResourceName(resourceName);
            isFirst = false;
          }
          Chunker.Chunk chunk = chunker.next();
          WriteRequest request = requestBuilder
              .setWriteOffset(chunk.getOffset())
              .setData(chunk.getData())
              .setFinishWrite(!chunker.hasNext())
              .build();
          emitter.onNext(request);
        } else {
          emitter.onComplete();
          break;
        }
      }
    });
  }

  private Single<WriteResponse> write(RxByteStreamStub stub, String resourceName, Chunker chunker) {
    return stub.write(newRequestObservable(resourceName, chunker));
  }

  private Single<QueryWriteStatusResponse> queryWriteStatus(RxByteStreamStub stub,
      String resourceName) {
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QueryWriteStatusRequest.newBuilder().setResourceName(resourceName).build());
    return stub.queryWriteStatus(requestSingle);
  }
}
