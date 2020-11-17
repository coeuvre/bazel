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

  private RxByteStreamStub newByteStreamStub() {
    Context ctx = Context.current();
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

  /*
     TODO: Test cases
        - Retry
        - Cancel
        - gRPC metadata
  */

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
    return Completable.defer(() -> {
      synchronized (lock) {
        if (!forceUpload && uploadedBlobs.contains(hash)) {
          return Completable.complete();
        }

        Observable<Void> upload = uploadsInProgress.get(hash);
        if (upload == null) {
          UUID uploadId = UUID.randomUUID();
          String resourceName = buildUploadResourceName(instanceName, uploadId, hash,
              chunker.getSize());
          upload = uploadBlob(resourceName, chunker)
              .onErrorResumeNext(e -> {
                if (e instanceof StatusRuntimeException) {
                  return Completable.error(new IOException(e));
                } else {
                  return Completable.error(e);
                }
              })
              .<Void>toObservable()
              .publish()
              .refCount()
              .doOnComplete(() -> {
                synchronized (lock) {
                  uploadedBlobs.add(hash);
                }
              })
              .doFinally(() -> {
                // TODO: If subscribe before this lock and after completion, what happens? NEED TEST
                // TODO: RACE: before this doFinally, after downstream.onComplete
                synchronized (lock) {
                  uploadsInProgress.remove(hash);
                }
              });

          uploadsInProgress.put(hash, upload);
        }

        return Completable.fromObservable(upload);
      }
    });
  }

  private Completable uploadBlob(String resourceName, Chunker chunker) {
    return Completable.defer(() -> {
      AtomicLong lastCommittedSize = new AtomicLong(0);
      ProgressiveBackoff backoff = new ProgressiveBackoff(retrier::newBackoff);

      Single<Long> writeResult = writeAndQueryOnFailure(resourceName,
          Single.fromCallable(() -> {
            try {
              chunker.seek(lastCommittedSize.get());
            } catch (IOException e) {
              chunker.reset();
            }
            return chunker;
          })
          , backoff)
          .doOnSuccess(committedSize -> {
            if (committedSize > lastCommittedSize.getAndSet(committedSize)) {
              // we have made progress on this upload in the last request,
              // reset the backoff so that this request has a full deck of retries
              backoff.reset();
            }

            long expected = chunker.getSize();
            if (committedSize != expected) {
              String message =
                  format(
                      "write incomplete: committed_size %d for %d total", committedSize, expected);
              throw new IOException(message);
            }
          })
          .retryWhen(errors -> retrier.retryWhen(errors, backoff));

      return Completable.fromSingle(writeResult);
    });
  }

  /**
   * Uploads chunks for data from {@link Chunker} using it's current offset and returns the {@code
   * committedSize}.
   *
   * <p>In case a write failed, query the server last {@code committedSize}. Query is retried with
   * given {@link Backoff} in case of retriable errors.
   */
  private Single<Long> writeAndQueryOnFailure(String resourceName, Single<Chunker> chunkerSingle,
      Backoff backoff) {
    return write(resourceName, chunkerSingle)
        .map(WriteResponse::getCommittedSize)
        .onErrorResumeNext(e ->
            queryWriteStatus(resourceName)
                .retryWhen(errors -> retrier.retryWhen(errors, backoff))
                .map(QueryWriteStatusResponse::getCommittedSize));
  }

  private Single<WriteResponse> write(String resourceName, Single<Chunker> chunkerSingle) {
    return chunkerSingle.flatMap(chunker -> {
      Observable<WriteRequest> requestObservable = Observable.create(emitter -> {
        while (chunker.hasNext()) {
          WriteRequest.Builder requestBuilder = WriteRequest.newBuilder();
          Chunker.Chunk chunk = chunker.next();
          WriteRequest request = requestBuilder
              .setResourceName(resourceName)
              .setWriteOffset(chunk.getOffset())
              .setData(chunk.getData())
              .setFinishWrite(!chunker.hasNext())
              .build();
          emitter.onNext(request);
        }
        emitter.onComplete();
      });
      return newByteStreamStub().write(requestObservable);
    });
  }

  private Single<QueryWriteStatusResponse> queryWriteStatus(String resourceName) {
    Single<QueryWriteStatusRequest> requestSingle = Single
        .just(QueryWriteStatusRequest.newBuilder().setResourceName(resourceName).build());
    return newByteStreamStub().queryWriteStatus(requestSingle);
  }
}
