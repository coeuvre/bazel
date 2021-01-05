package com.google.devtools.build.lib.remote;

import build.bazel.remote.execution.v2.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.devtools.build.lib.remote.common.RemoteCacheClient;
import com.google.devtools.build.lib.remote.options.RemoteOptions;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import com.google.devtools.build.lib.remote.util.Utils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Status;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

public class RxGrpcCacheClient implements RemoteCacheClient {

  private final ReferenceCountedChannel channel;
  private final RxRemoteRetrier retrier;
  private final RemoteOptions options;
  private final DigestUtil digestUtil;
  private final RxByteStreamClient byteStreamClient;
  private final int maxMissingBlobsDigestsPerMessage;

  private final AtomicBoolean closed = new AtomicBoolean();

  public RxGrpcCacheClient(
      ReferenceCountedChannel channel,
      RxRemoteRetrier retrier,
      RemoteOptions options,
      DigestUtil digestUtil,
      RxByteStreamClient byteStreamClient) {
    this.channel = channel;
    this.retrier = retrier;
    this.options = options;
    this.digestUtil = digestUtil;
    this.byteStreamClient = byteStreamClient;

    maxMissingBlobsDigestsPerMessage = computeMaxMissingBlobsDigestsPerMessage();
    Preconditions.checkState(
        maxMissingBlobsDigestsPerMessage > 0, "Error: gRPC message size too small.");
  }

  private int computeMaxMissingBlobsDigestsPerMessage() {
    final int overhead =
        FindMissingBlobsRequest.newBuilder()
            .setInstanceName(options.remoteInstanceName)
            .build()
            .getSerializedSize();
    final int tagSize =
        FindMissingBlobsRequest.newBuilder()
                .addBlobDigests(Digest.getDefaultInstance())
                .build()
                .getSerializedSize()
            - FindMissingBlobsRequest.getDefaultInstance().getSerializedSize();
    // We assume all non-empty digests have the same size. This is true for fixed-length hashes.
    final int digestSize = digestUtil.compute(new byte[] {1}).getSerializedSize() + tagSize;
    return (options.maxOutboundMessageSize - overhead) / digestSize;
  }

  private RxActionCacheStub newActionCacheStub(Context ctx) {
    return new RxActionCacheStub(
        Single.fromCallable(
            () ->
                ctx.call(
                    () ->
                        ActionCacheGrpc.newStub(channel)
                            .withInterceptors(
                                TracingMetadataUtils.attachMetadataFromContextInterceptor())
                            .withCallCredentials(retrier.getCallCredentials())
                            .withDeadlineAfter(
                                options.remoteTimeout.getSeconds(), TimeUnit.SECONDS))));
  }

  private RxContentAddressableStorageStub newCasStub(Context ctx) {
    return new RxContentAddressableStorageStub(
        Single.fromCallable(
            () ->
                ctx.call(
                    () ->
                        ContentAddressableStorageGrpc.newStub(channel)
                            .withInterceptors(
                                TracingMetadataUtils.attachMetadataFromContextInterceptor())
                            .withCallCredentials(retrier.getCallCredentials())
                            .withDeadlineAfter(
                                options.remoteTimeout.getSeconds(), TimeUnit.SECONDS))));
  }

  public Maybe<ActionResult> downloadActionResultRx(
      RemoteCacheClient.ActionKey actionKey, boolean inlineOutErr) {
    Context ctx = Context.current();
    RxActionCacheStub stub = newActionCacheStub(ctx);
    Single<GetActionResultRequest> requestSingle =
        Single.just(
            GetActionResultRequest.newBuilder()
                .setInstanceName(options.remoteInstanceName)
                .setActionDigest(actionKey.getDigest())
                .setInlineStderr(inlineOutErr)
                .setInlineStdout(inlineOutErr)
                .build());

    return stub.getActionResult(requestSingle)
        .toMaybe()
        .onErrorResumeNext(
            (error) -> {
              Status status = Status.fromThrowable(error);
              if (status.getCode() == Status.Code.NOT_FOUND) {
                return Maybe.empty();
              }

              return Maybe.error(error);
            })
        .retryWhen(retrier::retryWhen);
  }

  @Override
  public ListenableFuture<ActionResult> downloadActionResult(
      ActionKey actionKey, boolean inlineOutErr) {
    return RxFutures.toListenableFuture(
        downloadActionResultRx(actionKey, inlineOutErr), MoreExecutors.directExecutor());
  }

  public Completable uploadActionResultRx(ActionKey actionKey, ActionResult actionResult) {
    Context ctx = Context.current();
    RxActionCacheStub stub = newActionCacheStub(ctx);
    Single<UpdateActionResultRequest> requestSingle =
        Single.just(
            UpdateActionResultRequest.newBuilder()
                .setInstanceName(options.remoteInstanceName)
                .setActionDigest(actionKey.getDigest())
                .setActionResult(actionResult)
                .build());

    return Completable.fromSingle(
        stub.updateActionResult(requestSingle).retryWhen(retrier::retryWhen));
  }

  @Override
  public void uploadActionResult(ActionKey actionKey, ActionResult actionResult)
      throws IOException, InterruptedException {
    Utils.getFromFuture(
        RxFutures.toListenableFuture(
            uploadActionResultRx(actionKey, actionResult), MoreExecutors.directExecutor()));
  }

  public Single<byte[]> downloadBlobRx(Digest digest) {
    return byteStreamClient
        .download(digest)
        .doOnSuccess(
            blob -> {
              if (options.remoteVerifyDownloads) {
                Utils.verifyBlobContents(digest, digestUtil.compute(blob));
              }
            });
  }

  @Override
  public ListenableFuture<Void> downloadBlob(Digest digest, OutputStream out) {
    return RxFutures.toListenableFuture(
        Completable.fromSingle(
            downloadBlobRx(digest)
                .doOnSuccess(
                    blob -> {
                      out.write(blob);
                      out.flush();
                    })),
        MoreExecutors.directExecutor());
  }

  @Override
  public ListenableFuture<Void> uploadFile(Digest digest, Path path) {
    Completable upload =
        byteStreamClient
            .upload(
                digest,
                Chunker.builder().setInput(digest.getSizeBytes(), path).build(),
                /* forceUpload= */ true)
            .onErrorResumeNext(
                error ->
                    Completable.error(
                        new IOException(format("Error while uploading file: %s", path), error)));
    return RxFutures.toListenableFuture(upload, MoreExecutors.directExecutor());
  }

  @Override
  public ListenableFuture<Void> uploadBlob(Digest digest, ByteString data) {
    Completable upload =
        byteStreamClient
            .upload(
                digest,
                Chunker.builder().setInput(data.toByteArray()).build(),
                /* forceUpload= */ true)
            .onErrorResumeNext(
                error ->
                    Completable.error(
                        new IOException(
                            format(
                                "Error while uploading blob with digest '%s/%s'",
                                digest.getHash(), digest.getSizeBytes()),
                            error)));
    return RxFutures.toListenableFuture(upload, MoreExecutors.directExecutor());
  }

  @Override
  public void close() {
    if (closed.getAndSet(true)) {
      return;
    }
    byteStreamClient.release();
    channel.release();
  }

  public Single<ImmutableSet<Digest>> findMissingDigestsRx(Iterable<Digest> digests) {
    if (Iterables.isEmpty(digests)) {
      return Single.just(ImmutableSet.of());
    }

    Context ctx = Context.current();
    RxContentAddressableStorageStub stub = newCasStub(ctx);

    return Observable.fromIterable(digests)
        .buffer(maxMissingBlobsDigestsPerMessage)
        .map(
            splitDigests -> {
              FindMissingBlobsRequest.Builder requestBuilder =
                  FindMissingBlobsRequest.newBuilder().setInstanceName(options.remoteInstanceName);
              for (Digest digest : splitDigests) {
                requestBuilder.addBlobDigests(digest);
              }
              return requestBuilder.build();
            })
        .flatMapSingle(
            request ->
                stub.findMissingBlobs(Single.just(request))
                    .retryWhen(retrier::retryWhen), /* delayErrors */
            false)
        .<ImmutableSet.Builder<Digest>>collect(
            ImmutableSet::builder,
            (builder, response) -> builder.addAll(response.getMissingBlobDigestsList()))
        .map(ImmutableSet.Builder::build)
        .onErrorResumeNext(
            error -> {
              RequestMetadata requestMetadata = ctx.call(TracingMetadataUtils::fromCurrentContext);
              return Single.error(
                  new IOException(
                      String.format(
                          "findMissingBlobs for %s: %s",
                          requestMetadata.getActionId(), error.getMessage()),
                      error));
            });
  }

  @Override
  public ListenableFuture<ImmutableSet<Digest>> findMissingDigests(Iterable<Digest> digests) {
    return RxFutures.toListenableFuture(
        findMissingDigestsRx(digests), MoreExecutors.directExecutor());
  }
}
