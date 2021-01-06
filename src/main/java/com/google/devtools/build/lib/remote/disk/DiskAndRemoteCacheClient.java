// Copyright 2019 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.remote.disk;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.devtools.build.lib.remote.common.CacheNotFoundException;
import com.google.devtools.build.lib.remote.common.RemoteCacheClient;
import com.google.devtools.build.lib.remote.options.RemoteOptions;
import com.google.devtools.build.lib.vfs.Path;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * A {@link RemoteCacheClient} implementation combining two blob stores. A local disk blob store and
 * a remote blob store. If a blob isn't found in the first store, the second store is used, and the
 * blob added to the first. Put puts the blob on both stores.
 */
public final class DiskAndRemoteCacheClient implements RemoteCacheClient {

  private final RemoteCacheClient remoteCache;
  private final DiskCacheClient diskCache;
  private final RemoteOptions options;

  public DiskAndRemoteCacheClient(
      DiskCacheClient diskCache, RemoteCacheClient remoteCache, RemoteOptions options) {
    this.diskCache = Preconditions.checkNotNull(diskCache);
    this.remoteCache = Preconditions.checkNotNull(remoteCache);
    this.options = options;
  }

  @Override
  public Completable uploadActionResult(ActionKey actionKey, ActionResult actionResult) {
    Completable diskUpload = diskCache.uploadActionResult(actionKey, actionResult);
    Completable remoteUpload = Completable.complete();
    if (!options.incompatibleRemoteResultsIgnoreDisk || options.remoteUploadLocalResults) {
      remoteUpload = remoteCache.uploadActionResult(actionKey, actionResult);
    }

    return Completable.mergeArray(diskUpload, remoteUpload);
  }

  @Override
  public void close() {
    diskCache.close();
    remoteCache.close();
  }

  @Override
  public Completable uploadFile(Digest digest, Path file) {
    Completable diskUpload = diskCache.uploadFile(digest, file);
    Completable remoteUpload = Completable.complete();
    if (!options.incompatibleRemoteResultsIgnoreDisk || options.remoteUploadLocalResults) {
      remoteUpload = remoteCache.uploadFile(digest, file);
    }
    return Completable.mergeArray(diskUpload, remoteUpload);
  }

  @Override
  public Completable uploadBlob(Digest digest, ByteString data) {
    Completable diskUpload = diskCache.uploadBlob(digest, data);
    Completable remoteUpload = Completable.complete();
    if (!options.incompatibleRemoteResultsIgnoreDisk || options.remoteUploadLocalResults) {
      remoteUpload = remoteCache.uploadBlob(digest, data);
    }
    return Completable.mergeArray(diskUpload, remoteUpload);
  }

  @Override
  public Single<ImmutableSet<Digest>> findMissingDigests(Iterable<Digest> digests) {
    Single<ImmutableSet<Digest>> remoteQuery = remoteCache.findMissingDigests(digests);
    Single<ImmutableSet<Digest>> diskQuery = diskCache.findMissingDigests(digests);
    return Single.mergeArray(remoteQuery, diskQuery)
        .<ImmutableSet.Builder<Digest>>collect(ImmutableSet::builder, ImmutableSet.Builder::addAll)
        .map(ImmutableSet.Builder::build);
  }

  private Path newTempPath() {
    return diskCache.toPathNoSplit(UUID.randomUUID().toString());
  }

  private static ListenableFuture<Void> closeStreamOnError(
      ListenableFuture<Void> f, OutputStream out) {
    return Futures.catchingAsync(
        f,
        Exception.class,
        (rootCause) -> {
          try {
            out.close();
          } catch (IOException e) {
            rootCause.addSuppressed(e);
          }
          return Futures.immediateFailedFuture(rootCause);
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public Single<byte[]> downloadBlob(Digest digest) {
    if (diskCache.contains(digest)) {
      return diskCache.downloadBlob(digest);
    }

    if (!options.incompatibleRemoteResultsIgnoreDisk || options.remoteAcceptCached) {
      return remoteCache
          .downloadBlob(digest)
          .doOnSuccess(
              bytes -> {
                Path tempPath = newTempPath();
                try (OutputStream out = tempPath.getOutputStream()) {
                  out.write(bytes);
                }
                diskCache.captureFile(tempPath, digest, /* isActionCache= */ false);
              })
          .flatMap((unused) -> diskCache.downloadBlob(digest));
    } else {
      return Single.error(new CacheNotFoundException(digest));
    }
  }

  @Override
  public Maybe<ActionResult> downloadActionResult(ActionKey actionKey, boolean inlineOutErr) {
    if (diskCache.containsActionResult(actionKey)) {
      return diskCache.downloadActionResult(actionKey, inlineOutErr);
    }

    if (!options.incompatibleRemoteResultsIgnoreDisk || options.remoteAcceptCached) {
      return remoteCache
          .downloadActionResult(actionKey, inlineOutErr)
          .flatMapSingle(
              actionResult ->
                  diskCache
                      .uploadActionResult(actionKey, actionResult)
                      .toSingle(() -> actionResult));
    } else {
      return Maybe.empty();
    }
  }
}
