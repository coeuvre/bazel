// Copyright 2017 The Bazel Authors. All rights reserved.
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

package com.google.devtools.build.lib.remote.common;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.build.lib.vfs.Path;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An interface for a remote caching protocol.
 *
 * <p>Implementations must be thread-safe.
 */
public interface RemoteCacheClient extends MissingDigestsFinder {

  /**
   * A key in the remote action cache. The type wraps around a {@link Digest} of an {@link Action}.
   * Action keys are special in that they aren't content-addressable but refer to action results.
   */
  final class ActionKey {

    private final Digest digest;

    public Digest getDigest() {
      return digest;
    }

    public ActionKey(Digest digest) {
      this.digest = Preconditions.checkNotNull(digest, "digest");
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ActionKey)) {
        return false;
      }

      ActionKey otherKey = (ActionKey) other;
      return digest.equals(otherKey.digest);
    }

    @Override
    public int hashCode() {
      return digest.hashCode();
    }
  }

  /**
   * Downloads an action result for the {@code actionKey}.
   *
   * @param actionKey The digest of the {@link Action} that generated the action result.
   * @param inlineOutErr A hint to the server to inline the stdout and stderr in the {@code
   *     ActionResult} message.
   * @return A {@link Maybe} representing pending download of an action result. If an action result for
   *     {@code actionKey} cannot be found the result of the Maybe is {@code empty}.
   */
  Maybe<ActionResult> downloadActionResult(ActionKey actionKey, boolean inlineOutErr);

  /**
   * Uploads an action result for the {@code actionKey}.
   *
   * @param actionKey The digest of the {@link Action} that generated the action result.
   * @param actionResult The action result to associate with the {@code actionKey}.
   * @return A {@link Completable} representing pending completion of the upload.
   */
  Completable uploadActionResult(ActionKey actionKey, ActionResult actionResult);

  /**
   * Downloads a BLOB for the given {@code digest}.
   *
   * @return A {@link Single} representing pending completion of the download. If a BLOB for {@code
   *     digest} does not exist in the cache the Single fails with a {@link CacheNotFoundException}.
   */
  Single<byte[]> downloadBlob(Digest digest);

  /**
   * Uploads a {@code file} to the CAS.
   *
   * @param digest The digest of the file.
   * @param file The file to upload.
   * @return A future representing pending completion of the upload.
   */
  Completable uploadFile(Digest digest, Path file);

  /**
   * Uploads a BLOB to the CAS.
   *
   * @param digest The digest of the blob.
   * @param data The BLOB to upload.
   * @return A future representing pending completion of the upload.
   */
  Completable uploadBlob(Digest digest, ByteString data);

  /** Close resources associated with the remote cache. */
  void close();
}
