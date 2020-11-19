package com.google.devtools.build.lib.remote;

import build.bazel.remote.execution.v2.Digest;
import com.google.common.hash.HashCode;
import io.netty.util.ReferenceCounted;
import io.reactivex.rxjava3.core.Completable;

/** An interface for the ByteStream API. */
public interface RxByteStreamClient extends ReferenceCounted {
  /**
   * Uploads a {@link Chunker} asynchronously to the remote {@code ByteStream} service. The call
   * returns a {@link Completable} and only start the upload on subscription.
   *
   * <p>Upload is retried in case of retriable error. Retrying is transparent to the user of this
   * API.
   *
   * <p>Trying to upload the same BLOB multiple times concurrently, results in only one upload
   * being performed. This is transparent to the user of this API.
   *
   * @param digest the {@link Digest} of the data to upload.
   * @param chunker the data to upload.
   * @param forceUpload if {@code false} the blob is not uploaded if it has previously been
   * uploaded, if {@code true} the blob is uploaded.
   */
  Completable upload(Digest digest, Chunker chunker, boolean forceUpload);

  @Override
  RxByteStreamClient retain();
}
