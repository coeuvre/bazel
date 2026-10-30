// Copyright 2026 The Bazel Authors. All rights reserved.
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

package com.google.devtools.build.lib.sandbox;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.devtools.build.lib.remote.util.DigestUtil.isOldStyleDigestFunction;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ascii;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.build.lib.remote.common.RemoteActionExecutionContext;
import com.google.devtools.build.lib.remote.merkletree.MerkleTree;
import com.google.devtools.build.lib.remote.merkletree.MerkleTreeUploader;
import com.google.devtools.build.lib.remote.options.RemoteOptions;
import com.google.devtools.build.lib.runtime.CommandEnvironment;
import com.google.devtools.build.lib.util.DeterministicWriter;
import com.google.devtools.build.lib.vfs.FileSystemUtils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.Symlinks;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link MerkleTreeUploader} that populates in-memory blobs ({@link Directory}, {@code
 * VirtualActionInput}, {@link Command}, {@link Action}) and missing input files directly into the
 * shared local CAS directory ({@code --disk_cache}, using {@code
 * com.google.devtools.build.lib.remote.disk.DiskCacheClient}'s exact {@code
 * [<digest_function>/]cas/<hash[0..2]>/<hash>} layout) with zero gRPC data-plane traffic.
 */
public final class DiskCacheMerkleTreeUploader implements MerkleTreeUploader {

  private static final String JVM_TMP_PREFIX = UUID.randomUUID().toString();
  private static final AtomicLong TMP_SEQ = new AtomicLong();
  private static final int MAX_POPULATED_DIGESTS_CACHE_SIZE = 200_000;

  private final Path diskCacheRoot;
  private final Path casRoot;
  private final Path tmpRoot;
  private final ConcurrentHashMap<Digest, Object> inFlightUploads = new ConcurrentHashMap<>();
  private final Set<Digest> populatedDigests =
      Collections.newSetFromMap(
          Caffeine.newBuilder()
              .maximumSize(MAX_POPULATED_DIGESTS_CACHE_SIZE)
              .<Digest, Boolean>build()
              .asMap());
  private final AtomicInteger uploadedBlobCount = new AtomicInteger();
  private final AtomicInteger uploadedFileCount = new AtomicInteger();
  private final AtomicInteger diskCacheHitCount = new AtomicInteger();
  private final AtomicLong uploadedFileBytes = new AtomicLong();

  public DiskCacheMerkleTreeUploader(Path diskCacheRoot, DigestFunction.Value digestFunction) {
    this.diskCacheRoot = checkNotNull(diskCacheRoot);
    Path fnRoot =
        isOldStyleDigestFunction(digestFunction)
            ? diskCacheRoot
            : diskCacheRoot.getChild(
                Ascii.toLowerCase(digestFunction.getValueDescriptor().getName()));
    this.casRoot = fnRoot.getChild("cas");
    this.tmpRoot = diskCacheRoot.getChild("tmp");
    try {
      casRoot.createDirectoryAndParents();
      tmpRoot.createDirectoryAndParents();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to initialize shared disk_cache CAS directories under " + diskCacheRoot, e);
    }
  }

  /**
   * Resolves the shared local disk cache root directory from {@code --disk_cache} ({@link
   * RemoteOptions#getDiskCachePath(Path)}) when {@code --disk_cache} is enabled and not overridden
   * by {@code --experimental_remote_output_service}, defaulting to {@code
   * <outputBase>/sandbox/reapi-disk-cache} (cleaned up by {@link SandboxModule}) when {@code
   * --disk_cache} is not enabled.
   */
  public static Path resolveDiskCacheRoot(CommandEnvironment cmdEnv) {
    RemoteOptions remoteOptions = cmdEnv.getOptions().getOptions(RemoteOptions.class);
    if (remoteOptions != null
        && Strings.isNullOrEmpty(remoteOptions.getRemoteOutputService())
        && remoteOptions.isDiskCacheEnabled()) {
      Path outputUserRoot =
          cmdEnv.getDirectories() != null
                  && cmdEnv.getDirectories().getServerDirectories() != null
                  && cmdEnv.getDirectories().getServerDirectories().getOutputUserRoot() != null
              ? cmdEnv.getDirectories().getServerDirectories().getOutputUserRoot()
              : cmdEnv.getOutputBase().getParentDirectory();
      PathFragment diskCacheFragment = remoteOptions.getDiskCachePath(outputUserRoot);
      if (diskCacheFragment != null) {
        Path base =
            cmdEnv.getWorkingDirectory() != null
                ? cmdEnv.getWorkingDirectory()
                : cmdEnv.getOutputBase();
        return base.getRelative(diskCacheFragment);
      }
    }
    return cmdEnv.getOutputBase().getRelative("sandbox").getRelative("reapi-disk-cache");
  }

  public Path getDiskCacheRoot() {
    return diskCacheRoot;
  }

  public Path getCasRoot() {
    return casRoot;
  }

  public Path toCasPath(Digest digest) {
    return toCasPath(digest.getHash());
  }

  public Path toCasPath(String hash) {
    return casRoot.getChild(hash.substring(0, 2)).getChild(hash);
  }

  void verifyDiskCacheDirectory() {
    try {
      if (!casRoot.exists(Symlinks.NOFOLLOW)) {
        populatedDigests.clear();
        casRoot.createDirectoryAndParents();
        tmpRoot.createDirectoryAndParents();
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to re-initialize shared disk_cache CAS directories under " + diskCacheRoot, e);
    }
  }

  @VisibleForTesting
  public int getUploadedBlobCount() {
    return uploadedBlobCount.get();
  }

  @VisibleForTesting
  public int getUploadedFileCount() {
    return uploadedFileCount.get();
  }

  @VisibleForTesting
  public int getDiskCacheHitCount() {
    return diskCacheHitCount.get();
  }

  @VisibleForTesting
  public long getUploadedFileBytes() {
    return uploadedFileBytes.get();
  }

  void markPopulated(Digest digest) {
    if (digest.getSizeBytes() > 0) {
      populatedDigests.add(digest);
    }
  }

  void evictByHashes(Set<String> missingHashes) {
    if (missingHashes.isEmpty()) {
      populatedDigests.clear();
      return;
    }
    populatedDigests.removeIf(d -> missingHashes.contains(d.getHash()));
  }

  byte[] readBlobFromCas(Digest digest) throws IOException {
    if (digest.getSizeBytes() == 0) {
      return new byte[0];
    }
    Path target = toCasPath(digest);
    return FileSystemUtils.readContent(target);
  }

  void streamBlobFromCas(Digest digest, OutputStream out) throws IOException {
    if (digest.getSizeBytes() == 0) {
      return;
    }
    Path target = toCasPath(digest);
    try (InputStream in = target.getInputStream()) {
      in.transferTo(out);
    }
  }

  private static boolean refresh(Path path) throws IOException {
    try {
      path.setLastModifiedTime(Path.NOW_SENTINEL_TIME);
      return true;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Atomically moves a temporary file (already written/copied inside {@code <disk_cache>/tmp}) to
   * {@code target} ({@code <disk_cache>/cas/<xx>/<hash>}) and unlinks {@code temp}.
   *
   * <p>Using {@code link(temp, target) + unlink(temp)} on the temporary copy leaves {@code target}
   * with {@code nlink == 1} (never sharing an inode with {@code execRoot}) while avoiding cross-
   * directory {@code rename(2)} contention on the Linux kernel's global {@code s_vfs_rename_mutex}.
   */
  private static void publishTempToCasAtomically(Path temp, Path target) throws IOException {
    java.nio.file.Path tempNio = temp.getFileSystem().getNioPath(temp.asFragment());
    java.nio.file.Path targetNio = target.getFileSystem().getNioPath(target.asFragment());
    if (tempNio != null && targetNio != null) {
      try {
        java.nio.file.Files.createLink(targetNio, tempNio);
        temp.delete();
        return;
      } catch (java.nio.file.FileAlreadyExistsException e) {
        temp.delete();
        return;
      } catch (IOException ignored) {
        // Fall back to rename if hardlinking is not supported on this filesystem.
      }
    }
    FileSystemUtils.renameToleratingConcurrentCreation(temp, target);
  }

  private void ensureBlobInCas(Digest digest, byte[] data, boolean force) throws IOException {
    if (digest.getSizeBytes() == 0) {
      return;
    }
    if (!force && populatedDigests.contains(digest)) {
      return;
    }
    Object lock = inFlightUploads.computeIfAbsent(digest, k -> new Object());
    synchronized (lock) {
      try {
        if (!force && populatedDigests.contains(digest)) {
          return;
        }
        Path target = toCasPath(digest);
        if (refresh(target)) {
          if (populatedDigests.add(digest) && !force) {
            diskCacheHitCount.incrementAndGet();
          }
          return;
        }
        Path temp = tmpRoot.getChild(JVM_TMP_PREFIX + "-blob-" + TMP_SEQ.incrementAndGet());
        try {
          FileSystemUtils.writeContent(temp, data);
          target.getParentDirectory().createDirectoryAndParents();
          publishTempToCasAtomically(temp, target);
          populatedDigests.add(digest);
          uploadedBlobCount.incrementAndGet();
        } catch (IOException e) {
          try {
            temp.delete();
          } catch (IOException suppressed) {
            e.addSuppressed(suppressed);
          }
          throw e;
        }
      } finally {
        inFlightUploads.remove(digest, lock);
      }
    }
  }

  private static void copyFileContentOnly(Path from, Path to) throws IOException {
    java.nio.file.Path fromNio = from.getFileSystem().getNioPath(from.asFragment());
    java.nio.file.Path toNio = to.getFileSystem().getNioPath(to.asFragment());
    if (fromNio != null && toNio != null) {
      java.nio.file.Files.copy(
          fromNio, toNio, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      return;
    }
    FileSystemUtils.copyFile(from, to);
    to.chmod(0644);
    to.setLastModifiedTime(Path.NOW_SENTINEL_TIME);
  }

  private void ensureFileInCas(Digest digest, Path file, boolean force) throws IOException {
    if (digest.getSizeBytes() == 0) {
      return;
    }
    if (!force && populatedDigests.contains(digest)) {
      return;
    }
    Object lock = inFlightUploads.computeIfAbsent(digest, k -> new Object());
    synchronized (lock) {
      try {
        if (!force && populatedDigests.contains(digest)) {
          return;
        }
        boolean executable = file.isExecutable();
        Path target = toCasPath(digest);
        if (refresh(target)) {
          if (executable && !target.isExecutable()) {
            target.chmod(0755);
          }
          if (populatedDigests.add(digest) && !force) {
            diskCacheHitCount.incrementAndGet();
          }
          return;
        }
        Path temp = tmpRoot.getChild(JVM_TMP_PREFIX + "-file-" + TMP_SEQ.incrementAndGet());
        try {
          copyFileContentOnly(file, temp);
          if (executable) {
            temp.chmod(0755);
          }
          target.getParentDirectory().createDirectoryAndParents();
          publishTempToCasAtomically(temp, target);
          if (executable && !target.isExecutable()) {
            target.chmod(0755);
          }
          populatedDigests.add(digest);
          uploadedFileCount.incrementAndGet();
          uploadedFileBytes.addAndGet(digest.getSizeBytes());
        } catch (IOException e) {
          try {
            temp.delete();
          } catch (IOException suppressed) {
            e.addSuppressed(suppressed);
          }
          throw e;
        }
      } finally {
        inFlightUploads.remove(digest, lock);
      }
    }
  }

  @Override
  public ListenableFuture<Void> uploadBlob(
      RemoteActionExecutionContext context, Digest digest, byte[] data, boolean force) {
    try {
      ensureBlobInCas(digest, data, force);
      return immediateVoidFuture();
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }
  }

  @Override
  public ListenableFuture<Void> uploadFile(
      RemoteActionExecutionContext context,
      Digest digest,
      Path path,
      PathFragment execPath,
      boolean force) {
    try {
      ensureFileInCas(digest, path, force);
      return immediateVoidFuture();
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }
  }

  @Override
  public ListenableFuture<Void> uploadDeterministicWriter(
      RemoteActionExecutionContext context,
      Digest digest,
      DeterministicWriter deterministicWriter,
      boolean force) {
    if (digest.getSizeBytes() == 0) {
      return immediateVoidFuture();
    }
    if (!force && populatedDigests.contains(digest)) {
      return immediateVoidFuture();
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream((int) digest.getSizeBytes());
    try {
      deterministicWriter.writeTo(out);
      ensureBlobInCas(digest, out.toByteArray(), force);
      return immediateVoidFuture();
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }
  }

  @Override
  public void ensureInputsPresent(
      RemoteActionExecutionContext context, MerkleTree.Uploadable merkleTree, boolean force)
      throws IOException, InterruptedException {
    for (Digest digest : merkleTree.allDigests()) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      Optional<ListenableFuture<Void>> future = merkleTree.upload(this, context, digest, force);
      if (future.isPresent()) {
        try {
          future.get().get();
        } catch (ExecutionException e) {
          if (e.getCause() instanceof IOException ioEx) {
            throw ioEx;
          }
          throw new IOException(
              "Failed to prepare Merkle tree blob " + digest.getHash(), e.getCause());
        }
      }
    }
  }

  void uploadActionBlobs(
      Digest commandDigest,
      byte[] commandBytes,
      Digest actionDigest,
      byte[] actionBytes,
      boolean force)
      throws IOException {
    ensureBlobInCas(commandDigest, commandBytes, force);
    ensureBlobInCas(actionDigest, actionBytes, force);
  }
}
