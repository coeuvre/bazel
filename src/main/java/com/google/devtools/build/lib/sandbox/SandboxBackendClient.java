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
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.CapabilitiesGrpc;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionCapabilities;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.GetCapabilitiesRequest;
import build.bazel.remote.execution.v2.OutputDirectory;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.OutputSymlink;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.bazel.remote.execution.v2.Tree;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.Artifact.SpecialArtifact;
import com.google.devtools.build.lib.actions.Artifact.TreeFileArtifact;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.actions.FileContentsProxy;
import com.google.devtools.build.lib.actions.OutputMetadataStore;
import com.google.devtools.build.lib.actions.Spawn;
import com.google.devtools.build.lib.actions.UserExecException;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import com.google.devtools.build.lib.runtime.CommandEnvironment;
import com.google.devtools.build.lib.sandbox.SandboxHelpers.SandboxOutputs;
import com.google.devtools.build.lib.server.FailureDetails.FailureDetail;
import com.google.devtools.build.lib.server.FailureDetails.Sandbox;
import com.google.devtools.build.lib.server.FailureDetails.Sandbox.Code;
import com.google.devtools.build.lib.skyframe.TreeArtifactValue;
import com.google.devtools.build.lib.util.StringEncoding;
import com.google.devtools.build.lib.util.io.FileOutErr;
import com.google.devtools.build.lib.vfs.DigestUtils;
import com.google.devtools.build.lib.vfs.FileStatus;
import com.google.devtools.build.lib.vfs.FileSystemUtils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.Symlinks;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.PreconditionFailure;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.protobuf.StatusProto;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Helper class managing the communication between Bazel and a local REAPI sandbox backend:
 *
 * <ul>
 *   <li>Server-managed daemon process lifecycle ({@code serve --socket=<path> --disk_cache=<dir>})
 *       and {@code Capabilities.GetCapabilities} handshake.
 *   <li>gRPC channel management ({@code unix:}, {@code inprocess:}, TCP) and {@code
 *       Execution.Execute} RPC execution with cancellation propagation and {@code
 *       FAILED_PRECONDITION (MISSING)} parsing.
 *   <li>Shared {@code --disk_cache} CAS output verification and materialization ({@link
 *       #verifyOutputBlobsInCas}, {@link #writeOutErr}, {@link #finalizeOutputsFromDiskCache}).
 * </ul>
 */
public final class SandboxBackendClient implements AutoCloseable {

  private static final String INPROCESS_PREFIX = "inprocess:";
  private static final int MAX_RECENT_OUTPUT_LINES = 50;

  @Nullable private static EventLoopGroup unixEventLoopGroup;

  private final String backendName;
  private final String spec;
  private final boolean directTarget;
  private final boolean available;
  @Nullable private final java.nio.file.Path resolvedBinary;
  private final ImmutableList<String> backendOpts;
  private final Path diskCacheRoot;
  private final DigestFunction.Value expectedDigestFunction;

  @Nullable private ManagedChannel channel;
  @Nullable private DiskCacheMerkleTreeUploader uploader;
  @Nullable private Object uploadScope;
  @Nullable private Process daemonProcess;
  @Nullable private java.nio.file.Path socketTempDir;
  @Nullable private java.nio.file.Path socketPath;
  private final Deque<String> recentOutputLines = new ArrayDeque<>();
  private final AtomicInteger daemonStartCount = new AtomicInteger();

  private SandboxBackendClient(
      String backendName,
      String spec,
      boolean directTarget,
      boolean available,
      @Nullable java.nio.file.Path resolvedBinary,
      ImmutableList<String> backendOpts,
      Path diskCacheRoot,
      DigestFunction.Value expectedDigestFunction,
      @Nullable ManagedChannel precreatedChannel) {
    this.backendName = checkNotNull(backendName);
    this.spec = checkNotNull(spec);
    this.directTarget = directTarget;
    this.available = available;
    this.resolvedBinary = resolvedBinary;
    this.backendOpts = checkNotNull(backendOpts);
    this.diskCacheRoot = checkNotNull(diskCacheRoot);
    this.expectedDigestFunction = checkNotNull(expectedDigestFunction);
    if (precreatedChannel != null) {
      this.channel = precreatedChannel;
      this.uploader = new DiskCacheMerkleTreeUploader(diskCacheRoot, expectedDigestFunction);
      this.uploadScope = new Object();
    }
  }

  public static SandboxBackendClient create(
      String backendName, String spec, List<String> backendOpts, CommandEnvironment cmdEnv) {
    DigestFunction.Value digestFunc =
        new DigestUtil(
                cmdEnv.getSyscallCache(), cmdEnv.getRuntime().getFileSystem().getDigestFunction())
            .getDigestFunction();
    Path diskCacheRoot = DiskCacheMerkleTreeUploader.resolveDiskCacheRoot(cmdEnv);
    if (isDirectTarget(spec)) {
      return new SandboxBackendClient(
          backendName,
          spec,
          /* directTarget= */ true,
          /* available= */ true,
          /* resolvedBinary= */ null,
          ImmutableList.copyOf(backendOpts),
          diskCacheRoot,
          digestFunc,
          /* precreatedChannel= */ null);
    }
    java.nio.file.Path resolved = resolveExecutable(spec, cmdEnv);
    return new SandboxBackendClient(
        backendName,
        spec,
        /* directTarget= */ false,
        /* available= */ resolved != null,
        resolved,
        ImmutableList.copyOf(backendOpts),
        diskCacheRoot,
        digestFunc,
        /* precreatedChannel= */ null);
  }

  @VisibleForTesting
  public static SandboxBackendClient createForTesting(
      String backendName,
      java.nio.file.Path resolvedBinary,
      List<String> backendOpts,
      Path diskCacheRoot,
      DigestFunction.Value digestFunction) {
    return new SandboxBackendClient(
        backendName,
        resolvedBinary.toString(),
        /* directTarget= */ false,
        /* available= */ true,
        resolvedBinary,
        ImmutableList.copyOf(backendOpts),
        diskCacheRoot,
        digestFunction,
        /* precreatedChannel= */ null);
  }

  public static SandboxBackendClient createDirect(
      String backendName, String target, Path diskCacheRoot, DigestFunction.Value digestFunction) {
    return new SandboxBackendClient(
        backendName,
        target,
        /* directTarget= */ true,
        /* available= */ true,
        /* resolvedBinary= */ null,
        ImmutableList.of(),
        diskCacheRoot,
        digestFunction,
        /* precreatedChannel= */ null);
  }

  public static SandboxBackendClient fromChannel(
      String backendName,
      ManagedChannel channel,
      Path diskCacheRoot,
      DigestFunction.Value digestFunction) {
    return new SandboxBackendClient(
        backendName,
        "channel",
        /* directTarget= */ true,
        /* available= */ true,
        /* resolvedBinary= */ null,
        ImmutableList.of(),
        diskCacheRoot,
        digestFunction,
        channel);
  }

  public static boolean isDirectTarget(String spec) {
    return spec.startsWith(INPROCESS_PREFIX)
        || spec.startsWith("unix:")
        || spec.startsWith("localhost:")
        || spec.startsWith("127.0.0.1:")
        || spec.startsWith("[::1]:");
  }

  private static synchronized EventLoopGroup getUnixEventLoopGroup() {
    if (unixEventLoopGroup == null) {
      if (KQueue.isAvailable()) {
        unixEventLoopGroup = new KQueueEventLoopGroup();
      } else if (Epoll.isAvailable()) {
        unixEventLoopGroup = new EpollEventLoopGroup();
      } else {
        unixEventLoopGroup = new NioEventLoopGroup();
      }
    }
    return unixEventLoopGroup;
  }

  static ManagedChannel createChannel(String target) {
    if (target.startsWith(INPROCESS_PREFIX)) {
      return InProcessChannelBuilder.forName(target.substring(INPROCESS_PREFIX.length()))
          .directExecutor()
          .build();
    }
    if (target.startsWith("unix:")) {
      String sockPath =
          target.startsWith("unix://")
              ? target.substring("unix://".length())
              : target.substring("unix:".length());
      if (KQueue.isAvailable()) {
        return NettyChannelBuilder.forAddress(new DomainSocketAddress(sockPath))
            .eventLoopGroup(getUnixEventLoopGroup())
            .overrideAuthority("localhost")
            .usePlaintext()
            .channelType(KQueueDomainSocketChannel.class)
            .build();
      }
      if (Epoll.isAvailable()) {
        return NettyChannelBuilder.forAddress(new DomainSocketAddress(sockPath))
            .eventLoopGroup(getUnixEventLoopGroup())
            .overrideAuthority("localhost")
            .usePlaintext()
            .channelType(EpollDomainSocketChannel.class)
            .build();
      }
      return NettyChannelBuilder.forAddress(UnixDomainSocketAddress.of(sockPath))
          .eventLoopGroup(getUnixEventLoopGroup())
          .overrideAuthority("localhost")
          .usePlaintext()
          .channelType(NioDomainSocketChannel.class)
          .build();
    }
    return NettyChannelBuilder.forTarget(target).usePlaintext().build();
  }

  @Nullable
  static java.nio.file.Path resolveExecutable(String spec, CommandEnvironment cmdEnv) {
    if (spec.isEmpty()) {
      return null;
    }
    if (spec.indexOf('/') >= 0 || spec.indexOf(File.separatorChar) >= 0) {
      java.nio.file.Path candidate = java.nio.file.Path.of(spec);
      if (!candidate.isAbsolute()) {
        candidate = cmdEnv.getExecRoot().getPathFile().toPath().resolve(candidate);
      }
      candidate = candidate.toAbsolutePath().normalize();
      return Files.isRegularFile(candidate) && Files.isExecutable(candidate) ? candidate : null;
    }
    String pathEnv = cmdEnv.getClientEnv().getOrDefault("PATH", "");
    for (String entry :
        Splitter.on(File.pathSeparatorChar).trimResults().omitEmptyStrings().split(pathEnv)) {
      java.nio.file.Path candidate =
          java.nio.file.Path.of(entry).resolve(spec).toAbsolutePath().normalize();
      if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  public boolean isAvailable() {
    return available;
  }

  public Path getDiskCacheRoot() {
    return diskCacheRoot;
  }

  public boolean matchesConfig(String newSpec, List<String> newOpts, CommandEnvironment cmdEnv) {
    if (!spec.equals(newSpec)
        || !backendOpts.equals(newOpts)
        || !diskCacheRoot.equals(DiskCacheMerkleTreeUploader.resolveDiskCacheRoot(cmdEnv))) {
      return false;
    }
    if (!directTarget) {
      java.nio.file.Path newlyResolved = resolveExecutable(newSpec, cmdEnv);
      if (!Objects.equals(resolvedBinary, newlyResolved)) {
        return false;
      }
    }
    return true;
  }

  public synchronized void verifyDiskCacheDirectory() {
    if (uploader != null) {
      uploader.verifyDiskCacheDirectory();
    }
  }

  @VisibleForTesting
  public int getDaemonStartCount() {
    return daemonStartCount.get();
  }

  @VisibleForTesting
  @Nullable
  public java.nio.file.Path getSocketPath() {
    return socketPath;
  }

  public synchronized void ensureConnected() throws IOException, InterruptedException {
    if (!available) {
      throw new IOException(
          "Sandbox backend '"
              + backendName
              + "' binary '"
              + spec
              + "' does not exist or is not executable");
    }
    if (directTarget) {
      if (channel == null) {
        channel = createChannel(spec);
        uploader = new DiskCacheMerkleTreeUploader(diskCacheRoot, expectedDigestFunction);
        uploadScope = new Object();
      } else if (uploader != null) {
        uploader.verifyDiskCacheDirectory();
      }
      return;
    }
    if (channel != null && daemonProcess != null && daemonProcess.isAlive()) {
      if (uploader != null) {
        uploader.verifyDiskCacheDirectory();
      }
      return;
    }
    shutdownInternal();
    startManagedDaemon();
  }

  private void startManagedDaemon() throws IOException, InterruptedException {
    checkNotNull(resolvedBinary);
    DiskCacheMerkleTreeUploader newUploader =
        new DiskCacheMerkleTreeUploader(diskCacheRoot, expectedDigestFunction);
    socketTempDir =
        Files.createTempDirectory(
            java.nio.file.Path.of("/tmp"),
            "bazel-reapi-" + backendName + "-",
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    socketPath = socketTempDir.resolve("s.sock");

    List<String> cmd = new ArrayList<>();
    cmd.add(resolvedBinary.toString());
    cmd.add("serve");
    cmd.add("--socket=" + socketPath);
    cmd.add("--disk_cache=" + diskCacheRoot.getPathString());
    cmd.add("--cas_root=" + newUploader.getCasRoot().getPathString());
    cmd.addAll(backendOpts);

    synchronized (recentOutputLines) {
      recentOutputLines.clear();
    }

    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.redirectErrorStream(true);
    Process proc = pb.start();
    // Keep proc.getOutputStream() open while the daemon is alive so the daemon can detect parent
    // process termination via stdin EOF if Bazel is SIGKILLed.

    Thread logReader =
        Thread.ofVirtual()
            .name("reapi-daemon-log-" + backendName)
            .start(
                () -> {
                  try (BufferedReader reader =
                      new BufferedReader(new InputStreamReader(proc.getInputStream(), UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                      synchronized (recentOutputLines) {
                        if (recentOutputLines.size() >= MAX_RECENT_OUTPUT_LINES) {
                          recentOutputLines.removeFirst();
                        }
                        recentOutputLines.addLast(line);
                      }
                    }
                  } catch (IOException ignored) {
                    // Stream closed when daemon exits.
                  }
                });

    long deadlineNanos = System.nanoTime() + SECONDS.toNanos(10);
    while (!Files.exists(socketPath, LinkOption.NOFOLLOW_LINKS)) {
      if (!proc.isAlive()) {
        joinQuietly(logReader, 200);
        cleanupSocketTempDir();
        throw new IOException(
            "Sandbox backend '"
                + backendName
                + "' exited prematurely with code "
                + proc.exitValue()
                + " before creating socket "
                + socketPath
                + formatOutputTail());
      }
      if (System.nanoTime() >= deadlineNanos) {
        proc.destroyForcibly();
        joinQuietly(logReader, 200);
        cleanupSocketTempDir();
        throw new IOException(
            "Timed out waiting for sandbox backend '"
                + backendName
                + "' to create socket "
                + socketPath
                + formatOutputTail());
      }
      try {
        MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        proc.destroyForcibly();
        cleanupSocketTempDir();
        throw e;
      }
    }

    ManagedChannel ch = createChannel("unix:" + socketPath);
    try {
      ServerCapabilities capabilities =
          CapabilitiesGrpc.newBlockingStub(ch)
              .withDeadlineAfter(10, SECONDS)
              .getCapabilities(GetCapabilitiesRequest.getDefaultInstance());
      if (!capabilities.hasExecutionCapabilities()
          || !capabilities.getExecutionCapabilities().getExecEnabled()) {
        throw new IOException(
            "Sandbox backend '"
                + backendName
                + "' did not report exec_enabled=true in ExecutionCapabilities");
      }
      ExecutionCapabilities execCaps = capabilities.getExecutionCapabilities();
      boolean digestSupported =
          !execCaps.getDigestFunctionsList().isEmpty()
              ? execCaps.getDigestFunctionsList().contains(expectedDigestFunction)
              : execCaps.getDigestFunction() == expectedDigestFunction;
      if (!digestSupported) {
        throw new IOException(
            "Sandbox backend '"
                + backendName
                + "' digest function mismatch: expected "
                + expectedDigestFunction
                + " but daemon reported "
                + (execCaps.getDigestFunctionsList().isEmpty()
                    ? execCaps.getDigestFunction()
                    : execCaps.getDigestFunctionsList()));
      }
    } catch (StatusRuntimeException | IOException e) {
      ch.shutdownNow();
      proc.destroyForcibly();
      joinQuietly(logReader, 200);
      cleanupSocketTempDir();
      if (e instanceof IOException ioEx) {
        throw ioEx;
      }
      throw new IOException(
          "Failed to verify Capabilities.GetCapabilities on sandbox backend '"
              + backendName
              + "'"
              + formatOutputTail(),
          e);
    }

    this.daemonProcess = proc;
    this.channel = ch;
    this.uploader = newUploader;
    this.uploadScope = new Object();
    this.daemonStartCount.incrementAndGet();
  }

  private static void joinQuietly(Thread thread, long timeoutMs) {
    try {
      thread.join(timeoutMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  String describeProcessState() {
    Process proc = daemonProcess;
    if (proc == null) {
      return "";
    }
    if (!proc.isAlive()) {
      return " (daemon process exited unexpectedly with code "
          + proc.exitValue()
          + formatOutputTail()
          + ")";
    }
    return "";
  }

  private String formatOutputTail() {
    synchronized (recentOutputLines) {
      if (recentOutputLines.isEmpty()) {
        return "";
      }
      return "; daemon output: " + Joiner.on(" | ").join(recentOutputLines);
    }
  }

  synchronized ManagedChannel getChannel() {
    return checkNotNull(channel, "Session not connected");
  }

  synchronized DiskCacheMerkleTreeUploader getUploader() {
    return checkNotNull(uploader, "Session not connected");
  }

  @VisibleForTesting
  public synchronized Object getUploadScope() {
    return checkNotNull(uploadScope, "Session not connected");
  }

  /**
   * Executes {@code Execution.Execute} on the local REAPI sandbox backend, propagating thread
   * interrupts as gRPC call cancellation and unpacking {@code FAILED_PRECONDITION} status details.
   */
  ExecuteResponse executeAction(Digest actionDigest, RequestMetadata requestMetadata)
      throws UserExecException, InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    ExecuteRequest request =
        ExecuteRequest.newBuilder().setActionDigest(actionDigest).setSkipCacheLookup(true).build();
    Channel interceptedChannel =
        ClientInterceptors.intercept(
            getChannel(), TracingMetadataUtils.attachMetadataInterceptor(requestMetadata));
    ClientCall<ExecuteRequest, Operation> call =
        interceptedChannel.newCall(ExecutionGrpc.getExecuteMethod(), CallOptions.DEFAULT);
    AtomicReference<Operation> lastOpRef = new AtomicReference<>();
    AtomicReference<io.grpc.Status> closeStatusRef = new AtomicReference<>();
    AtomicReference<Metadata> closeTrailersRef = new AtomicReference<>();
    CountDownLatch doneLatch = new CountDownLatch(1);

    call.start(
        new ClientCall.Listener<Operation>() {
          @Override
          public void onMessage(Operation message) {
            lastOpRef.set(message);
            call.request(1);
          }

          @Override
          public void onClose(io.grpc.Status status, Metadata trailers) {
            closeStatusRef.set(status);
            closeTrailersRef.set(trailers);
            doneLatch.countDown();
          }
        },
        new Metadata());
    call.sendMessage(request);
    call.halfClose();
    call.request(1);

    try {
      doneLatch.await();
    } catch (InterruptedException e) {
      call.cancel("Spawn interrupted", e);
      throw e;
    }

    if (Thread.interrupted()) {
      call.cancel("Spawn interrupted", null);
      throw new InterruptedException();
    }

    io.grpc.Status closeStatus = closeStatusRef.get();
    if (closeStatus != null && !closeStatus.isOk()) {
      if (closeStatus.getCode() == io.grpc.Status.Code.CANCELLED) {
        throw new InterruptedException();
      }
      StatusRuntimeException sre = closeStatus.asRuntimeException(closeTrailersRef.get());
      com.google.rpc.Status statusProto = StatusProto.fromThrowable(sre);
      if (statusProto != null
          && statusProto.getCode() == com.google.rpc.Code.FAILED_PRECONDITION.getNumber()) {
        return ExecuteResponse.newBuilder().setStatus(statusProto).build();
      }
      throw toUserExecException(
          "gRPC call to local REAPI sandbox Execute failed" + describeProcessState(), sre);
    }

    Operation lastOp = lastOpRef.get();
    if (lastOp == null || !lastOp.getDone()) {
      throw new UserExecException(
          FailureDetail.newBuilder()
              .setMessage(
                  "Local REAPI sandbox Execute stream ended without a completed Operation"
                      + describeProcessState())
              .setSandbox(Sandbox.newBuilder().setCode(Code.EXECUTION_IO_EXCEPTION))
              .build());
    }
    if (lastOp.hasError()) {
      com.google.rpc.Status opError = lastOp.getError();
      if (opError.getCode() == com.google.rpc.Code.FAILED_PRECONDITION.getNumber()) {
        return ExecuteResponse.newBuilder().setStatus(opError).build();
      }
      throw new UserExecException(
          FailureDetail.newBuilder()
              .setMessage("Local REAPI sandbox Operation error: " + opError.getMessage())
              .setSandbox(Sandbox.newBuilder().setCode(Code.EXECUTION_IO_EXCEPTION))
              .build());
    }
    try {
      return lastOp.getResponse().unpack(ExecuteResponse.class);
    } catch (IOException e) {
      throw toUserExecException("Failed to unpack ExecuteResponse from local REAPI sandbox", e);
    }
  }

  static boolean isMissingBlobsFailure(
      com.google.rpc.Status rpcStatus, Set<String> outMissingHashes) {
    if (rpcStatus.getCode() != com.google.rpc.Code.FAILED_PRECONDITION.getNumber()) {
      return false;
    }
    boolean foundMissing = false;
    for (Any detail : rpcStatus.getDetailsList()) {
      if (detail.is(PreconditionFailure.class)) {
        try {
          PreconditionFailure pf = detail.unpack(PreconditionFailure.class);
          for (PreconditionFailure.Violation violation : pf.getViolationsList()) {
            if ("MISSING".equals(violation.getType())) {
              foundMissing = true;
              String hash = parseHashFromSubject(violation.getSubject());
              if (hash != null) {
                outMissingHashes.add(hash);
              }
            }
          }
        } catch (InvalidProtocolBufferException ignored) {
          // Ignore malformed error detail.
        }
      }
    }
    return foundMissing;
  }

  @Nullable
  private static String parseHashFromSubject(String subject) {
    if (subject == null || subject.isEmpty()) {
      return null;
    }
    String trimmed = subject.startsWith("blobs/") ? subject.substring("blobs/".length()) : subject;
    int slash = trimmed.indexOf('/');
    String hash = slash > 0 ? trimmed.substring(0, slash) : trimmed;
    return hash.isEmpty() ? null : hash;
  }

  /**
   * Verifies that all non-empty output blobs and directory trees referenced by {@code actionResult}
   * exist in the shared {@code --disk_cache} CAS directory.
   */
  void verifyOutputBlobsInCas(ActionResult actionResult) throws IOException {
    DiskCacheMerkleTreeUploader currentUploader = getUploader();
    for (OutputFile outputFile : actionResult.getOutputFilesList()) {
      Digest digest = outputFile.getDigest();
      if (digest.getSizeBytes() > 0 && outputFile.getContents().isEmpty()) {
        Path casPath = currentUploader.toCasPath(digest);
        if (!casPath.exists(Symlinks.NOFOLLOW)) {
          throw new IOException(
              "Output blob "
                  + digest.getHash()
                  + " missing from shared local CAS ("
                  + casPath
                  + ") for "
                  + outputFile.getPath());
        }
      }
    }
    for (OutputDirectory outputDir : actionResult.getOutputDirectoriesList()) {
      Digest treeDigest = outputDir.getTreeDigest();
      if (treeDigest.getSizeBytes() > 0) {
        Path casPath = currentUploader.toCasPath(treeDigest);
        if (!casPath.exists(Symlinks.NOFOLLOW)) {
          throw new IOException(
              "Output tree blob "
                  + treeDigest.getHash()
                  + " missing from shared local CAS ("
                  + casPath
                  + ") for "
                  + outputDir.getPath());
        }
      }
    }
  }

  /**
   * Writes stdout/stderr from {@code actionResult} (inline or via shared CAS) into {@code outErr}.
   */
  void writeOutErr(ActionResult actionResult, FileOutErr tmpOutErr) throws IOException {
    DiskCacheMerkleTreeUploader currentUploader = getUploader();
    if (!actionResult.getStdoutRaw().isEmpty()) {
      actionResult.getStdoutRaw().writeTo(tmpOutErr.getOutputStream());
      tmpOutErr.getOutputStream().flush();
    } else if (actionResult.hasStdoutDigest()
        && actionResult.getStdoutDigest().getSizeBytes() > 0) {
      currentUploader.streamBlobFromCas(
          actionResult.getStdoutDigest(), tmpOutErr.getOutputStream());
      tmpOutErr.getOutputStream().flush();
    }

    if (!actionResult.getStderrRaw().isEmpty()) {
      actionResult.getStderrRaw().writeTo(tmpOutErr.getErrorStream());
      tmpOutErr.getErrorStream().flush();
    } else if (actionResult.hasStderrDigest()
        && actionResult.getStderrDigest().getSizeBytes() > 0) {
      currentUploader.streamBlobFromCas(actionResult.getStderrDigest(), tmpOutErr.getErrorStream());
      tmpOutErr.getErrorStream().flush();
    }
  }

  private record MaterializedFile(FileStatus status, byte[] binaryDigest) {}

  /**
   * Links or clones declared outputs from the shared {@code --disk_cache} CAS directory into {@code
   * execRoot}, applying Bazel's output path mapping.
   */
  void finalizeOutputsFromDiskCache(
      Path execRoot, DigestUtil digestUtil, SandboxOutputs outputs, ActionResult actionResult)
      throws IOException {
    var unused =
        finalizeOutputsFromDiskCache(
            execRoot,
            digestUtil,
            outputs,
            actionResult,
            /* spawn= */ null,
            /* outputMetadataStore= */ null);
  }

  /**
   * Links or clones declared outputs from the shared {@code --disk_cache} CAS directory into {@code
   * execRoot}, applying Bazel's output path mapping and injecting precomputed digests into {@link
   * DigestUtils} and {@link OutputMetadataStore} when available.
   *
   * @return the set of unmapped output paths materialized into {@code execRoot}
   */
  @SuppressWarnings(
      "deprecation") // Support REAPI v2.0 output_file_symlinks/output_directory_symlinks
  Set<PathFragment> finalizeOutputsFromDiskCache(
      Path execRoot,
      DigestUtil digestUtil,
      SandboxOutputs outputs,
      ActionResult actionResult,
      @Nullable Spawn spawn,
      @Nullable OutputMetadataStore outputMetadataStore)
      throws IOException {
    DiskCacheMerkleTreeUploader currentUploader = getUploader();
    Map<PathFragment, PathFragment> mappedToUnmappedFiles = new HashMap<>();
    for (Map.Entry<PathFragment, PathFragment> entry : outputs.files().entrySet()) {
      mappedToUnmappedFiles.put(entry.getValue(), entry.getKey());
    }
    Map<PathFragment, PathFragment> mappedToUnmappedDirs = new HashMap<>();
    for (Map.Entry<PathFragment, PathFragment> entry : outputs.dirs().entrySet()) {
      mappedToUnmappedDirs.put(entry.getValue(), entry.getKey());
    }

    boolean canInjectMetadata =
        outputMetadataStore != null
            && spawn != null
            && !"TestRunner".equals(spawn.getMnemonic());
    Map<PathFragment, Artifact> artifactsByUnmappedPath = new HashMap<>();
    if (canInjectMetadata) {
      for (ActionInput output : spawn.getOutputFiles()) {
        if (output instanceof Artifact artifact) {
          artifactsByUnmappedPath.put(artifact.getExecPath(), artifact);
        }
      }
    }

    Set<PathFragment> materializedUnmappedPaths = new HashSet<>();

    for (OutputFile outputFile : actionResult.getOutputFilesList()) {
      PathFragment mappedPath =
          validateDeclaredOutputPath(
              StringEncoding.unicodeToInternal(outputFile.getPath()),
              mappedToUnmappedFiles.keySet(),
              mappedToUnmappedDirs.keySet());
      PathFragment unmappedPath =
          resolveUnmappedOutputPath(mappedPath, mappedToUnmappedFiles, mappedToUnmappedDirs);
      Path dest = execRoot.getRelative(unmappedPath);
      MaterializedFile materialized =
          materializeCasFileToExecRoot(
              outputFile.getDigest(),
              outputFile.getContents(),
              outputFile.getIsExecutable(),
              dest,
              currentUploader);
      materializedUnmappedPaths.add(unmappedPath);
      if (canInjectMetadata) {
        Artifact artifact = artifactsByUnmappedPath.get(unmappedPath);
        if (artifact != null
            && !artifact.isTreeArtifact()
            && !artifact.isChildOfDeclaredDirectory()
            && !artifact.isSymlink()
            && !artifact.isDirectory()) {
          outputMetadataStore.injectFile(
              artifact,
              FileArtifactValue.createForNormalFile(
                  materialized.binaryDigest(),
                  FileContentsProxy.create(materialized.status()),
                  outputFile.getDigest().getSizeBytes()));
        }
      }
    }

    List<OutputSymlink> allOutputSymlinks = new ArrayList<>();
    allOutputSymlinks.addAll(actionResult.getOutputSymlinksList());
    allOutputSymlinks.addAll(actionResult.getOutputFileSymlinksList());
    allOutputSymlinks.addAll(actionResult.getOutputDirectorySymlinksList());

    for (OutputSymlink outputSymlink : allOutputSymlinks) {
      PathFragment mappedPath =
          validateDeclaredOutputPath(
              StringEncoding.unicodeToInternal(outputSymlink.getPath()),
              mappedToUnmappedFiles.keySet(),
              mappedToUnmappedDirs.keySet());
      PathFragment unmappedPath =
          resolveUnmappedOutputPath(mappedPath, mappedToUnmappedFiles, mappedToUnmappedDirs);
      Path dest = execRoot.getRelative(unmappedPath);
      Path parent = dest.getParentDirectory();
      if (parent != null) {
        parent.createDirectoryAndParents();
      }
      if (dest.exists(Symlinks.NOFOLLOW)) {
        dest.deleteTree();
      }
      dest.createSymbolicLink(
          PathFragment.create(StringEncoding.unicodeToInternal(outputSymlink.getTarget())));
      materializedUnmappedPaths.add(unmappedPath);
    }

    for (OutputDirectory outputDir : actionResult.getOutputDirectoriesList()) {
      PathFragment mappedPath =
          validateDeclaredOutputPath(
              StringEncoding.unicodeToInternal(outputDir.getPath()),
              mappedToUnmappedFiles.keySet(),
              mappedToUnmappedDirs.keySet());
      PathFragment unmappedPath =
          resolveUnmappedOutputPath(mappedPath, mappedToUnmappedFiles, mappedToUnmappedDirs);
      Path destDir = execRoot.getRelative(unmappedPath);
      if (destDir.exists(Symlinks.NOFOLLOW)) {
        destDir.deleteTree();
      }
      destDir.createDirectoryAndParents();
      materializedUnmappedPaths.add(unmappedPath);

      SpecialArtifact treeArtifact = null;
      TreeArtifactValue.Builder treeBuilder = null;
      if (canInjectMetadata
          && artifactsByUnmappedPath.get(unmappedPath) instanceof SpecialArtifact specialArtifact
          && specialArtifact.isTreeArtifact()) {
        treeArtifact = specialArtifact;
        treeBuilder = TreeArtifactValue.newBuilder(specialArtifact);
      }

      boolean hasSymlinks = false;
      Digest treeDigest = outputDir.getTreeDigest();
      if (treeDigest.getSizeBytes() > 0) {
        currentUploader.markPopulated(treeDigest);
        byte[] treeBytes = currentUploader.readBlobFromCas(treeDigest);
        Tree tree = Tree.parseFrom(treeBytes, ExtensionRegistryLite.getEmptyRegistry());
        Map<Digest, Directory> childrenByDigest = new HashMap<>();
        for (Directory childDir : tree.getChildrenList()) {
          Digest childDigest = digestUtil.compute(childDir.toByteArray());
          childrenByDigest.put(childDigest, childDir);
        }
        Set<Digest> visitedDirs = new HashSet<>();
        Digest rootDirDigest = digestUtil.compute(tree.getRoot().toByteArray());
        visitedDirs.add(rootDirDigest);
        hasSymlinks =
            materializeDirectoryFromCas(
                tree.getRoot(),
                destDir,
                destDir,
                childrenByDigest,
                visitedDirs,
                currentUploader,
                treeArtifact,
                treeBuilder);
      }
      if (treeArtifact != null && treeBuilder != null && !hasSymlinks) {
        outputMetadataStore.injectTree(treeArtifact, treeBuilder.build());
      }
    }

    return materializedUnmappedPaths;
  }

  private static boolean materializeDirectoryFromCas(
      Directory directory,
      Path treeRootDir,
      Path targetDir,
      Map<Digest, Directory> childrenByDigest,
      Set<Digest> visitedDirs,
      DiskCacheMerkleTreeUploader uploader,
      @Nullable SpecialArtifact treeArtifact,
      @Nullable TreeArtifactValue.Builder treeBuilder)
      throws IOException {
    targetDir.createDirectoryAndParents();
    boolean hasSymlinks = !directory.getSymlinksList().isEmpty();
    for (FileNode fileNode : directory.getFilesList()) {
      String childName =
          validateSinglePathComponent(StringEncoding.unicodeToInternal(fileNode.getName()));
      Path childDest = targetDir.getChild(childName);
      MaterializedFile materialized =
          materializeCasFileToExecRoot(
              fileNode.getDigest(),
              ByteString.EMPTY,
              fileNode.getIsExecutable(),
              childDest,
              uploader);
      if (treeArtifact != null && treeBuilder != null) {
        TreeFileArtifact treeFile =
            TreeFileArtifact.createTreeOutput(treeArtifact, childDest.relativeTo(treeRootDir));
        treeBuilder.putChild(
            treeFile,
            FileArtifactValue.createForNormalFile(
                materialized.binaryDigest(),
                FileContentsProxy.create(materialized.status()),
                fileNode.getDigest().getSizeBytes()));
      }
    }
    for (SymlinkNode symlinkNode : directory.getSymlinksList()) {
      String childName =
          validateSinglePathComponent(StringEncoding.unicodeToInternal(symlinkNode.getName()));
      Path childDest = targetDir.getChild(childName);
      if (childDest.exists(Symlinks.NOFOLLOW)) {
        childDest.deleteTree();
      }
      childDest.createSymbolicLink(
          PathFragment.create(StringEncoding.unicodeToInternal(symlinkNode.getTarget())));
    }
    for (DirectoryNode dirNode : directory.getDirectoriesList()) {
      String childName =
          validateSinglePathComponent(StringEncoding.unicodeToInternal(dirNode.getName()));
      Path childDirPath = targetDir.getChild(childName);
      Digest dirDigest = dirNode.getDigest();
      if (!visitedDirs.add(dirDigest)) {
        throw new IOException(
            "Cyclic Directory reference detected in sandbox output Tree: " + dirDigest.getHash());
      }
      Directory childDir = childrenByDigest.get(dirDigest);
      if (childDir == null && dirDigest.getSizeBytes() > 0) {
        byte[] dirBytes = uploader.readBlobFromCas(dirDigest);
        childDir = Directory.parseFrom(dirBytes, ExtensionRegistryLite.getEmptyRegistry());
      }
      if (childDir != null) {
        if (materializeDirectoryFromCas(
            childDir,
            treeRootDir,
            childDirPath,
            childrenByDigest,
            visitedDirs,
            uploader,
            treeArtifact,
            treeBuilder)) {
          hasSymlinks = true;
        }
      } else {
        childDirPath.createDirectoryAndParents();
      }
      visitedDirs.remove(dirDigest);
    }
    return hasSymlinks;
  }

  private static String validateSinglePathComponent(String name) throws IOException {
    if (name.isEmpty()
        || name.equals(".")
        || name.equals("..")
        || name.indexOf('/') >= 0
        || name.indexOf('\\') >= 0
        || name.indexOf('\0') >= 0) {
      throw new IOException("Invalid directory entry name returned by sandbox daemon: " + name);
    }
    return name;
  }

  private static MaterializedFile materializeCasFileToExecRoot(
      Digest digest,
      ByteString inlineContents,
      boolean isExecutable,
      Path dest,
      DiskCacheMerkleTreeUploader uploader)
      throws IOException {
    Path parent = dest.getParentDirectory();
    if (parent != null) {
      parent.createDirectoryAndParents();
    }
    if (dest.exists(Symlinks.NOFOLLOW)) {
      dest.deleteTree();
    }
    if (digest.getSizeBytes() == 0) {
      FileSystemUtils.writeContent(dest, new byte[0]);
      if (isExecutable) {
        dest.setExecutable(true);
      }
      FileStatus destStat = dest.stat(Symlinks.NOFOLLOW);
      byte[] binaryDigest = DigestUtil.toBinaryDigest(digest);
      DigestUtils.injectDigest(dest, destStat, binaryDigest);
      return new MaterializedFile(destStat, binaryDigest);
    }
    if (!inlineContents.isEmpty()) {
      FileSystemUtils.writeContent(dest, inlineContents.toByteArray());
      if (isExecutable) {
        dest.setExecutable(true);
      }
      FileStatus destStat = dest.stat(Symlinks.NOFOLLOW);
      byte[] binaryDigest = DigestUtil.toBinaryDigest(digest);
      DigestUtils.injectDigest(dest, destStat, binaryDigest);
      return new MaterializedFile(destStat, binaryDigest);
    }

    uploader.markPopulated(digest);
    Path casPath = uploader.toCasPath(digest);

    // Materialize declared outputs from the shared local CAS into execRoot via copy (matching
    // DiskCacheClient.download so execRoot and --disk_cache do not share mutable inodes).
    java.nio.file.Path casNio = casPath.getFileSystem().getNioPath(casPath.asFragment());
    java.nio.file.Path destNio = dest.getFileSystem().getNioPath(dest.asFragment());
    if (casNio != null && destNio != null) {
      try {
        Files.copy(casNio, destNio, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      } catch (java.nio.file.NoSuchFileException e) {
        throw new IOException(
            "Output blob "
                + digest.getHash()
                + " missing from shared local CAS ("
                + casPath
                + ") for "
                + dest,
            e);
      }
      if (isExecutable) {
        dest.chmod(0755);
      }
    } else {
      if (!casPath.exists(Symlinks.NOFOLLOW)) {
        throw new IOException(
            "Output blob "
                + digest.getHash()
                + " missing from shared local CAS ("
                + casPath
                + ") for "
                + dest);
      }
      FileSystemUtils.copyFile(casPath, dest);
      if (isExecutable) {
        dest.setExecutable(true);
      }
    }
    FileStatus destStat = dest.stat(Symlinks.NOFOLLOW);
    byte[] binaryDigest = DigestUtil.toBinaryDigest(digest);
    DigestUtils.injectDigest(dest, destStat, binaryDigest);
    return new MaterializedFile(destStat, binaryDigest);
  }

  private static PathFragment validateDeclaredOutputPath(
      String rawPath, Set<PathFragment> declaredFiles, Set<PathFragment> declaredDirs)
      throws IOException {
    PathFragment pf = PathFragment.create(rawPath);
    if (pf.isEmpty() || pf.isAbsolute() || pf.containsUplevelReferences()) {
      throw new IOException("Invalid output path returned by sandbox daemon: " + rawPath);
    }
    if (declaredFiles.contains(pf) || declaredDirs.contains(pf)) {
      return pf;
    }
    for (PathFragment dir : declaredDirs) {
      if (pf.startsWith(dir)) {
        return pf;
      }
    }
    throw new IOException("Undeclared output path returned by sandbox daemon: " + rawPath);
  }

  private static PathFragment resolveUnmappedOutputPath(
      PathFragment mappedPath,
      Map<PathFragment, PathFragment> mappedToUnmappedFiles,
      Map<PathFragment, PathFragment> mappedToUnmappedDirs) {
    PathFragment directFile = mappedToUnmappedFiles.get(mappedPath);
    if (directFile != null) {
      return directFile;
    }
    PathFragment directDir = mappedToUnmappedDirs.get(mappedPath);
    if (directDir != null) {
      return directDir;
    }
    for (Map.Entry<PathFragment, PathFragment> dirEntry : mappedToUnmappedDirs.entrySet()) {
      if (mappedPath.startsWith(dirEntry.getKey())) {
        return dirEntry.getValue().getRelative(mappedPath.relativeTo(dirEntry.getKey()));
      }
    }
    return mappedPath;
  }

  static UserExecException toUserExecException(String prefix, Exception cause) {
    return new UserExecException(
        cause,
        FailureDetail.newBuilder()
            .setMessage(prefix + ": " + cause.getMessage())
            .setSandbox(Sandbox.newBuilder().setCode(Code.EXECUTION_IO_EXCEPTION))
            .build());
  }

  private void shutdownInternal() {
    if (channel != null) {
      channel.shutdown();
      try {
        if (!channel.awaitTermination(2, SECONDS)) {
          channel.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        channel.shutdownNow();
      }
      channel = null;
    }
    if (daemonProcess != null) {
      try {
        daemonProcess.getOutputStream().close();
      } catch (IOException ignored) {
        // Best-effort stdin close before terminating daemon.
      }
      daemonProcess.destroy();
      try {
        if (!daemonProcess.waitFor(2, SECONDS)) {
          daemonProcess.destroyForcibly();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        daemonProcess.destroyForcibly();
      }
      daemonProcess = null;
    }
    cleanupSocketTempDir();
    uploader = null;
    uploadScope = null;
  }

  private void cleanupSocketTempDir() {
    if (socketPath != null) {
      try {
        Files.deleteIfExists(socketPath);
      } catch (IOException ignored) {
        // Best-effort cleanup.
      }
      socketPath = null;
    }
    if (socketTempDir != null) {
      try {
        if (Files.exists(socketTempDir)) {
          try (var stream = Files.list(socketTempDir)) {
            for (java.nio.file.Path child : stream.toList()) {
              Files.deleteIfExists(child);
            }
          }
          Files.deleteIfExists(socketTempDir);
        }
      } catch (IOException ignored) {
        // Best-effort cleanup.
      }
      socketTempDir = null;
    }
  }

  @Override
  public synchronized void close() {
    shutdownInternal();
  }
}
