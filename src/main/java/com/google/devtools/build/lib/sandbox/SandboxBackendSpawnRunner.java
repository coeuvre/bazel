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

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.build.lib.actions.ActionExecutionMetadata;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ExecException;
import com.google.devtools.build.lib.actions.LostInputsExecException;
import com.google.devtools.build.lib.actions.ResourceSet;
import com.google.devtools.build.lib.actions.Spawn;
import com.google.devtools.build.lib.actions.SpawnMetrics;
import com.google.devtools.build.lib.actions.SpawnResult;
import com.google.devtools.build.lib.actions.SpawnResult.Status;
import com.google.devtools.build.lib.actions.Spawns;
import com.google.devtools.build.lib.actions.UserExecException;
import com.google.devtools.build.lib.exec.SpawnRunner;
import com.google.devtools.build.lib.exec.TreeDeleter;
import com.google.devtools.build.lib.exec.local.LocalEnvProvider;
import com.google.devtools.build.lib.profiler.Profiler;
import com.google.devtools.build.lib.profiler.SilentCloseable;
import com.google.devtools.build.lib.remote.common.RemoteActionExecutionContext;
import com.google.devtools.build.lib.remote.merkletree.MerkleTree;
import com.google.devtools.build.lib.remote.merkletree.MerkleTreeComputer;
import com.google.devtools.build.lib.remote.merkletree.MerkleTreeComputer.BlobPolicy;
import com.google.devtools.build.lib.remote.options.RemoteOptions;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import com.google.devtools.build.lib.runtime.CommandEnvironment;
import com.google.devtools.build.lib.sandbox.SandboxHelpers.SandboxOutputs;
import com.google.devtools.build.lib.server.FailureDetails;
import com.google.devtools.build.lib.server.FailureDetails.FailureDetail;
import com.google.devtools.build.lib.server.FailureDetails.Sandbox;
import com.google.devtools.build.lib.server.FailureDetails.Sandbox.Code;
import com.google.devtools.build.lib.util.StringEncoding;
import com.google.devtools.build.lib.util.io.FileOutErr;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.Symlinks;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;

/**
 * A {@link SpawnRunner} that orchestrates sandboxed action execution via a local daemon
 * implementing the standard Remote Execution API (REAPI v2), exchanging blobs and files through a
 * shared local CAS on disk ({@code --disk_cache} via {@link DiskCacheMerkleTreeUploader}) while
 * using gRPC purely as the control plane ({@code Execution.Execute} via {@link
 * SandboxBackendClient}).
 *
 * <p>Lifecycle per spawn (extending {@link AbstractSandboxSpawnRunner} to reuse local execution
 * setup, resource acquisition, output locking, and metrics while overriding {@link #prepareSpawn},
 * {@link #run}, and {@link #verifyPostCondition}):
 *
 * <ol>
 *   <li><b>1. Prepare ({@link #prepareSpawn} + {@link SandboxBackendSpawn#createFileSystem})</b>:
 *       Builds the standard REAPI {@link Action}, {@link Command}, and {@link Directory} Merkle
 *       tree via {@link MerkleTreeComputer} ({@link BlobPolicy#KEEP}) and ensures all blobs ({@link
 *       Directory}, {@code VirtualActionInput}, {@link Command}, {@link Action}) and missing input
 *       files exist under {@code <disk_cache>/cas/<hash[0..2]>/<hash>}.
 *   <li><b>2. Execute ({@link #run})</b>: Calls {@link SandboxBackendClient#executeAction} on the
 *       sandbox backend. If {@code --disk_cache} GC evicts a blob while a subtree is cached in
 *       memory, {@code FAILED_PRECONDITION (MISSING)} triggers automatic retry with {@link
 *       BlobPolicy#KEEP_AND_REUPLOAD}.
 *   <li><b>3. Finalize ({@link #verifyPostCondition} + {@link
 *       SandboxBackendSpawn#copyOutputs})</b>: Verifies output blobs exist in {@code
 *       <disk_cache>/cas/} and, after winning {@link SpawnExecutionContext#lockOutputFiles(int,
 *       String, FileOutErr)}, links/clones declared outputs from {@code <disk_cache>/cas/} into
 *       {@code execRoot} and records {@link SpawnMetrics}.
 * </ol>
 */
public final class SandboxBackendSpawnRunner extends AbstractSandboxSpawnRunner
    implements AutoCloseable {

  /** Prefix used when registering named REAPI sandbox backends as spawn strategies. */
  public static final String STRATEGY_PREFIX = "sandbox_backend:";

  /** Platform property key for the workspace execRoot path on the local host. */
  public static final String PLATFORM_EXEC_ROOT = "exec_root";

  /** Platform property key for the shared local disk cache root directory (`--disk_cache`). */
  public static final String PLATFORM_DISK_CACHE = "disk_cache";

  /** Platform property key for the resolved CAS directory (`<disk_cache>/[<digest_fn>/]cas`). */
  public static final String PLATFORM_CAS_ROOT = "bazel.sandbox.cas_root";

  /** Platform property key for the sandbox base directory (`<outputBase>/sandbox`). */
  public static final String PLATFORM_SANDBOX_BASE = "bazel.sandbox.sandbox_base";

  /** Platform property key for the per-spawn sandbox ID. */
  public static final String PLATFORM_SANDBOX_ID = "sandbox_id";

  /** Platform property key for whether network access is allowed in the sandbox. */
  public static final String PLATFORM_ALLOW_NETWORK = "allow_network";

  /** Platform property key for whether sandbox directories should be preserved for debugging. */
  public static final String PLATFORM_SANDBOX_DEBUG = "sandbox_debug";

  /** Platform property key for additional writable paths inside the sandbox. */
  public static final String PLATFORM_WRITABLE_PATH = "writable_path";

  /** Platform property key for tmpfs mount paths inside the sandbox. */
  public static final String PLATFORM_TMPFS_PATH = "tmpfs_path";

  /** Platform property key for inaccessible (blocked) paths inside the sandbox. */
  public static final String PLATFORM_INACCESSIBLE_PATH = "inaccessible_path";

  /** Platform property key for requested CPU usage. */
  public static final String PLATFORM_RESOURCES_CPU = "resources:cpu";

  /** Platform property key for requested RAM in MB. */
  public static final String PLATFORM_RESOURCES_MEMORY_MB = "resources:memory_mb";

  private static final int MAX_MISSING_RETRIES = 2;

  private record PrecomputedTreeOverride(MerkleTree tree, boolean skipUploader) {}

  private final Path execRoot;
  private final Path sandboxBase;
  private final String backendName;
  private final String strategyName;
  private final LocalEnvProvider localEnvProvider;
  private final DigestUtil digestUtil;
  private final String buildRequestId;
  private final String commandId;
  private final String workspaceName;
  private final SandboxBackendClient client;
  private final boolean ownsClient;
  private final boolean sharesConfiguredDiskCache;
  private final ThreadLocal<PrecomputedTreeOverride> precomputedTreeOverride = new ThreadLocal<>();

  @Nullable private volatile Object cachedComputerScope;
  @Nullable private volatile MerkleTreeComputer cachedMerkleTreeComputer;

  public SandboxBackendSpawnRunner(CommandEnvironment cmdEnv, String target) {
    this(cmdEnv, target, DiskCacheMerkleTreeUploader.resolveDiskCacheRoot(cmdEnv));
  }

  public SandboxBackendSpawnRunner(CommandEnvironment cmdEnv, String target, Path diskCacheRoot) {
    this(
        cmdEnv,
        cmdEnv.getOutputBase().getRelative("sandbox"),
        "reapi-sandbox",
        SandboxBackendClient.createDirect(
            "reapi-sandbox",
            target,
            diskCacheRoot,
            new DigestUtil(
                    cmdEnv.getSyscallCache(),
                    cmdEnv.getRuntime().getFileSystem().getDigestFunction())
                .getDigestFunction()),
        /* ownsClient= */ true);
  }

  @VisibleForTesting
  public SandboxBackendSpawnRunner(CommandEnvironment cmdEnv, ManagedChannel channel) {
    this(
        cmdEnv,
        cmdEnv.getOutputBase().getRelative("sandbox"),
        "reapi-sandbox",
        SandboxBackendClient.fromChannel(
            "reapi-sandbox",
            channel,
            DiskCacheMerkleTreeUploader.resolveDiskCacheRoot(cmdEnv),
            new DigestUtil(
                    cmdEnv.getSyscallCache(),
                    cmdEnv.getRuntime().getFileSystem().getDigestFunction())
                .getDigestFunction()),
        /* ownsClient= */ true);
  }

  public SandboxBackendSpawnRunner(
      CommandEnvironment cmdEnv,
      Path sandboxBase,
      String backendName,
      SandboxBackendClient client,
      boolean ownsClient) {
    super(checkNotNull(cmdEnv));
    this.execRoot = cmdEnv.getExecRoot();
    this.sandboxBase = checkNotNull(sandboxBase);
    this.backendName = checkNotNull(backendName);
    this.strategyName =
        backendName.equals("reapi-sandbox") ? "reapi-sandbox" : STRATEGY_PREFIX + backendName;
    this.localEnvProvider = LocalEnvProvider.forCurrentOs(cmdEnv.getClientEnv());
    this.digestUtil =
        new DigestUtil(
            cmdEnv.getSyscallCache(), cmdEnv.getRuntime().getFileSystem().getDigestFunction());
    this.buildRequestId = cmdEnv.getBuildRequestId();
    this.commandId = cmdEnv.getCommandId().toString();
    this.workspaceName = cmdEnv.getWorkspaceName();
    this.client = checkNotNull(client);
    this.ownsClient = ownsClient;
    RemoteOptions remoteOptions = cmdEnv.getOptions().getOptions(RemoteOptions.class);
    this.sharesConfiguredDiskCache =
        remoteOptions != null
            && remoteOptions.isDiskCacheEnabled()
            && client
                .getDiskCacheRoot()
                .equals(DiskCacheMerkleTreeUploader.resolveDiskCacheRoot(cmdEnv));
  }

  @Override
  public String getName() {
    return strategyName;
  }

  @Override
  public boolean canExec(Spawn spawn) {
    return client.isAvailable() && super.canExec(spawn);
  }

  @VisibleForTesting
  public SandboxBackendClient getClient() {
    return client;
  }

  @VisibleForTesting
  public DiskCacheMerkleTreeUploader getUploader() {
    try {
      client.ensureConnected();
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException("Failed to connect client for uploader access", e);
    }
    return client.getUploader();
  }

  private MerkleTreeComputer getMerkleTreeComputer() {
    Object currentScope = client.getUploadScope();
    MerkleTreeComputer computer = cachedMerkleTreeComputer;
    if (computer == null || cachedComputerScope != currentScope) {
      synchronized (this) {
        computer = cachedMerkleTreeComputer;
        if (computer == null || cachedComputerScope != currentScope) {
          computer =
              new MerkleTreeComputer(
                  digestUtil,
                  client.getUploader(),
                  currentScope,
                  buildRequestId,
                  commandId,
                  workspaceName);
          cachedComputerScope = currentScope;
          cachedMerkleTreeComputer = computer;
        }
      }
    }
    return computer;
  }

  @VisibleForTesting
  SpawnResult execWithPrecomputedMerkleTree(
      Spawn spawn, SpawnExecutionContext context, MerkleTree precomputedTree, boolean skipUploader)
      throws ExecException, InterruptedException {
    precomputedTreeOverride.set(new PrecomputedTreeOverride(precomputedTree, skipUploader));
    try {
      return exec(spawn, context);
    } finally {
      precomputedTreeOverride.remove();
    }
  }

  @Override
  protected SandboxedSpawn prepareSpawn(Spawn spawn, SpawnExecutionContext context)
      throws IOException, ExecException, InterruptedException {
    try {
      client.ensureConnected();
    } catch (IOException e) {
      throw SandboxBackendClient.toUserExecException(
          "Failed to initialize local REAPI sandbox backend '" + backendName + "'", e);
    }

    ImmutableMap<String, String> environment =
        localEnvProvider.rewriteLocalEnv(spawn.getEnvironment(), binTools, "/tmp");
    SandboxOutputs outputs = SandboxHelpers.getOutputs(spawn);
    Command command = buildCommand(spawn, environment, outputs);
    byte[] commandBytes = command.toByteArray();
    Digest commandDigest = digestUtil.compute(commandBytes);
    Platform platform = buildPlatform(spawn, context, spawn.getLocalResources());

    PrecomputedTreeOverride override = precomputedTreeOverride.get();
    MerkleTree merkleTree =
        override != null
            ? override.tree()
            : buildMerkleTreeForSpawn(spawn, context, BlobPolicy.KEEP);
    boolean skipUploader = override != null && override.skipUploader();

    return new SandboxBackendSpawn(
        spawn,
        context,
        environment,
        outputs,
        commandBytes,
        commandDigest,
        platform,
        merkleTree,
        skipUploader);
  }

  private MerkleTree buildMerkleTreeForSpawn(
      Spawn spawn, SpawnExecutionContext context, BlobPolicy blobPolicy)
      throws ExecException, InterruptedException {
    MerkleTreeComputer merkleTreeComputer = getMerkleTreeComputer();
    try (SilentCloseable c = Profiler.instance().profile("SandboxBackend.buildMerkleTree")) {
      return merkleTreeComputer.buildForSpawn(
          spawn, ImmutableSet.of(), /* scrubber= */ null, context, blobPolicy);
    } catch (LostInputsExecException e) {
      throw e;
    } catch (IOException e) {
      if (e instanceof FileNotFoundException || e.getCause() instanceof FileNotFoundException) {
        context.checkForLostInputs();
      }
      throw SandboxBackendClient.toUserExecException(
          "Failed to build Merkle tree for local REAPI sandbox", e);
    }
  }

  @Override
  protected SubprocessResult run(
      Spawn originalSpawn, SandboxedSpawn sandbox, SpawnExecutionContext context)
      throws IOException, ExecException, InterruptedException {
    SandboxBackendSpawn reapiSpawn = (SandboxBackendSpawn) sandbox;
    reapiSpawn.rethrowPendingExecException();

    Instant startTime = Instant.now();
    for (int attempt = 0; ; attempt++) {
      if (attempt > 0) {
        MerkleTree retryTree =
            buildMerkleTreeForSpawn(originalSpawn, context, BlobPolicy.KEEP_AND_REUPLOAD);
        reapiSpawn.setMerkleTree(retryTree);
        reapiSpawn.prepareSharedCas(/* forceReupload= */ true);
        reapiSpawn.rethrowPendingExecException();
      }

      ExecuteResponse executeResponse =
          client.executeAction(reapiSpawn.getActionDigest(), reapiSpawn.getRequestMetadata());
      com.google.rpc.Status rpcStatus = executeResponse.getStatus();
      Set<String> missingHashes = new HashSet<>();
      if (SandboxBackendClient.isMissingBlobsFailure(rpcStatus, missingHashes)
          && attempt < MAX_MISSING_RETRIES) {
        client.getUploader().evictByHashes(missingHashes);
        continue;
      }

      boolean timedOut = rpcStatus.getCode() == com.google.rpc.Code.DEADLINE_EXCEEDED.getNumber();
      if (rpcStatus.getCode() != com.google.rpc.Code.OK.getNumber() && !timedOut) {
        throw new UserExecException(
            FailureDetail.newBuilder()
                .setMessage(
                    "Local REAPI sandbox execution failed: "
                        + rpcStatus.getMessage()
                        + client.describeProcessState())
                .setSandbox(Sandbox.newBuilder().setCode(Code.EXECUTION_IO_EXCEPTION))
                .build());
      }

      Duration wallTime = Duration.between(startTime, Instant.now());
      ActionResult actionResult = executeResponse.getResult();
      int exitCode = timedOut ? SpawnResult.POSIX_TIMEOUT_EXIT_CODE : actionResult.getExitCode();
      reapiSpawn.setExecutionOutcome(actionResult, exitCode);

      if (timedOut) {
        String failureMessage = makeFailureMessage(originalSpawn, sandbox);
        FailureDetail failureDetail =
            FailureDetail.newBuilder()
                .setMessage(failureMessage)
                .setSpawn(
                    FailureDetails.Spawn.newBuilder().setCode(FailureDetails.Spawn.Code.TIMEOUT))
                .build();
        return new SubprocessResult(
            SpawnResult.POSIX_TIMEOUT_EXIT_CODE,
            Status.TIMEOUT,
            wallTime,
            startTime,
            failureMessage,
            failureDetail,
            /* statisticsPath= */ null);
      }

      if (exitCode != 0) {
        String failureMessage = makeFailureMessage(originalSpawn, sandbox);
        FailureDetail failureDetail =
            FailureDetail.newBuilder()
                .setMessage(failureMessage)
                .setSpawn(
                    FailureDetails.Spawn.newBuilder()
                        .setCode(FailureDetails.Spawn.Code.NON_ZERO_EXIT)
                        .setSpawnExitCode(exitCode))
                .build();
        return new SubprocessResult(
            exitCode,
            Status.NON_ZERO_EXIT,
            wallTime,
            startTime,
            failureMessage,
            failureDetail,
            /* statisticsPath= */ null);
      }

      return new SubprocessResult(
          /* exitCode= */ 0,
          Status.SUCCESS,
          wallTime,
          startTime,
          /* failureMessage= */ "",
          /* failureDetail= */ null,
          /* statisticsPath= */ null);
    }
  }

  @Override
  public void verifyPostCondition(
      Spawn originalSpawn, SandboxedSpawn sandbox, SpawnExecutionContext context)
      throws IOException {
    SandboxBackendSpawn reapiSpawn = (SandboxBackendSpawn) sandbox;
    if (reapiSpawn.getExitCode() == 0 && reapiSpawn.getActionResult() != null) {
      client.verifyOutputBlobsInCas(reapiSpawn.getActionResult());
    }
  }

  /**
   * {@link SandboxedSpawn} implementation mapping input preparation ({@link #createFileSystem()})
   * and output materialization ({@link #copyOutputs(Path)}) onto {@link
   * AbstractSandboxSpawnRunner}'s template lifecycle.
   */
  private final class SandboxBackendSpawn implements SandboxedSpawn {
    private final Spawn spawn;
    private final SpawnExecutionContext context;
    private final ImmutableMap<String, String> environment;
    private final SandboxOutputs outputs;
    private final byte[] commandBytes;
    private final Digest commandDigest;
    private final Platform platform;
    private final boolean skipUploader;

    private MerkleTree merkleTree;
    private Digest actionDigest;
    private RequestMetadata requestMetadata;
    @Nullable private ActionResult actionResult;
    private int exitCode;
    private boolean allMandatoryOutputsMaterialized;
    @Nullable private ExecException pendingExecException;

    SandboxBackendSpawn(
        Spawn spawn,
        SpawnExecutionContext context,
        ImmutableMap<String, String> environment,
        SandboxOutputs outputs,
        byte[] commandBytes,
        Digest commandDigest,
        Platform platform,
        MerkleTree merkleTree,
        boolean skipUploader) {
      this.spawn = spawn;
      this.context = context;
      this.environment = environment;
      this.outputs = outputs;
      this.commandBytes = commandBytes;
      this.commandDigest = commandDigest;
      this.platform = platform;
      this.merkleTree = merkleTree;
      this.skipUploader = skipUploader;
    }

    @Override
    public Path getSandboxExecRoot() {
      return execRoot;
    }

    @Override
    public ImmutableList<String> getArguments() {
      return spawn.getArguments();
    }

    @Override
    public ImmutableMap<String, String> getEnvironment() {
      return environment;
    }

    @Override
    public String getMnemonic() {
      return spawn.getMnemonic();
    }

    @Nullable
    @Override
    public Path getSandboxDebugPath() {
      return null;
    }

    @Nullable
    @Override
    public Path getStatisticsPath() {
      return null;
    }

    @Override
    public void createFileSystem() throws IOException, InterruptedException {
      prepareSharedCas(/* forceReupload= */ false);
    }

    void setMerkleTree(MerkleTree merkleTree) {
      this.merkleTree = merkleTree;
    }

    void prepareSharedCas(boolean forceReupload) throws IOException, InterruptedException {
      Duration timeout = context.getTimeout();
      Action.Builder actionBuilder =
          Action.newBuilder()
              .setCommandDigest(commandDigest)
              .setInputRootDigest(merkleTree.digest())
              .setDoNotCache(true)
              .setPlatform(platform);
      if (!timeout.isZero()) {
        actionBuilder.setTimeout(Durations.fromMillis(timeout.toMillis()));
      }
      Action action = actionBuilder.build();
      byte[] actionBytes = action.toByteArray();
      this.actionDigest = digestUtil.compute(actionBytes);
      this.requestMetadata = buildRequestMetadata(spawn, actionDigest);

      RemoteActionExecutionContext remoteContext =
          RemoteActionExecutionContext.create(spawn, context, requestMetadata);
      DiskCacheMerkleTreeUploader uploader = client.getUploader();
      try (SilentCloseable c = Profiler.instance().profile("SandboxBackend.prepareSharedCas")) {
        if (!skipUploader && merkleTree instanceof MerkleTree.Uploadable uploadable) {
          uploader.ensureInputsPresent(remoteContext, uploadable, forceReupload);
        }
        uploader.uploadActionBlobs(
            commandDigest, commandBytes, actionDigest, actionBytes, forceReupload);
      } catch (IOException e) {
        if (e instanceof FileNotFoundException || e.getCause() instanceof FileNotFoundException) {
          try {
            context.checkForLostInputs();
          } catch (LostInputsExecException lostInputsException) {
            this.pendingExecException = lostInputsException;
            return;
          }
        }
        throw e;
      }
    }

    void rethrowPendingExecException() throws ExecException {
      if (pendingExecException != null) {
        throw pendingExecException;
      }
    }

    Digest getActionDigest() {
      return actionDigest;
    }

    RequestMetadata getRequestMetadata() {
      return requestMetadata;
    }

    void setExecutionOutcome(ActionResult actionResult, int exitCode) {
      this.actionResult = actionResult;
      this.exitCode = exitCode;
    }

    @Nullable
    ActionResult getActionResult() {
      return actionResult;
    }

    int getExitCode() {
      return exitCode;
    }

    @Override
    public void copyOutputs(Path targetExecRoot) throws IOException {
      if (actionResult == null) {
        return;
      }
      try (SilentCloseable c = Profiler.instance().profile("SandboxBackend.finalizeOutputs")) {
        client.writeOutErr(actionResult, context.getFileOutErr());
        Set<PathFragment> materialized =
            client.finalizeOutputsFromDiskCache(
                targetExecRoot,
                digestUtil,
                outputs,
                actionResult,
                spawn,
                exitCode == 0 ? context.getOutputMetadataStore() : null);
        if (exitCode == 0) {
          boolean allPresent = true;
          for (ActionInput output : spawn.getOutputFiles()) {
            if (spawn.isMandatoryOutput(output)
                && !materialized.contains(output.getExecPath())) {
              allPresent = false;
              break;
            }
          }
          this.allMandatoryOutputsMaterialized = allPresent;
        }
      }
    }

    @Nullable
    @Override
    public ByteString getDiskCacheActionResultBytes() {
      if (!sharesConfiguredDiskCache
          || exitCode != 0
          || actionResult == null
          || !allMandatoryOutputsMaterialized
          || "TestRunner".equals(spawn.getMnemonic())
          || !spawn.getPathMapper().isNoop()
          || spawn.getStdout() != null) {
        return null;
      }
      if ((!actionResult.getStdoutRaw().isEmpty() && !actionResult.hasStdoutDigest())
          || (!actionResult.getStderrRaw().isEmpty() && !actionResult.hasStderrDigest())) {
        return null;
      }
      for (OutputFile outputFile : actionResult.getOutputFilesList()) {
        if (!outputFile.getContents().isEmpty() && outputFile.getDigest().getSizeBytes() > 0) {
          return null;
        }
      }
      ActionResult.Builder normalized = actionResult.toBuilder();
      if (normalized.hasStdoutDigest()) {
        normalized.clearStdoutRaw();
      }
      if (normalized.hasStderrDigest()) {
        normalized.clearStderrRaw();
      }
      return normalized.build().toByteString();
    }

    @Override
    public void delete() {
      // Sandbox directory cleanup is handled by the REAPI sandbox backend at the end of Execute
      // (unless --sandbox_debug is enabled).
    }
  }

  private RequestMetadata buildRequestMetadata(Spawn spawn, Digest actionDigest) {
    ActionExecutionMetadata owner = spawn.getResourceOwner();
    String mnemonic = owner != null ? owner.getMnemonic() : null;
    String label =
        owner != null && owner.getOwner() != null && owner.getOwner().getLabel() != null
            ? owner.getOwner().getLabel().getCanonicalForm()
            : null;
    String configId =
        owner != null && owner.getOwner() != null
            ? owner.getOwner().getConfigurationChecksum()
            : null;
    return TracingMetadataUtils.buildMetadata(
        buildRequestId, commandId, actionDigest.getHash(), mnemonic, label, configId);
  }

  private Command buildCommand(
      Spawn spawn, ImmutableMap<String, String> environment, SandboxOutputs outputs) {
    Command.Builder commandBuilder = Command.newBuilder();
    for (String arg : spawn.getArguments()) {
      commandBuilder.addArguments(StringEncoding.internalToUnicode(arg));
    }

    environment.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry ->
                commandBuilder
                    .addEnvironmentVariablesBuilder()
                    .setName(StringEncoding.internalToUnicode(entry.getKey()))
                    .setValue(StringEncoding.internalToUnicode(entry.getValue())));

    TreeSet<String> outputPaths = new TreeSet<>();
    for (PathFragment mapped : outputs.files().values()) {
      outputPaths.add(StringEncoding.internalToUnicode(mapped.getPathString()));
    }
    TreeSet<String> outputDirs = new TreeSet<>();
    for (PathFragment mapped : outputs.dirs().values()) {
      String unicodePath = StringEncoding.internalToUnicode(mapped.getPathString());
      outputPaths.add(unicodePath);
      outputDirs.add(unicodePath);
    }
    return commandBuilder
        .addAllOutputPaths(outputPaths)
        .addAllOutputDirectories(outputDirs)
        .build();
  }

  private Platform buildPlatform(
      Spawn spawn, SpawnExecutionContext context, ResourceSet localResources) {
    SandboxOptions sandboxOptions = getSandboxOptions();
    List<Platform.Property> properties = new ArrayList<>();
    properties.add(
        Platform.Property.newBuilder()
            .setName(PLATFORM_EXEC_ROOT)
            .setValue(execRoot.getPathString())
            .build());
    properties.add(
        Platform.Property.newBuilder()
            .setName(PLATFORM_DISK_CACHE)
            .setValue(client.getDiskCacheRoot().getPathString())
            .build());
    properties.add(
        Platform.Property.newBuilder()
            .setName(PLATFORM_CAS_ROOT)
            .setValue(client.getUploader().getCasRoot().getPathString())
            .build());
    properties.add(
        Platform.Property.newBuilder()
            .setName(PLATFORM_SANDBOX_BASE)
            .setValue(sandboxBase.getPathString())
            .build());
    properties.add(
        Platform.Property.newBuilder()
            .setName(PLATFORM_SANDBOX_ID)
            .setValue(Integer.toString(context.getId()))
            .build());
    properties.add(
        Platform.Property.newBuilder()
            .setName(PLATFORM_ALLOW_NETWORK)
            .setValue(
                Boolean.toString(
                    Spawns.requiresNetwork(spawn, sandboxOptions.getDefaultSandboxAllowNetwork())))
            .build());
    properties.add(
        Platform.Property.newBuilder()
            .setName(PLATFORM_SANDBOX_DEBUG)
            .setValue(Boolean.toString(sandboxOptions.getSandboxDebug()))
            .build());
    properties.add(
        Platform.Property.newBuilder()
            .setName(PLATFORM_RESOURCES_CPU)
            .setValue(Double.toString(localResources.getCpuUsage()))
            .build());
    properties.add(
        Platform.Property.newBuilder()
            .setName(PLATFORM_RESOURCES_MEMORY_MB)
            .setValue(Double.toString(localResources.getMemoryMb()))
            .build());
    for (PathFragment writablePath : sandboxOptions.getSandboxWritablePath()) {
      properties.add(
          Platform.Property.newBuilder()
              .setName(PLATFORM_WRITABLE_PATH)
              .setValue(writablePath.getPathString())
              .build());
    }
    for (PathFragment tmpfsPath : sandboxOptions.getSandboxTmpfsPath()) {
      properties.add(
          Platform.Property.newBuilder()
              .setName(PLATFORM_TMPFS_PATH)
              .setValue(tmpfsPath.getPathString())
              .build());
    }
    for (Path inaccessiblePath : getInaccessiblePaths()) {
      properties.add(
          Platform.Property.newBuilder()
              .setName(PLATFORM_INACCESSIBLE_PATH)
              .setValue(inaccessiblePath.getPathString())
              .build());
    }
    properties.sort(
        Comparator.comparing(Platform.Property::getName)
            .thenComparing(Platform.Property::getValue));

    return Platform.newBuilder().addAllProperties(properties).build();
  }

  @Override
  public void cleanupSandboxBase(Path sandboxBase, @Nullable TreeDeleter treeDeleter)
      throws IOException {
    Path backendStagingRoot = sandboxBase.getRelative("reapi-" + backendName);
    if (backendStagingRoot.exists(Symlinks.NOFOLLOW)) {
      if (treeDeleter != null) {
        treeDeleter.deleteTree(backendStagingRoot);
      } else {
        backendStagingRoot.deleteTree();
      }
    }
  }

  @Override
  public void close() {
    if (ownsClient) {
      client.close();
    }
  }
}
