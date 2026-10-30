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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.CacheCapabilities;
import build.bazel.remote.execution.v2.CapabilitiesGrpc.CapabilitiesImplBase;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionCapabilities;
import build.bazel.remote.execution.v2.ExecutionGrpc.ExecutionImplBase;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.GetCapabilitiesRequest;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.bazel.remote.execution.v2.Tree;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.build.lib.actions.ActionContext;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputHelper;
import com.google.devtools.build.lib.actions.Artifact.SpecialArtifact;
import com.google.devtools.build.lib.actions.Artifact.TreeFileArtifact;
import com.google.devtools.build.lib.actions.ArtifactPathResolver;
import com.google.devtools.build.lib.actions.ArtifactRoot;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.actions.InputMetadataProvider;
import com.google.devtools.build.lib.actions.LocalHostCapacity;
import com.google.devtools.build.lib.actions.ParamFileActionInput;
import com.google.devtools.build.lib.actions.ParameterFile.ParameterFileType;
import com.google.devtools.build.lib.actions.ResourceSet;
import com.google.devtools.build.lib.actions.Spawn;
import com.google.devtools.build.lib.actions.SpawnMetrics;
import com.google.devtools.build.lib.actions.SpawnMetrics.ExecKind;
import com.google.devtools.build.lib.actions.SpawnResult;
import com.google.devtools.build.lib.actions.SpawnStrategy;
import com.google.devtools.build.lib.actions.util.ActionsTestUtil;
import com.google.devtools.build.lib.exec.Protos.Digest;
import com.google.devtools.build.lib.exec.SpawnRunner.ProgressStatus;
import com.google.devtools.build.lib.exec.SpawnRunner.SpawnExecutionContext;
import com.google.devtools.build.lib.exec.SpawnStrategyRegistry;
import com.google.devtools.build.lib.exec.util.FakeActionInputFileCache;
import com.google.devtools.build.lib.exec.util.SpawnBuilder;
import com.google.devtools.build.lib.remote.common.RemoteActionExecutionContext;
import com.google.devtools.build.lib.remote.merkletree.MerkleTree;
import com.google.devtools.build.lib.remote.merkletree.MerkleTreeComputer;
import com.google.devtools.build.lib.remote.merkletree.MerkleTreeComputer.BlobPolicy;
import com.google.devtools.build.lib.remote.merkletree.MerkleTreeUploader;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import com.google.devtools.build.lib.runtime.CommandEnvironment;
import com.google.devtools.build.lib.sandbox.SpawnRunnerTestUtil.SpawnExecutionContextForTesting;
import com.google.devtools.build.lib.skyframe.TreeArtifactValue;
import com.google.devtools.build.lib.util.AbruptExitException;
import com.google.devtools.build.lib.util.DeterministicWriter;
import com.google.devtools.build.lib.util.OS;
import com.google.devtools.build.lib.util.io.FileOutErr;
import com.google.devtools.build.lib.vfs.FileSystem;
import com.google.devtools.build.lib.vfs.FileSystemUtils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.SyscallCache;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.Status;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerDomainSocketChannel;
import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SandboxBackendSpawnRunner}, {@link SandboxBackendClient}, and {@link SandboxModule}. */
@RunWith(JUnit4.class)
public final class SandboxBackendSpawnRunnerTest extends SandboxedSpawnRunnerTestCase {

  private CommandEnvironment commandEnvironment;
  private Path execRoot;
  private Path diskCacheRoot;
  private FakeReapiDaemon fakeDaemon;
  private SandboxBackendSpawnRunner runner;

  @Before
  public void setUpServerAndRunner() throws Exception {
    assumeTrue(OS.getCurrent() != OS.WINDOWS);

    commandEnvironment = runtimeWrapper.newCommand();
    commandEnvironment
        .getLocalResourceManager()
        .setAvailableResources(LocalHostCapacity.getLocalHostCapacity());

    execRoot = commandEnvironment.getExecRoot();
    execRoot.createDirectoryAndParents();

    diskCacheRoot = DiskCacheMerkleTreeUploader.resolveDiskCacheRoot(commandEnvironment);
    diskCacheRoot.createDirectoryAndParents();

    Path sandboxBase = execRoot.getRelative("reapi-sandbox-base");
    sandboxBase.createDirectoryAndParents();

    String serverName = "local-reapi-sandbox-" + System.nanoTime();
    fakeDaemon =
        new FakeReapiDaemon(
            sandboxBase.getPathFile().toPath(), diskCacheRoot.getPathFile().toPath(), serverName);
    runner = new SandboxBackendSpawnRunner(commandEnvironment, fakeDaemon.getTarget());
  }

  @After
  public void tearDownServerAndRunner() {
    if (runner != null) {
      runner.close();
    }
    if (fakeDaemon != null) {
      fakeDaemon.close();
    }
  }

  @Test
  public void exec_simpleCommand_capturesStdoutStderrWithoutCasRpcs() throws Exception {
    assertThat(runner).isInstanceOf(AbstractSandboxSpawnRunner.class);
    Spawn spawn =
        new SpawnBuilder("/bin/sh", "-c", "echo hello-stdout; echo hello-stderr >&2").build();
    FileOutErr fileOutErr =
        new FileOutErr(testRoot.getChild("stdout"), testRoot.getChild("stderr"));
    SpawnExecutionContext context =
        createContext(spawn, fileOutErr, Duration.ofSeconds(30), new FakeActionInputFileCache());

    SpawnResult result = runner.exec(spawn, context);

    assertThat(result.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(fileOutErr.outAsLatin1()).isEqualTo("hello-stdout\n");
    assertThat(fileOutErr.errAsLatin1()).isEqualTo("hello-stderr\n");

    SpawnMetrics metrics = result.getMetrics();
    assertThat(metrics.execKind()).isEqualTo(ExecKind.LOCAL);
    assertThat(metrics.totalTimeInMs()).isAtLeast(0);
    assertThat(fakeDaemon.getExecutedActions()).isEqualTo(1);
  }

  @Test
  public void exec_withDeclaredInputs_stagesInputsFromDiskCacheAndHidesUndeclaredFiles()
      throws Exception {
    Path declaredInputPath = execRoot.getRelative("pkg/declared.txt");
    declaredInputPath.getParentDirectory().createDirectoryAndParents();
    FileSystemUtils.writeContent(declaredInputPath, UTF_8, "declared-payload\n");

    Path undeclaredSecretPath = execRoot.getRelative("pkg/undeclared_secret.txt");
    FileSystemUtils.writeContent(undeclaredSecretPath, UTF_8, "top-secret\n");

    ActionInput declaredInput = ActionInputHelper.fromPath("pkg/declared.txt");
    Spawn spawn =
        new SpawnBuilder(
                "/bin/sh",
                "-c",
                "test -f pkg/declared.txt"
                    + " && ! test -e pkg/undeclared_secret.txt"
                    + " && cat pkg/declared.txt > out/result.txt")
            .withInputs(declaredInput)
            .withOutputs("out/result.txt")
            .build();

    FileOutErr fileOutErr =
        new FileOutErr(testRoot.getChild("stdout"), testRoot.getChild("stderr"));
    SpawnExecutionContext context =
        createContext(spawn, fileOutErr, Duration.ofSeconds(30), new FakeActionInputFileCache());

    SpawnResult result = runner.exec(spawn, context);

    assertThat(result.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(result.exitCode()).isEqualTo(0);
    Path outputPath = execRoot.getRelative("out/result.txt");
    assertThat(outputPath.exists()).isTrue();
    assertThat(FileSystemUtils.readContent(outputPath, UTF_8)).isEqualTo("declared-payload\n");
    assertThat(runner.getUploader().getUploadedFileCount()).isEqualTo(1);
  }

  @Test
  public void exec_withVirtualActionInput_materializesParamFileViaDiskCache() throws Exception {
    ParamFileActionInput paramFile =
        new ParamFileActionInput(
            PathFragment.create("params/args.params"),
            ImmutableList.of("--flag_one", "--flag_two=42"),
            ParameterFileType.UNQUOTED);

    Spawn spawn =
        new SpawnBuilder("/bin/sh", "-c", "cp params/args.params out.txt")
            .withInputs(paramFile)
            .withOutputs("out.txt")
            .build();

    FileOutErr fileOutErr =
        new FileOutErr(testRoot.getChild("stdout"), testRoot.getChild("stderr"));
    SpawnExecutionContext context =
        createContext(spawn, fileOutErr, Duration.ofSeconds(30), new FakeActionInputFileCache());

    SpawnResult result = runner.exec(spawn, context);

    assertThat(result.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(FileSystemUtils.readContent(execRoot.getRelative("out.txt"), UTF_8))
        .isEqualTo("--flag_one\n--flag_two=42\n");
  }

  @Test
  public void exec_withTreeArtifactAndGeneratedOutput_deduplicatesAndReusesDiskCacheBlobs()
      throws Exception {
    ArtifactRoot derivedRoot =
        ArtifactRoot.asDerivedRoot(execRoot, ArtifactRoot.RootType.OUTPUT, "bazel-out");
    derivedRoot.getRoot().asPath().createDirectoryAndParents();

    SpecialArtifact treeArtifact =
        ActionsTestUtil.createTreeArtifactWithGeneratingAction(derivedRoot, "pkg/my_tree_artifact");
    treeArtifact.getPath().createDirectoryAndParents();
    TreeFileArtifact child1 = TreeFileArtifact.createTreeOutput(treeArtifact, "a.txt");
    TreeFileArtifact child2 = TreeFileArtifact.createTreeOutput(treeArtifact, "sub/b.txt");
    child1.getPath().getParentDirectory().createDirectoryAndParents();
    child2.getPath().getParentDirectory().createDirectoryAndParents();
    FileSystemUtils.writeContent(child1.getPath(), UTF_8, "alpha");
    FileSystemUtils.writeContent(child2.getPath(), UTF_8, "beta");

    TreeArtifactValue treeValue =
        TreeArtifactValue.newBuilder(treeArtifact)
            .putChild(child1, FileArtifactValue.createForTesting(child1))
            .putChild(child2, FileArtifactValue.createForTesting(child2))
            .build();
    FakeActionInputFileCache fakeMetadataProvider = new FakeActionInputFileCache();
    fakeMetadataProvider.putTreeArtifact(treeArtifact, treeValue);

    Spawn spawn1 =
        new SpawnBuilder(
                "/bin/sh",
                "-c",
                "cat bazel-out/pkg/my_tree_artifact/a.txt"
                    + " bazel-out/pkg/my_tree_artifact/sub/b.txt > bazel-out/pkg/out1.txt")
            .withInputs(treeArtifact)
            .withOutputs("bazel-out/pkg/out1.txt")
            .build();
    FileOutErr outErr1 = new FileOutErr(testRoot.getChild("stdout1"), testRoot.getChild("stderr1"));
    SpawnResult result1 =
        runner.exec(
            spawn1, createContext(spawn1, outErr1, Duration.ofSeconds(30), fakeMetadataProvider));
    assertThat(result1.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    Path out1Path = execRoot.getRelative("bazel-out/pkg/out1.txt");
    assertThat(FileSystemUtils.readContent(out1Path, UTF_8)).isEqualTo("alphabeta");

    int filesPopulatedAfterSpawn1 = runner.getUploader().getUploadedFileCount();
    assertThat(filesPopulatedAfterSpawn1).isEqualTo(2);

    ActionInput generatedOut1Input = ActionInputHelper.fromPath("bazel-out/pkg/out1.txt");
    fakeMetadataProvider.put(generatedOut1Input, FileArtifactValue.createForTesting(out1Path));
    Spawn spawn2 =
        new SpawnBuilder(
                "/bin/sh",
                "-c",
                "cat bazel-out/pkg/out1.txt bazel-out/pkg/my_tree_artifact/a.txt > out2.txt")
            .withInputs(treeArtifact, generatedOut1Input)
            .withOutputs("out2.txt")
            .build();
    FileOutErr outErr2 = new FileOutErr(testRoot.getChild("stdout2"), testRoot.getChild("stderr2"));
    SpawnResult result2 =
        runner.exec(
            spawn2, createContext(spawn2, outErr2, Duration.ofSeconds(30), fakeMetadataProvider));
    assertThat(result2.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(FileSystemUtils.readContent(execRoot.getRelative("out2.txt"), UTF_8))
        .isEqualTo("alphabetaalpha");
    assertThat(runner.getUploader().getUploadedFileCount()).isEqualTo(filesPopulatedAfterSpawn1);
  }

  @Test
  public void exec_withTreeOutputAndSymlinkOutput_materializesOutputsToExecRoot() throws Exception {
    ArtifactRoot derivedRoot =
        ArtifactRoot.asDerivedRoot(execRoot, ArtifactRoot.RootType.OUTPUT, "bazel-out");
    derivedRoot.getRoot().asPath().createDirectoryAndParents();
    SpecialArtifact outTree =
        ActionsTestUtil.createTreeArtifactWithGeneratingAction(derivedRoot, "pkg/out_tree");

    Spawn spawn =
        new SpawnBuilder(
                "/bin/sh",
                "-c",
                "mkdir -p bazel-out/pkg/out_tree/nested"
                    + " && printf 'leaf' > bazel-out/pkg/out_tree/nested/f.txt"
                    + " && ln -s f.txt bazel-out/pkg/out_tree/nested/link.txt"
                    + " && ln -s target.txt out_sym")
            .withOutputs(outTree, ActionInputHelper.fromPath("out_sym"))
            .build();

    FileOutErr outErr = new FileOutErr(testRoot.getChild("tree_out"), testRoot.getChild("tree_err"));
    SpawnResult res =
        runner.exec(
            spawn, createContext(spawn, outErr, Duration.ofSeconds(30), new FakeActionInputFileCache()));

    assertThat(res.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(
            FileSystemUtils.readContent(
                execRoot.getRelative("bazel-out/pkg/out_tree/nested/f.txt"), UTF_8))
        .isEqualTo("leaf");
    assertThat(
            execRoot
                .getRelative("bazel-out/pkg/out_tree/nested/link.txt")
                .readSymbolicLink())
        .isEqualTo(PathFragment.create("f.txt"));
    assertThat(execRoot.getRelative("out_sym").readSymbolicLink())
        .isEqualTo(PathFragment.create("target.txt"));
  }

  @Test
  public void exec_acrossIndependentSessions_reusesSharedDiskCacheBlobs() throws Exception {
    Path srcFile = execRoot.getRelative("pkg/shared_source.cc");
    srcFile.getParentDirectory().createDirectoryAndParents();
    FileSystemUtils.writeContent(srcFile, UTF_8, "int main() { return 0; }\n");
    ActionInput srcInput = ActionInputHelper.fromPath("pkg/shared_source.cc");
    FakeActionInputFileCache metadataProvider = new FakeActionInputFileCache();
    metadataProvider.put(srcInput, FileArtifactValue.createForTesting(srcFile));

    Spawn spawn1 =
        new SpawnBuilder("/bin/sh", "-c", "cp pkg/shared_source.cc out_ws1.cc")
            .withInputs(srcInput)
            .withOutputs("out_ws1.cc")
            .build();
    FileOutErr outErr1 = new FileOutErr(testRoot.getChild("ws1_out"), testRoot.getChild("ws1_err"));
    SpawnResult res1 =
        runner.exec(
            spawn1, createContext(spawn1, outErr1, Duration.ofSeconds(30), metadataProvider));
    assertThat(res1.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(runner.getUploader().getUploadedFileCount()).isEqualTo(1);

    try (SandboxBackendSpawnRunner runner2 =
        new SandboxBackendSpawnRunner(commandEnvironment, fakeDaemon.getTarget(), diskCacheRoot)) {
      Spawn spawn2 =
          new SpawnBuilder("/bin/sh", "-c", "cp pkg/shared_source.cc out_ws2.cc")
              .withInputs(srcInput)
              .withOutputs("out_ws2.cc")
              .build();
      FileOutErr outErr2 =
          new FileOutErr(testRoot.getChild("ws2_out"), testRoot.getChild("ws2_err"));
      SpawnResult res2 =
          runner2.exec(
              spawn2, createContext(spawn2, outErr2, Duration.ofSeconds(30), metadataProvider));
      assertThat(res2.status()).isEqualTo(SpawnResult.Status.SUCCESS);
      assertThat(FileSystemUtils.readContent(execRoot.getRelative("out_ws2.cc"), UTF_8))
          .isEqualTo("int main() { return 0; }\n");
      assertThat(runner2.getUploader().getUploadedFileCount()).isEqualTo(0);
      assertThat(runner2.getUploader().getDiskCacheHitCount()).isAtLeast(1);
    }
  }

  @Test
  public void exec_nonZeroExitAndTimeout_returnsExpectedStatuses() throws Exception {
    Spawn failingSpawn = new SpawnBuilder("/bin/sh", "-c", "exit 17").build();
    FileOutErr outErr1 =
        new FileOutErr(testRoot.getChild("stdout_fail"), testRoot.getChild("stderr_fail"));
    SpawnResult failResult =
        runner.exec(
            failingSpawn,
            createContext(
                failingSpawn, outErr1, Duration.ofSeconds(30), new FakeActionInputFileCache()));
    assertThat(failResult.status()).isEqualTo(SpawnResult.Status.NON_ZERO_EXIT);
    assertThat(failResult.exitCode()).isEqualTo(17);

    Spawn slowSpawn = new SpawnBuilder("/bin/sh", "-c", "sleep 10").build();
    FileOutErr outErr2 =
        new FileOutErr(testRoot.getChild("stdout_timeout"), testRoot.getChild("stderr_timeout"));
    SpawnResult timeoutResult =
        runner.exec(
            slowSpawn,
            createContext(
                slowSpawn, outErr2, Duration.ofMillis(100), new FakeActionInputFileCache()));
    assertThat(timeoutResult.status()).isEqualTo(SpawnResult.Status.TIMEOUT);
    assertThat(timeoutResult.exitCode()).isEqualTo(SpawnResult.POSIX_TIMEOUT_EXIT_CODE);
  }

  @Test
  public void
      exec_afterInPlaceSourceMutationAndCasEviction_preservesCasIntegrityAndReuploadsMissingBlobs()
          throws Exception {
    ArtifactRoot derivedRoot =
        ArtifactRoot.asDerivedRoot(execRoot, ArtifactRoot.RootType.OUTPUT, "bazel-out");
    derivedRoot.getRoot().asPath().createDirectoryAndParents();

    Path aPath = execRoot.getRelative("pkg/a.txt");
    aPath.getParentDirectory().createDirectoryAndParents();
    FileSystemUtils.writeContent(aPath, UTF_8, "content-v1");
    ActionInput aInput = ActionInputHelper.fromPath("pkg/a.txt");

    SpecialArtifact treeArtifact =
        ActionsTestUtil.createTreeArtifactWithGeneratingAction(derivedRoot, "pkg/cached_tree");
    treeArtifact.getPath().createDirectoryAndParents();
    TreeFileArtifact bChild = TreeFileArtifact.createTreeOutput(treeArtifact, "sub/b.txt");
    bChild.getPath().getParentDirectory().createDirectoryAndParents();
    FileSystemUtils.writeContent(bChild.getPath(), UTF_8, "content-v1");

    TreeArtifactValue treeValue =
        TreeArtifactValue.newBuilder(treeArtifact)
            .putChild(bChild, FileArtifactValue.createForTesting(bChild))
            .build();
    FakeActionInputFileCache metadataProvider = new FakeActionInputFileCache();
    metadataProvider.putTreeArtifact(treeArtifact, treeValue);
    metadataProvider.put(aInput, FileArtifactValue.createForTesting(aPath));

    Spawn spawn1 =
        new SpawnBuilder(
                "/bin/sh", "-c", "cat pkg/a.txt bazel-out/pkg/cached_tree/sub/b.txt > out1.txt")
            .withInputs(aInput, treeArtifact)
            .withOutputs("out1.txt")
            .build();
    FileOutErr outErr1 =
        new FileOutErr(testRoot.getChild("s1_stdout"), testRoot.getChild("s1_stderr"));
    SpawnResult res1 =
        runner.exec(
            spawn1, createContext(spawn1, outErr1, Duration.ofSeconds(30), metadataProvider));
    assertThat(res1.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(FileSystemUtils.readContent(execRoot.getRelative("out1.txt"), UTF_8))
        .isEqualTo("content-v1content-v1");

    // Mutate and delete pkg/a.txt in-place; the copied CAS blob for D1 must remain intact.
    FileSystemUtils.writeContent(aPath, UTF_8, "content-v2");
    aPath.delete();

    Spawn treeSpawn =
        new SpawnBuilder("/bin/sh", "-c", "cat bazel-out/pkg/cached_tree/sub/b.txt > out2.txt")
            .withInputs(treeArtifact)
            .withOutputs("out2.txt")
            .build();
    FileOutErr outErr2 =
        new FileOutErr(testRoot.getChild("s2_stdout"), testRoot.getChild("s2_stderr"));
    SpawnResult res2 =
        runner.exec(
            treeSpawn, createContext(treeSpawn, outErr2, Duration.ofSeconds(30), metadataProvider));
    assertThat(res2.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(FileSystemUtils.readContent(execRoot.getRelative("out2.txt"), UTF_8))
        .isEqualTo("content-v1");
    assertThat(fakeDaemon.getMissingPreconditionCount()).isEqualTo(0);

    // Simulate --disk_cache GC eviction of D1 while treeArtifact stays cached in MerkleTreeComputer.
    byte[] d1Raw = MessageDigest.getInstance("SHA-256").digest("content-v1".getBytes(UTF_8));
    String d1Hash = BaseEncoding.base16().lowerCase().encode(d1Raw);
    Path d1CasPath = runner.getUploader().toCasPath(d1Hash);
    assertThat(d1CasPath.exists()).isTrue();
    d1CasPath.delete();

    FileOutErr outErr3 =
        new FileOutErr(testRoot.getChild("s3_stdout"), testRoot.getChild("s3_stderr"));
    SpawnResult res3 =
        runner.exec(
            treeSpawn, createContext(treeSpawn, outErr3, Duration.ofSeconds(30), metadataProvider));
    assertThat(res3.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(FileSystemUtils.readContent(execRoot.getRelative("out2.txt"), UTF_8))
        .isEqualTo("content-v1");
    assertThat(fakeDaemon.getMissingPreconditionCount()).isEqualTo(1);
    assertThat(d1CasPath.exists()).isTrue();
  }

  @Test
  public void exec_coexistingWithRemoteMerkleTreeComputer_maintainsIsolatedSubtreeUploadScopes()
      throws Exception {
    ArtifactRoot derivedRoot =
        ArtifactRoot.asDerivedRoot(execRoot, ArtifactRoot.RootType.OUTPUT, "bazel-out");
    derivedRoot.getRoot().asPath().createDirectoryAndParents();
    SpecialArtifact sharedTree =
        ActionsTestUtil.createTreeArtifactWithGeneratingAction(derivedRoot, "pkg/shared_tree");
    sharedTree.getPath().createDirectoryAndParents();

    TreeArtifactValue.Builder tb = TreeArtifactValue.newBuilder(sharedTree);
    for (int i = 0; i < 3; i++) {
      TreeFileArtifact child = TreeFileArtifact.createTreeOutput(sharedTree, "item_" + i + ".txt");
      FileSystemUtils.writeContentAsLatin1(child.getPath(), "tree_item_" + i);
      tb.putChild(child, FileArtifactValue.createForTesting(child.getPath()));
    }
    FakeActionInputFileCache meta = new FakeActionInputFileCache();
    meta.putTreeArtifact(sharedTree, tb.build());

    Set<build.bazel.remote.execution.v2.Digest> remoteUploaded = new HashSet<>();
    MerkleTreeUploader remoteMockUploader =
        new MerkleTreeUploader() {
          @Override
          public void ensureInputsPresent(
              RemoteActionExecutionContext context, MerkleTree.Uploadable tree, boolean force) {
            remoteUploaded.addAll(tree.blobs().keySet());
          }

          @Override
          public ListenableFuture<Void> uploadBlob(
              RemoteActionExecutionContext context,
              build.bazel.remote.execution.v2.Digest digest,
              byte[] data,
              boolean force) {
            remoteUploaded.add(digest);
            return Futures.immediateVoidFuture();
          }

          @Override
          public ListenableFuture<Void> uploadFile(
              RemoteActionExecutionContext context,
              build.bazel.remote.execution.v2.Digest digest,
              Path path,
              PathFragment execPath,
              boolean force) {
            remoteUploaded.add(digest);
            return Futures.immediateVoidFuture();
          }

          @Override
          public ListenableFuture<Void> uploadDeterministicWriter(
              RemoteActionExecutionContext context,
              build.bazel.remote.execution.v2.Digest digest,
              DeterministicWriter writer,
              boolean force) {
            remoteUploaded.add(digest);
            return Futures.immediateVoidFuture();
          }
        };
    DigestUtil digestUtil =
        new DigestUtil(
            SyscallCache.NO_CACHE,
            commandEnvironment.getRuntime().getFileSystem().getDigestFunction());
    MerkleTreeComputer remoteComputer =
        new MerkleTreeComputer(
            digestUtil,
            remoteMockUploader,
            new Object(),
            commandEnvironment.getBuildRequestId(),
            commandEnvironment.getCommandId().toString(),
            commandEnvironment.getWorkspaceName());

    Spawn spawn =
        new SpawnBuilder("/bin/sh", "-c", "cat bazel-out/pkg/shared_tree/item_0.txt > f3_out.txt")
            .withInputs(sharedTree)
            .withOutputs("f3_out.txt")
            .build();
    FileOutErr outErr = new FileOutErr(testRoot.getChild("f3.out"), testRoot.getChild("f3.err"));
    SpawnExecutionContext ctx = createContext(spawn, outErr, Duration.ofSeconds(30), meta);

    MerkleTree remoteTree =
        remoteComputer.buildForSpawn(
            spawn, ImmutableSet.of(), /* scrubber= */ null, ctx, BlobPolicy.KEEP);
    assertThat(remoteUploaded).isNotEmpty();

    SpawnResult res = runner.exec(spawn, ctx);
    assertThat(res.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(FileSystemUtils.readContent(execRoot.getRelative("f3_out.txt"), UTF_8))
        .isEqualTo("tree_item_0");
    assertThat(runner.getUploader().getUploadedFileCount()).isEqualTo(3);
    assertThat(runner.getUploader().toCasPath(remoteTree.digest()).exists()).isTrue();
  }

  @Test
  public void exec_dynamicExecutionRace_isolatesOutputsAndCancelsLosingProcess() throws Exception {
    CountDownLatch subprocessStarted = new CountDownLatch(1);
    fakeDaemon.setExecutionStartedLatch(subprocessStarted);

    Path raceOutput = execRoot.getRelative("out/race.txt");
    raceOutput.getParentDirectory().createDirectoryAndParents();
    Spawn slowLocalSpawn =
        new SpawnBuilder(
                "/bin/sh",
                "-c",
                "echo local-losing-output > out/race.txt; echo local-stdout; sleep 30")
            .withOutputs("out/race.txt")
            .build();
    FileOutErr localOutErr =
        new FileOutErr(testRoot.getChild("race_local_out"), testRoot.getChild("race_local_err"));
    SpawnExecutionContext slowCtx =
        createContext(
            slowLocalSpawn, localOutErr, Duration.ofSeconds(60), new FakeActionInputFileCache());

    AtomicReference<Throwable> localException = new AtomicReference<>();
    Thread localThread =
        new Thread(
            () -> {
              try {
                var unused = runner.exec(slowLocalSpawn, slowCtx);
              } catch (Throwable t) {
                localException.set(t);
              }
            });
    localThread.start();

    assertThat(subprocessStarted.await(10, SECONDS)).isTrue();
    Process spawnedProcess = fakeDaemon.getLastSpawnedProcess();
    assertThat(spawnedProcess).isNotNull();

    FileSystemUtils.writeContent(raceOutput, UTF_8, "remote-winning-output\n");
    localThread.interrupt();
    localThread.join(10_000);

    assertThat(localThread.isAlive()).isFalse();
    assertThat(localException.get()).isInstanceOf(InterruptedException.class);
    assertThat(spawnedProcess.waitFor(5, SECONDS)).isTrue();
    assertThat(spawnedProcess.isAlive()).isFalse();
    assertThat(fakeDaemon.getCancelledExecutions()).isEqualTo(1);

    assertThat(FileSystemUtils.readContent(raceOutput, UTF_8)).isEqualTo("remote-winning-output\n");
    assertThat(localOutErr.hasRecordedOutput()).isFalse();

    fakeDaemon.setExecutionStartedLatch(null);

    Spawn fastLosingSpawn =
        new SpawnBuilder(
                "/bin/sh", "-c", "echo local-finished-too-late > out/race.txt; echo late-stdout")
            .withOutputs("out/race.txt")
            .build();
    FileOutErr lateOutErr =
        new FileOutErr(testRoot.getChild("late_out"), testRoot.getChild("late_err"));
    SpawnExecutionContext losingLockCtx =
        createContextWithLockHook(
            fastLosingSpawn,
            lateOutErr,
            Duration.ofSeconds(30),
            new FakeActionInputFileCache(),
            () -> {
              throw new InterruptedException("Remote branch already locked outputs");
            });

    assertThrows(InterruptedException.class, () -> runner.exec(fastLosingSpawn, losingLockCtx));
    assertThat(FileSystemUtils.readContent(raceOutput, UTF_8)).isEqualTo("remote-winning-output\n");
    assertThat(lateOutErr.hasRecordedOutput()).isFalse();

    AtomicBoolean competingBranchCancelled = new AtomicBoolean(false);
    Spawn winningLocalSpawn =
        new SpawnBuilder(
                "/bin/sh", "-c", "echo local-winning-output > out/race.txt; echo winning-stdout")
            .withOutputs("out/race.txt")
            .build();
    FileOutErr winningOutErr =
        new FileOutErr(testRoot.getChild("win_out"), testRoot.getChild("win_err"));
    SpawnExecutionContext winningLockCtx =
        createContextWithLockHook(
            winningLocalSpawn,
            winningOutErr,
            Duration.ofSeconds(30),
            new FakeActionInputFileCache(),
            () -> competingBranchCancelled.set(true));

    SpawnResult winResult = runner.exec(winningLocalSpawn, winningLockCtx);
    assertThat(winResult.status()).isEqualTo(SpawnResult.Status.SUCCESS);
    assertThat(competingBranchCancelled.get()).isTrue();
    assertThat(FileSystemUtils.readContent(raceOutput, UTF_8)).isEqualTo("local-winning-output\n");
    assertThat(winningOutErr.outAsLatin1()).isEqualTo("winning-stdout\n");
  }

  @Test
  public void beforeCommand_managedDaemonLifecycle_startsReusesRestartsAndValidatesOptions()
      throws Exception {
    String javaBin = System.getProperty("java.home") + "/bin/java";
    String classPath = System.getProperty("java.class.path");
    Path daemonScript = testRoot.getChild("mock-sandboxd.sh");
    FileSystemUtils.writeContent(
        daemonScript,
        UTF_8,
        "#!/bin/sh\n"
            + "exec \""
            + javaBin
            + "\" -cp \""
            + classPath
            + "\" '"
            + FakeReapiDaemon.class.getName()
            + "' \"$@\"\n");
    daemonScript.setExecutable(true);

    Path argsRecordFile = testRoot.getChild("daemon_args.txt");

    SandboxModule sandboxModule = new SandboxModule();
    try {
      runtimeWrapper.addOptions(
          "--experimental_sandbox_backend=fskit=" + daemonScript.getPathString(),
          "--experimental_sandbox_backend_opt=fskit=--record_args_file="
              + argsRecordFile.getPathString(),
          "--experimental_sandbox_backend_opt=fskit=--custom_opt=alpha",
          "--experimental_sandbox_backend=missing=/nonexistent/path/to/sandboxd");
      CommandEnvironment env1 = runtimeWrapper.newCommand();
      env1.getLocalResourceManager()
          .setAvailableResources(LocalHostCapacity.getLocalHostCapacity());
      env1.getExecRoot().createDirectoryAndParents();

      sandboxModule.beforeCommand(env1);
      SpawnStrategyRegistry.Builder registryBuilder1 = SpawnStrategyRegistry.builder();
      sandboxModule.registerSpawnStrategies(registryBuilder1, env1);
      SpawnStrategyRegistry registry1 = registryBuilder1.build();

      Spawn testSpawn =
          new SpawnBuilder("/bin/sh", "-c", "echo managed-daemon-ok > managed_out.txt")
              .withOutputs("managed_out.txt")
              .build();

      List<? extends SpawnStrategy> missingStrategies =
          registry1.getStrategies(
              testSpawn.getResourceOwner(), testSpawn.getMnemonic(), event -> {});
      assertThat(missingStrategies).isNotNull();
      SandboxBackendClient missingClient = sandboxModule.getSandboxBackendClients().get("missing");
      assertThat(missingClient).isNotNull();
      assertThat(missingClient.isAvailable()).isFalse();
      SandboxBackendSpawnRunner missingRunner =
          new SandboxBackendSpawnRunner(
              env1,
              env1.getOutputBase().getRelative("sandbox"),
              "missing",
              missingClient,
              /* ownsClient= */ false);
      assertThat(missingRunner.getName()).isEqualTo("sandbox_backend:missing");
      assertThat(missingRunner.canExec(testSpawn)).isFalse();

      SandboxBackendClient fskitClient1 = sandboxModule.getSandboxBackendClients().get("fskit");
      assertThat(fskitClient1).isNotNull();
      assertThat(fskitClient1.isAvailable()).isTrue();
      assertThat(fskitClient1.getDaemonStartCount()).isEqualTo(0);

      SandboxBackendSpawnRunner fskitRunner1 =
          new SandboxBackendSpawnRunner(
              env1,
              env1.getOutputBase().getRelative("sandbox"),
              "fskit",
              fskitClient1,
              /* ownsClient= */ false);
      assertThat(fskitRunner1.getName()).isEqualTo("sandbox_backend:fskit");
      assertThat(fskitRunner1.canExec(testSpawn)).isTrue();

      FileOutErr outErr1 = new FileOutErr(testRoot.getChild("m1_out"), testRoot.getChild("m1_err"));
      SpawnResult res1 =
          fskitRunner1.exec(
              testSpawn,
              createContext(
                  testSpawn, outErr1, Duration.ofSeconds(30), new FakeActionInputFileCache()));
      assertThat(res1.status()).isEqualTo(SpawnResult.Status.SUCCESS);
      assertThat(
              FileSystemUtils.readContent(env1.getExecRoot().getRelative("managed_out.txt"), UTF_8))
          .isEqualTo("managed-daemon-ok\n");
      assertThat(fskitClient1.getDaemonStartCount()).isEqualTo(1);

      List<String> recordedArgs1 = Files.readAllLines(argsRecordFile.getPathFile().toPath(), UTF_8);
      assertThat(recordedArgs1.get(0)).isEqualTo("serve");
      assertThat(recordedArgs1.get(1)).startsWith("--socket=/tmp/bazel-reapi-fskit-");
      assertThat(recordedArgs1.get(2)).startsWith("--disk_cache=");
      assertThat(recordedArgs1.get(3)).startsWith("--cas_root=");
      assertThat(recordedArgs1).contains("--custom_opt=alpha");
      sandboxModule.afterCommand();

      // Same backend binary, disk_cache, and options -> client and daemon reused.
      runtimeWrapper.resetOptions();
      runtimeWrapper.addOptions(
          "--experimental_sandbox_backend=fskit=" + daemonScript.getPathString(),
          "--experimental_sandbox_backend_opt=fskit=--record_args_file="
              + argsRecordFile.getPathString(),
          "--experimental_sandbox_backend_opt=fskit=--custom_opt=alpha");
      CommandEnvironment env2 = runtimeWrapper.newCommand();
      sandboxModule.beforeCommand(env2);
      sandboxModule.registerSpawnStrategies(SpawnStrategyRegistry.builder(), env2);
      SandboxBackendClient fskitClient2 = sandboxModule.getSandboxBackendClients().get("fskit");
      assertThat(fskitClient2).isSameInstanceAs(fskitClient1);
      assertThat(fskitClient2.getDaemonStartCount()).isEqualTo(1);
      sandboxModule.afterCommand();

      // Changed --experimental_sandbox_backend_opt -> daemon restarted with new options.
      runtimeWrapper.resetOptions();
      runtimeWrapper.addOptions(
          "--experimental_sandbox_backend=fskit=" + daemonScript.getPathString(),
          "--experimental_sandbox_backend_opt=fskit=--record_args_file="
              + argsRecordFile.getPathString(),
          "--experimental_sandbox_backend_opt=fskit=--custom_opt=beta");
      CommandEnvironment env3 = runtimeWrapper.newCommand();
      env3.getLocalResourceManager()
          .setAvailableResources(LocalHostCapacity.getLocalHostCapacity());
      sandboxModule.beforeCommand(env3);
      sandboxModule.registerSpawnStrategies(SpawnStrategyRegistry.builder(), env3);
      SandboxBackendClient fskitClient3 = sandboxModule.getSandboxBackendClients().get("fskit");
      assertThat(fskitClient3).isNotSameInstanceAs(fskitClient1);

      SandboxBackendSpawnRunner fskitRunner3 =
          new SandboxBackendSpawnRunner(
              env3,
              env3.getOutputBase().getRelative("sandbox"),
              "fskit",
              fskitClient3,
              /* ownsClient= */ false);
      FileOutErr outErr3 = new FileOutErr(testRoot.getChild("m3_out"), testRoot.getChild("m3_err"));
      SpawnResult res3 =
          fskitRunner3.exec(
              testSpawn,
              createContext(
                  testSpawn, outErr3, Duration.ofSeconds(30), new FakeActionInputFileCache()));
      assertThat(res3.status()).isEqualTo(SpawnResult.Status.SUCCESS);
      assertThat(fskitClient3.getDaemonStartCount()).isEqualTo(1);
      List<String> recordedArgs3 = Files.readAllLines(argsRecordFile.getPathFile().toPath(), UTF_8);
      assertThat(recordedArgs3).contains("--custom_opt=beta");
      sandboxModule.afterCommand();

      // Option targeting an unregistered backend fails fast.
      runtimeWrapper.resetOptions();
      runtimeWrapper.addOptions("--experimental_sandbox_backend_opt=unknown_backend=--flag");
      CommandEnvironment env4 = runtimeWrapper.newCommand();
      sandboxModule.beforeCommand(env4);
      assertThrows(
          AbruptExitException.class,
          () -> sandboxModule.registerSpawnStrategies(SpawnStrategyRegistry.builder(), env4));
    } finally {
      sandboxModule.blazeShutdown();
    }
  }

  @Test
  public void exec_withSandboxOptionsAndResources_populatesPlatformPropertiesAndRequestMetadata()
      throws Exception {
    Path blockedTarget = testRoot.getChild("blocked_dir");
    blockedTarget.createDirectoryAndParents();

    runtimeWrapper.addOptions(
        "--sandbox_writable_path=/tmp/custom_writable",
        "--sandbox_tmpfs_path=/tmp/custom_tmpfs",
        "--sandbox_block_path=" + blockedTarget.getPathString(),
        "--sandbox_default_allow_network=false");
    CommandEnvironment policyEnv = runtimeWrapper.newCommand();
    policyEnv
        .getLocalResourceManager()
        .setAvailableResources(ResourceSet.createWithRamCpu(/* memoryMb= */ 1024, /* cpu= */ 4));
    policyEnv.getExecRoot().createDirectoryAndParents();

    try (SandboxBackendSpawnRunner policyRunner =
        new SandboxBackendSpawnRunner(policyEnv, fakeDaemon.getTarget(), diskCacheRoot)) {
      Spawn spawn =
          new SpawnBuilder("/bin/sh", "-c", "echo policy-ok > out/policy.txt")
              .withMnemonic("CppCompile")
              .withProgressMessage("Compiling foo.cc")
              .withOwnerLabel("//pkg:foo_lib")
              .withExecutionInfo("requires-network", "")
              .withLocalResources(ResourceSet.createWithRamCpu(/* memoryMb= */ 512, /* cpu= */ 2.5))
              .withOutputs("out/policy.txt")
              .build();

      FileOutErr outErr =
          new FileOutErr(testRoot.getChild("pol_out"), testRoot.getChild("pol_err"));
      SpawnResult result =
          policyRunner.exec(
              spawn,
              createContext(spawn, outErr, Duration.ofSeconds(30), new FakeActionInputFileCache()));
      assertThat(result.status()).isEqualTo(SpawnResult.Status.SUCCESS);

      Action recordedAction = fakeDaemon.getLastAction();
      assertThat(recordedAction).isNotNull();
      ListMultimap<String, String> props = ArrayListMultimap.create();
      for (Platform.Property prop : recordedAction.getPlatform().getPropertiesList()) {
        props.put(prop.getName(), prop.getValue());
      }

      assertThat(props.get(SandboxBackendSpawnRunner.PLATFORM_WRITABLE_PATH))
          .contains("/tmp/custom_writable");
      assertThat(props.get(SandboxBackendSpawnRunner.PLATFORM_TMPFS_PATH))
          .contains("/tmp/custom_tmpfs");
      assertThat(props.get(SandboxBackendSpawnRunner.PLATFORM_INACCESSIBLE_PATH))
          .contains(blockedTarget.resolveSymbolicLinks().getPathString());
      assertThat(props.get(SandboxBackendSpawnRunner.PLATFORM_ALLOW_NETWORK))
          .containsExactly("true");
      assertThat(props.get(SandboxBackendSpawnRunner.PLATFORM_DISK_CACHE))
          .containsExactly(diskCacheRoot.getPathString());
      assertThat(props.get(SandboxBackendSpawnRunner.PLATFORM_CAS_ROOT))
          .containsExactly(diskCacheRoot.getRelative("cas").getPathString());
      assertThat(props.get(SandboxBackendSpawnRunner.PLATFORM_SANDBOX_BASE)).isNotEmpty();
      assertThat(props.get(SandboxBackendSpawnRunner.PLATFORM_RESOURCES_CPU))
          .containsExactly("2.5");
      assertThat(props.get(SandboxBackendSpawnRunner.PLATFORM_RESOURCES_MEMORY_MB))
          .containsExactly("512.0");

      RequestMetadata recordedMetadata = fakeDaemon.getLastRequestMetadata();
      assertThat(recordedMetadata).isNotNull();
      assertThat(recordedMetadata.getCorrelatedInvocationsId())
          .isEqualTo(policyEnv.getBuildRequestId());
      assertThat(recordedMetadata.getToolInvocationId())
          .isEqualTo(policyEnv.getCommandId().toString());
      assertThat(recordedMetadata.getActionId()).isNotEmpty();
      assertThat(recordedMetadata.getActionMnemonic()).isEqualTo("CppCompile");
    }
  }

  @FunctionalInterface
  private interface LockOutputHook {
    void onLockOutputFiles() throws InterruptedException;
  }

  private SpawnExecutionContext createContext(
      Spawn spawn,
      FileOutErr fileOutErr,
      Duration timeout,
      InputMetadataProvider metadataProvider) {
    return createContextWithLockHook(spawn, fileOutErr, timeout, metadataProvider, () -> {});
  }

  private SpawnExecutionContext createContextWithLockHook(
      Spawn spawn,
      FileOutErr fileOutErr,
      Duration timeout,
      InputMetadataProvider metadataProvider,
      LockOutputHook lockHook) {
    SpawnExecutionContextForTesting delegate =
        new SpawnExecutionContextForTesting(spawn, fileOutErr, timeout);
    ArtifactPathResolver pathResolver = ArtifactPathResolver.forExecRoot(execRoot);
    return new SpawnExecutionContext() {
      @Override
      public int getId() {
        return delegate.getId();
      }

      @Override
      public void setDigest(Digest digest) {
        delegate.setDigest(digest);
      }

      @Override
      @Nullable
      public Digest getDigest() {
        return delegate.getDigest();
      }

      @Override
      public ListenableFuture<Void> prefetchInputs() {
        return delegate.prefetchInputs();
      }

      @Override
      public void lockOutputFiles(int exitCode, String errorMessage, FileOutErr outErr)
          throws InterruptedException {
        lockHook.onLockOutputFiles();
        delegate.lockOutputFiles(exitCode, errorMessage, outErr);
      }

      @Override
      public boolean speculating() {
        return delegate.speculating();
      }

      @Override
      public InputMetadataProvider getInputMetadataProvider() {
        return metadataProvider;
      }

      @Override
      public ArtifactPathResolver getPathResolver() {
        return pathResolver;
      }

      @Override
      public Duration getTimeout() {
        return delegate.getTimeout();
      }

      @Override
      public FileOutErr getFileOutErr() {
        return delegate.getFileOutErr();
      }

      @Override
      public SortedMap<PathFragment, ActionInput> getInputMapping(boolean willAccessRepeatedly) {
        return delegate.getInputMapping(willAccessRepeatedly);
      }

      @Override
      public void report(ProgressStatus progress) {
        delegate.report(progress);
      }

      @Override
      public <T extends ActionContext> T getContext(Class<T> identifyingType) {
        return delegate.getContext(identifyingType);
      }

      @Override
      public boolean isRewindingEnabled() {
        return delegate.isRewindingEnabled();
      }

      @Override
      public void checkForLostInputs() {
        delegate.checkForLostInputs();
      }

      @Nullable
      @Override
      public FileSystem getActionFileSystem() {
        return delegate.getActionFileSystem();
      }

      @Override
      public ImmutableMap<String, String> getClientEnv() {
        return delegate.getClientEnv();
      }
    };
  }

  /**
   * Minimal test REAPI fake implementing {@code Capabilities} and {@code Execution} backed by a
   * local {@code --disk_cache/cas} directory (in-process for unit tests, or over a Unix domain
   * socket via {@link #main} for managed-daemon lifecycle tests).
   */
  public static final class FakeReapiDaemon implements AutoCloseable {

    private static final build.bazel.remote.execution.v2.Digest EMPTY_DIGEST =
        build.bazel.remote.execution.v2.Digest.newBuilder()
            .setHash("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .setSizeBytes(0)
            .build();
    private static final AtomicLong TMP_SEQ = new AtomicLong();

    private static final class MissingDigestsException extends IOException {
      private final ImmutableList<build.bazel.remote.execution.v2.Digest> missing;

      MissingDigestsException(List<build.bazel.remote.execution.v2.Digest> missing) {
        super("Missing CAS blobs: " + missing);
        this.missing = ImmutableList.copyOf(missing);
      }
    }

    private final java.nio.file.Path sandboxBaseDir;
    private final java.nio.file.Path diskCacheRoot;
    private volatile java.nio.file.Path casRoot;
    private final String target;
    private final ExecutorService rpcExecutor;
    @Nullable private final EventLoopGroup bossGroup;
    @Nullable private final EventLoopGroup workerGroup;
    private final Server server;
    private final AtomicInteger sandboxCounter = new AtomicInteger();
    private final AtomicInteger executedActions = new AtomicInteger();
    private final AtomicInteger missingPreconditionCount = new AtomicInteger();
    private final AtomicInteger cancelledExecutions = new AtomicInteger();
    private final AtomicReference<Action> lastAction = new AtomicReference<>();
    private final AtomicReference<RequestMetadata> lastRequestMetadata = new AtomicReference<>();
    private final AtomicReference<Process> lastSpawnedProcess = new AtomicReference<>();
    @Nullable private volatile CountDownLatch executionStartedLatch;

    FakeReapiDaemon(
        java.nio.file.Path sandboxBaseDir, java.nio.file.Path diskCacheRoot, String serverName)
        throws IOException {
      this.sandboxBaseDir = sandboxBaseDir;
      this.diskCacheRoot = diskCacheRoot;
      this.casRoot = diskCacheRoot.resolve("cas");
      this.target = "inprocess:" + serverName;
      this.rpcExecutor =
          Executors.newThreadPerTaskExecutor(
              Thread.ofVirtual().name("fake-reapi-", 0).factory());
      this.bossGroup = null;
      this.workerGroup = null;
      Files.createDirectories(sandboxBaseDir);
      Files.createDirectories(casRoot);
      Files.createDirectories(diskCacheRoot.resolve("tmp"));
      ServerInterceptor interceptor = metadataInterceptor();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .executor(rpcExecutor)
              .addService(ServerInterceptors.intercept(capabilitiesService(), interceptor))
              .addService(ServerInterceptors.intercept(executionService(), interceptor))
              .build()
              .start();
    }

    private FakeReapiDaemon(
        java.nio.file.Path sandboxBaseDir,
        java.nio.file.Path diskCacheRoot,
        java.nio.file.Path casRoot,
        java.nio.file.Path unixSocketPath)
        throws IOException {
      this.sandboxBaseDir = sandboxBaseDir;
      this.diskCacheRoot = diskCacheRoot;
      this.casRoot = casRoot;
      this.target = "unix:" + unixSocketPath;
      this.rpcExecutor =
          Executors.newThreadPerTaskExecutor(
              Thread.ofVirtual().name("fake-reapi-uds-", 0).factory());
      Files.createDirectories(sandboxBaseDir);
      Files.createDirectories(casRoot);
      Files.createDirectories(diskCacheRoot.resolve("tmp"));
      if (unixSocketPath.getParent() != null) {
        Files.createDirectories(unixSocketPath.getParent());
      }
      Files.deleteIfExists(unixSocketPath);
      this.bossGroup = new NioEventLoopGroup(1);
      this.workerGroup = new NioEventLoopGroup();
      ServerInterceptor interceptor = metadataInterceptor();
      this.server =
          NettyServerBuilder.forAddress(UnixDomainSocketAddress.of(unixSocketPath))
              .bossEventLoopGroup(bossGroup)
              .workerEventLoopGroup(workerGroup)
              .channelType(NioServerDomainSocketChannel.class)
              .executor(rpcExecutor)
              .addService(ServerInterceptors.intercept(capabilitiesService(), interceptor))
              .addService(ServerInterceptors.intercept(executionService(), interceptor))
              .build()
              .start();
    }

    String getTarget() {
      return target;
    }

    int getExecutedActions() {
      return executedActions.get();
    }

    int getMissingPreconditionCount() {
      return missingPreconditionCount.get();
    }

    int getCancelledExecutions() {
      return cancelledExecutions.get();
    }

    @Nullable
    Action getLastAction() {
      return lastAction.get();
    }

    @Nullable
    RequestMetadata getLastRequestMetadata() {
      return lastRequestMetadata.get();
    }

    @Nullable
    Process getLastSpawnedProcess() {
      return lastSpawnedProcess.get();
    }

    void setExecutionStartedLatch(@Nullable CountDownLatch latch) {
      this.executionStartedLatch = latch;
    }

    private ServerInterceptor metadataInterceptor() {
      return new ServerInterceptor() {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
          RequestMetadata meta = headers.get(TracingMetadataUtils.METADATA_KEY);
          if (meta != null) {
            lastRequestMetadata.set(meta);
          }
          return next.startCall(call, headers);
        }
      };
    }

    private CapabilitiesImplBase capabilitiesService() {
      return new CapabilitiesImplBase() {
        @Override
        public void getCapabilities(
            GetCapabilitiesRequest request, StreamObserver<ServerCapabilities> responseObserver) {
          responseObserver.onNext(
              ServerCapabilities.newBuilder()
                  .setCacheCapabilities(
                      CacheCapabilities.newBuilder()
                          .addDigestFunctions(DigestFunction.Value.SHA256)
                          .build())
                  .setExecutionCapabilities(
                      ExecutionCapabilities.newBuilder()
                          .setDigestFunction(DigestFunction.Value.SHA256)
                          .addDigestFunctions(DigestFunction.Value.SHA256)
                          .setExecEnabled(true)
                          .build())
                  .build());
          responseObserver.onCompleted();
        }
      };
    }

    private ExecutionImplBase executionService() {
      return new ExecutionImplBase() {
        @Override
        public void execute(ExecuteRequest request, StreamObserver<Operation> responseObserver) {
          int actionNum = executedActions.incrementAndGet();
          AtomicReference<Process> activeProcess = new AtomicReference<>();
          AtomicReference<Thread> execThreadRef = new AtomicReference<>();
          AtomicBoolean cancelled = new AtomicBoolean(false);
          if (responseObserver instanceof ServerCallStreamObserver<Operation> serverObserver) {
            serverObserver.setOnCancelHandler(
                () -> {
                  if (cancelled.compareAndSet(false, true)) {
                    cancelledExecutions.incrementAndGet();
                  }
                  Process proc = activeProcess.get();
                  if (proc != null) {
                    proc.descendants().forEach(ProcessHandle::destroyForcibly);
                    proc.destroyForcibly();
                  }
                  Thread execThread = execThreadRef.get();
                  if (execThread != null) {
                    execThread.interrupt();
                  }
                });
          }

          rpcExecutor.execute(
              () -> {
                execThreadRef.set(Thread.currentThread());
                try {
                  ExecuteResponse resp =
                      runAction(request.getActionDigest(), activeProcess, cancelled);
                  if (!cancelled.get()) {
                    responseObserver.onNext(
                        Operation.newBuilder()
                            .setName("operations/fake-" + actionNum)
                            .setDone(true)
                            .setResponse(Any.pack(resp))
                            .build());
                    responseObserver.onCompleted();
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } catch (IOException | RuntimeException e) {
                  if (!cancelled.get()) {
                    ExecuteResponse errResp =
                        ExecuteResponse.newBuilder()
                            .setStatus(
                                Status.newBuilder()
                                    .setCode(Code.INTERNAL.getNumber())
                                    .setMessage(String.valueOf(e.getMessage()))
                                    .build())
                            .build();
                    responseObserver.onNext(
                        Operation.newBuilder()
                            .setName("operations/fake-" + actionNum)
                            .setDone(true)
                            .setResponse(Any.pack(errResp))
                            .build());
                    responseObserver.onCompleted();
                  }
                } finally {
                  execThreadRef.set(null);
                }
              });
        }
      };
    }

    private ExecuteResponse runAction(
        build.bazel.remote.execution.v2.Digest actionDigest,
        AtomicReference<Process> activeProcess,
        AtomicBoolean cancelled)
        throws IOException, InterruptedException {
      byte[] actionBytes = readCasBlob(actionDigest);
      if (actionBytes == null) {
        return missingResponse(ImmutableList.of(actionDigest));
      }
      Action action = Action.parseFrom(actionBytes, ExtensionRegistryLite.getEmptyRegistry());
      lastAction.set(action);

      byte[] commandBytes = readCasBlob(action.getCommandDigest());
      if (commandBytes == null) {
        return missingResponse(ImmutableList.of(action.getCommandDigest()));
      }
      Command command = Command.parseFrom(commandBytes, ExtensionRegistryLite.getEmptyRegistry());

      java.nio.file.Path workDir = sandboxBaseDir.resolve("work-" + sandboxCounter.incrementAndGet());
      Files.createDirectories(workDir);
      try {
        try {
          stageDirectory(action.getInputRootDigest(), workDir);
        } catch (MissingDigestsException e) {
          return missingResponse(e.missing);
        }

        for (String outPath : command.getOutputPathsList()) {
          java.nio.file.Path out = workDir.resolve(outPath);
          if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
          }
        }
        java.nio.file.Path cwd =
            command.getWorkingDirectory().isEmpty()
                ? workDir
                : workDir.resolve(command.getWorkingDirectory());
        Files.createDirectories(cwd);

        java.nio.file.Path stdoutFile = sandboxBaseDir.resolve(workDir.getFileName() + ".out");
        java.nio.file.Path stderrFile = sandboxBaseDir.resolve(workDir.getFileName() + ".err");
        ProcessBuilder pb = new ProcessBuilder(command.getArgumentsList());
        pb.directory(cwd.toFile());
        pb.redirectOutput(stdoutFile.toFile());
        pb.redirectError(stderrFile.toFile());
        pb.environment().clear();
        for (Command.EnvironmentVariable kv : command.getEnvironmentVariablesList()) {
          pb.environment().put(kv.getName(), kv.getValue());
        }

        Process proc = pb.start();
        proc.getOutputStream().close();
        activeProcess.set(proc);
        lastSpawnedProcess.set(proc);
        if (cancelled.get()) {
          proc.destroyForcibly();
        }
        CountDownLatch latch = executionStartedLatch;
        if (latch != null) {
          latch.countDown();
        }

        boolean timedOut = false;
        ByteString stdoutBytes;
        ByteString stderrBytes;
        try {
          if (action.hasTimeout() && Durations.toMillis(action.getTimeout()) > 0) {
            if (!proc.waitFor(Duration.ofMillis(Durations.toMillis(action.getTimeout())))) {
              timedOut = true;
              proc.destroyForcibly();
              var unused = proc.waitFor(Duration.ofSeconds(5));
            }
          } else {
            proc.waitFor();
          }
          stdoutBytes =
              Files.exists(stdoutFile)
                  ? ByteString.copyFrom(Files.readAllBytes(stdoutFile))
                  : ByteString.EMPTY;
          stderrBytes =
              Files.exists(stderrFile)
                  ? ByteString.copyFrom(Files.readAllBytes(stderrFile))
                  : ByteString.EMPTY;
        } finally {
          activeProcess.set(null);
          Files.deleteIfExists(stdoutFile);
          Files.deleteIfExists(stderrFile);
        }

        if (cancelled.get()) {
          return ExecuteResponse.getDefaultInstance();
        }

        ActionResult.Builder result =
            ActionResult.newBuilder()
                .setExitCode(timedOut ? 142 : proc.exitValue())
                .setStdoutRaw(stdoutBytes)
                .setStderrRaw(stderrBytes);

        for (String outPath : command.getOutputPathsList()) {
          java.nio.file.Path staged = workDir.resolve(outPath);
          if (!Files.exists(staged, LinkOption.NOFOLLOW_LINKS)) {
            continue;
          }
          if (Files.isSymbolicLink(staged)) {
            result
                .addOutputSymlinksBuilder()
                .setPath(outPath)
                .setTarget(Files.readSymbolicLink(staged).toString());
          } else if (Files.isDirectory(staged, LinkOption.NOFOLLOW_LINKS)) {
            List<Directory> children = new ArrayList<>();
            Directory root = buildDirectoryAndStoreFiles(staged, children);
            Tree tree = Tree.newBuilder().setRoot(root).addAllChildren(children).build();
            result
                .addOutputDirectoriesBuilder()
                .setPath(outPath)
                .setTreeDigest(writeBytesToCas(tree.toByteArray()));
          } else {
            boolean isExec = Files.isExecutable(staged);
            result
                .addOutputFilesBuilder()
                .setPath(outPath)
                .setDigest(writeBytesToCas(Files.readAllBytes(staged)))
                .setIsExecutable(isExec);
          }
        }

        Status status =
            timedOut
                ? Status.newBuilder().setCode(Code.DEADLINE_EXCEEDED.getNumber()).build()
                : Status.newBuilder().setCode(Code.OK.getNumber()).build();
        return ExecuteResponse.newBuilder().setStatus(status).setResult(result.build()).build();
      } finally {
        deleteRecursively(workDir);
      }
    }

    private Directory buildDirectoryAndStoreFiles(
        java.nio.file.Path dir, List<Directory> outChildren) throws IOException {
      Directory.Builder b = Directory.newBuilder();
      List<java.nio.file.Path> entries;
      try (var stream = Files.list(dir)) {
        entries = stream.sorted(Comparator.comparing(p -> p.getFileName().toString())).toList();
      }
      for (java.nio.file.Path entry : entries) {
        String name = entry.getFileName().toString();
        if (Files.isSymbolicLink(entry)) {
          b.addSymlinksBuilder().setName(name).setTarget(Files.readSymbolicLink(entry).toString());
        } else if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
          Directory child = buildDirectoryAndStoreFiles(entry, outChildren);
          outChildren.add(child);
          b.addDirectoriesBuilder()
              .setName(name)
              .setDigest(writeBytesToCas(child.toByteArray()));
        } else {
          b.addFilesBuilder()
              .setName(name)
              .setDigest(writeBytesToCas(Files.readAllBytes(entry)))
              .setIsExecutable(Files.isExecutable(entry));
        }
      }
      return b.build();
    }

    private void stageDirectory(
        build.bazel.remote.execution.v2.Digest dirDigest, java.nio.file.Path targetDir)
        throws IOException {
      Files.createDirectories(targetDir);
      if (dirDigest.getSizeBytes() == 0) {
        return;
      }
      byte[] dirBytes = readCasBlob(dirDigest);
      if (dirBytes == null) {
        throw new MissingDigestsException(ImmutableList.of(dirDigest));
      }
      Directory directory = Directory.parseFrom(dirBytes, ExtensionRegistryLite.getEmptyRegistry());
      List<build.bazel.remote.execution.v2.Digest> missing = null;
      for (FileNode fileNode : directory.getFilesList()) {
        java.nio.file.Path dst = targetDir.resolve(fileNode.getName());
        build.bazel.remote.execution.v2.Digest d = fileNode.getDigest();
        if (d.getSizeBytes() == 0) {
          Files.write(dst, new byte[0]);
        } else {
          java.nio.file.Path casBlob = toCasPath(d);
          if (!Files.exists(casBlob, LinkOption.NOFOLLOW_LINKS)) {
            if (missing == null) {
              missing = new ArrayList<>();
            }
            missing.add(d);
            continue;
          }
          Files.copy(casBlob, dst, StandardCopyOption.REPLACE_EXISTING);
        }
        if (fileNode.getIsExecutable()) {
          Set<PosixFilePermission> perms = EnumSet.copyOf(Files.getPosixFilePermissions(dst));
          perms.add(PosixFilePermission.OWNER_EXECUTE);
          Files.setPosixFilePermissions(dst, perms);
        }
      }
      if (missing != null) {
        throw new MissingDigestsException(missing);
      }
      for (SymlinkNode sym : directory.getSymlinksList()) {
        Files.createSymbolicLink(
            targetDir.resolve(sym.getName()), java.nio.file.Path.of(sym.getTarget()));
      }
      for (DirectoryNode sub : directory.getDirectoriesList()) {
        stageDirectory(sub.getDigest(), targetDir.resolve(sub.getName()));
      }
    }

    private java.nio.file.Path toCasPath(build.bazel.remote.execution.v2.Digest digest) {
      String h = digest.getHash();
      return casRoot.resolve(h.substring(0, 2)).resolve(h);
    }

    @Nullable
    private byte[] readCasBlob(build.bazel.remote.execution.v2.Digest digest) throws IOException {
      if (digest.getSizeBytes() == 0) {
        return new byte[0];
      }
      java.nio.file.Path p = toCasPath(digest);
      return Files.exists(p, LinkOption.NOFOLLOW_LINKS) ? Files.readAllBytes(p) : null;
    }

    private build.bazel.remote.execution.v2.Digest writeBytesToCas(byte[] data) throws IOException {
      if (data.length == 0) {
        return EMPTY_DIGEST;
      }
      build.bazel.remote.execution.v2.Digest digest;
      try {
        byte[] hash = MessageDigest.getInstance("SHA-256").digest(data);
        digest =
            build.bazel.remote.execution.v2.Digest.newBuilder()
                .setHash(BaseEncoding.base16().lowerCase().encode(hash))
                .setSizeBytes(data.length)
                .build();
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException(e);
      }
      java.nio.file.Path target = toCasPath(digest);
      if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
        return digest;
      }
      java.nio.file.Path tmpDir = diskCacheRoot.resolve("tmp");
      Files.createDirectories(tmpDir);
      java.nio.file.Path temp = tmpDir.resolve("fake-cas-" + TMP_SEQ.incrementAndGet());
      try {
        Files.write(temp, data);
        Files.createDirectories(target.getParent());
        Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
      } finally {
        Files.deleteIfExists(temp);
      }
      return digest;
    }

    private ExecuteResponse missingResponse(
        List<build.bazel.remote.execution.v2.Digest> missingDigests) {
      missingPreconditionCount.incrementAndGet();
      PreconditionFailure.Builder pf = PreconditionFailure.newBuilder();
      for (build.bazel.remote.execution.v2.Digest d : missingDigests) {
        pf.addViolationsBuilder()
            .setType("MISSING")
            .setSubject("blobs/" + d.getHash() + "/" + d.getSizeBytes());
      }
      return ExecuteResponse.newBuilder()
          .setStatus(
              Status.newBuilder()
                  .setCode(Code.FAILED_PRECONDITION.getNumber())
                  .addDetails(Any.pack(pf.build()))
                  .build())
          .build();
    }

    private static void deleteRecursively(java.nio.file.Path root) throws IOException {
      if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
        return;
      }
      Files.walkFileTree(
          root,
          new SimpleFileVisitor<java.nio.file.Path>() {
            @Override
            public FileVisitResult visitFile(
                java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {
              Files.deleteIfExists(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(
                java.nio.file.Path dir, IOException exc) throws IOException {
              Files.deleteIfExists(dir);
              return FileVisitResult.CONTINUE;
            }
          });
    }

    @Override
    public void close() {
      server.shutdownNow();
      rpcExecutor.shutdownNow();
      if (bossGroup != null) {
        var unused = bossGroup.shutdownGracefully(0, 1, SECONDS);
      }
      if (workerGroup != null) {
        var unused = workerGroup.shutdownGracefully(0, 1, SECONDS);
      }
    }

    public static void main(String[] args) throws Exception {
      String socketPathStr = null;
      String diskCachePathStr = null;
      String casRootPathStr = null;
      java.nio.file.Path recordArgsFile = null;
      for (int i = 1; i < args.length; i++) {
        if (args[i].startsWith("--socket=")) {
          socketPathStr = args[i].substring("--socket=".length());
        } else if (args[i].startsWith("--disk_cache=")) {
          diskCachePathStr = args[i].substring("--disk_cache=".length());
        } else if (args[i].startsWith("--cas_root=")) {
          casRootPathStr = args[i].substring("--cas_root=".length());
        } else if (args[i].startsWith("--record_args_file=")) {
          recordArgsFile = java.nio.file.Path.of(args[i].substring("--record_args_file=".length()));
        }
      }
      java.nio.file.Path socketPath = java.nio.file.Path.of(socketPathStr);
      if (recordArgsFile != null) {
        Files.writeString(recordArgsFile, String.join("\n", args) + "\n", UTF_8);
      }
      java.nio.file.Path workBase = socketPath.getParent().resolve("work");
      java.nio.file.Path diskCachePath =
          diskCachePathStr != null ? java.nio.file.Path.of(diskCachePathStr) : workBase.resolve("dc");
      java.nio.file.Path casRootPath =
          casRootPathStr != null ? java.nio.file.Path.of(casRootPathStr) : diskCachePath.resolve("cas");
      try (FakeReapiDaemon daemon =
          new FakeReapiDaemon(workBase, diskCachePath, casRootPath, socketPath)) {
        Runtime.getRuntime().addShutdownHook(new Thread(daemon::close));
        daemon.server.awaitTermination();
      }
    }
  }
}
