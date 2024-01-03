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

package com.google.devtools.build.lib.remote;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.Subscribe;
import com.google.devtools.build.lib.actions.Action;
import com.google.devtools.build.lib.actions.ActionExecutionMetadata;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputMap;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.Artifact.SpecialArtifact;
import com.google.devtools.build.lib.actions.ArtifactPathResolver;
import com.google.devtools.build.lib.actions.FilesetOutputSymlink;
import com.google.devtools.build.lib.actions.InputMetadataProvider;
import com.google.devtools.build.lib.actions.RemoteArtifactChecker;
import com.google.devtools.build.lib.actions.cache.MetadataInjector;
import com.google.devtools.build.lib.actions.cache.OutputMetadataStore;
import com.google.devtools.build.lib.buildtool.buildevent.ExecutionPhaseCompleteEvent;
import com.google.devtools.build.lib.events.EventHandler;
import com.google.devtools.build.lib.remote.RemoteOutputServiceGrpc.RemoteOutputServiceBlockingStub;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.BatchCreateRequest;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.BatchCreateRequest.File;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.BatchCreateRequest.Symlink;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.BatchStatRequest;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.CleanRequest;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.FileStatus;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.FinalizeActionRequest;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.FinalizeBuildRequest;
import com.google.devtools.build.lib.remote.RemoteOutputServiceProto.StartBuildRequest;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.runtime.CommandEnvironment;
import com.google.devtools.build.lib.server.FailureDetails.Execution;
import com.google.devtools.build.lib.server.FailureDetails.Execution.Code;
import com.google.devtools.build.lib.server.FailureDetails.FailureDetail;
import com.google.devtools.build.lib.unix.UnixFileSystem;
import com.google.devtools.build.lib.util.AbruptExitException;
import com.google.devtools.build.lib.util.DetailedExitCode;
import com.google.devtools.build.lib.vfs.BatchStat;
import com.google.devtools.build.lib.vfs.FileStatusWithDigest;
import com.google.devtools.build.lib.vfs.FileSystem;
import com.google.devtools.build.lib.vfs.ModifiedFileSet;
import com.google.devtools.build.lib.vfs.OutputService;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.Root;
import com.google.devtools.build.skyframe.SkyFunction.Environment;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/** Output service implementation for the remote module */
public class RemoteOutputService implements OutputService {

  private final CommandEnvironment env;
  private final ExecutorService executorService;
  @Nullable private final ManagedChannel channel;

  private final RemoteOutputChecker remoteOutputChecker;
  @Nullable private RemoteActionInputFetcher actionInputFetcher;
  @Nullable private LeaseService leaseService;
  @Nullable private Supplier<InputMetadataProvider> fileCacheSupplier;

  private final String workspaceId;
  @Nullable private String buildId;

  public RemoteOutputService(
      CommandEnvironment env,
      ExecutorService executorService,
      RemoteOutputChecker remoteOutputChecker,
      @Nullable
      ManagedChannel channelToOutputServiceDaemon) {
    this.env = checkNotNull(env);
    this.executorService = checkNotNull(executorService);
    this.remoteOutputChecker = remoteOutputChecker;
    this.workspaceId =
        DigestUtil.hashCodeToString(
            md5().hashString(checkNotNull(env.getWorkspace()).toString(), UTF_8));
    // TODO: channel pools
    this.channel = channelToOutputServiceDaemon;
  }

  void setActionInputFetcher(RemoteActionInputFetcher actionInputFetcher) {
    this.actionInputFetcher = checkNotNull(actionInputFetcher, "actionInputFetcher");
  }

  void setLeaseService(LeaseService leaseService) {
    this.leaseService = leaseService;
  }

  void setFileCacheSupplier(Supplier<InputMetadataProvider> fileCacheSupplier) {
    this.fileCacheSupplier = fileCacheSupplier;
  }

  @Override
  public ActionFileSystemType actionFileSystemType() {
    return actionInputFetcher != null
        ? ActionFileSystemType.REMOTE_FILE_SYSTEM
        : ActionFileSystemType.DISABLED;
  }

  @Nullable
  @Override
  public FileSystem createActionFileSystem(
      FileSystem delegateFileSystem,
      PathFragment execRootFragment,
      String relativeOutputPath,
      ImmutableList<Root> sourceRoots,
      ActionInputMap inputArtifactData,
      Iterable<Artifact> outputArtifacts,
      boolean rewindingEnabled) {
    checkNotNull(actionInputFetcher, "actionInputFetcher");
    return new RemoteActionFileSystem(
        delegateFileSystem,
        execRootFragment,
        relativeOutputPath,
        inputArtifactData,
        outputArtifacts,
        fileCacheSupplier.get(),
        actionInputFetcher);
  }

  @Override
  public void updateActionFileSystemContext(
      ActionExecutionMetadata action,
      FileSystem actionFileSystem,
      Environment env,
      MetadataInjector injector,
      ImmutableMap<Artifact, ImmutableList<FilesetOutputSymlink>> filesets) {
    ((RemoteActionFileSystem) actionFileSystem).updateContext(action);
  }

  @Override
  public String getFilesSystemName() {
    return "remoteActionFS";
  }

  private RemoteOutputServiceBlockingStub newBlockingStub() {
    return RemoteOutputServiceGrpc.newBlockingStub(channel);
  }

  private Path getOutputPath() {
    return env.getDirectories().getOutputPath(env.getWorkspaceName());
  }

  @Override
  public ModifiedFileSet startBuild(
      EventHandler eventHandler, UUID buildId, boolean finalizeActions) throws AbruptExitException {
    // One of the responsibilities of OutputService.startBuild() is that
    // it ensures the output path is valid. If the previous
    // OutputService redirected the output path to a remote location, we
    // must undo this.
    Path outputPath = getOutputPath();
    if (outputPath.isSymbolicLink()) {
      try {
        outputPath.delete();
      } catch (IOException e) {
        throw new AbruptExitException(
            DetailedExitCode.of(
                FailureDetail.newBuilder()
                    .setMessage(
                        String.format("Couldn't remove output path symlink: %s", e.getMessage()))
                    .setExecution(
                        Execution.newBuilder().setCode(Code.LOCAL_OUTPUT_DIRECTORY_SYMLINK_FAILURE))
                    .build()),
            e);
      }
    }

    this.buildId = buildId.toString();
    if (channel != null) {
      var fs = env.getOutputBase().getFileSystem();
      var stub = newBlockingStub();
      var request =
          StartBuildRequest.newBuilder()
              .setWorkspaceId(workspaceId)
              .setBuildId(this.buildId)
              .setOutputPath(outputPath.toString())
              .setDigestFunction(fs.getDigestFunction().toString());
      if (fs instanceof UnixFileSystem) {
        request.setUnixDigestHashAttributeName(((UnixFileSystem) fs).getHashAttributeName());
      }

      // TODO(chiwang): Handle gRPC error
      var response = stub.startBuild(request.build());
      if (response.hasInitialOutputPathContents()) {
        var modifiedFileSet = ModifiedFileSet.builder();
        for (var modifiedPath : response.getInitialOutputPathContents().getModifiedPathsList()) {
          modifiedFileSet.modify(PathFragment.create(modifiedPath));
        }
        return modifiedFileSet.build();
      }
    }

    return ModifiedFileSet.EVERYTHING_MODIFIED;
  }

  @Override
  public void flushOutputTree() throws InterruptedException {
    if (actionInputFetcher != null) {
      actionInputFetcher.flushOutputTree();
    }
  }

  @Override
  public void finalizeBuild(boolean buildSuccessful) {
    // Intentionally left empty.
    if (channel != null) {
      var stub = newBlockingStub();
      var request =
          FinalizeBuildRequest.newBuilder()
              .setBuildId(buildId)
              .setBuildSuccessful(buildSuccessful)
              .build();
      // TODO(chiwang): Handle gRPC error
      stub.finalizeBuild(request);
    }
  }

  @Subscribe
  public void onExecutionPhaseCompleteEvent(ExecutionPhaseCompleteEvent event) {
    if (leaseService != null) {
      var missingActionInputs = ImmutableSet.<ActionInput>of();
      if (actionInputFetcher != null) {
        missingActionInputs = actionInputFetcher.getMissingActionInputs();
      }
      leaseService.finalizeExecution(missingActionInputs);
    }
  }

  @Override
  public void finalizeAction(Action action, OutputMetadataStore outputMetadataStore)
      throws IOException, InterruptedException {
    if (actionInputFetcher != null) {
      actionInputFetcher.finalizeAction(action, outputMetadataStore);
    }

    if (leaseService != null) {
      leaseService.finalizeAction();
    }

    if (channel != null) {
      var request = FinalizeActionRequest.newBuilder().setBuildId(buildId);
      for (var output : action.getOutputs()) {
        if (outputMetadataStore.artifactOmitted(output)) {
          continue;
        }

        if (output.isTreeArtifact()) {
          var children = outputMetadataStore.getTreeArtifactChildren((SpecialArtifact) output);

          // We may have empty tree artifacts, in this case we just whitelist the tree artifact
          // output directory without specifying a digest.
          if (children.isEmpty()) {
            addArtifact(request, output, outputMetadataStore);
          } else {
            for (var child : children) {
              addArtifact(request, child, outputMetadataStore);
            }
          }
        } else {
          addArtifact(request, output, outputMetadataStore);
        }
      }

      // TODO(chiwang): Handle gRPC error
      var response = newBlockingStub().finalizeAction(request.build());
    }
  }

  private static void addArtifact(
      FinalizeActionRequest.Builder builder,
      Artifact artifact,
      OutputMetadataStore outputMetadataStore)
      throws IOException, InterruptedException {
    var artifactBuilder = builder.addArtifactsBuilder();
    artifactBuilder.setPath(artifact.getExecPathString());
    if (!artifact.isTreeArtifact()) {
      var metadata = outputMetadataStore.getOutputMetadata(artifact);
      if (metadata.getType().isFile()) {
        artifactBuilder.setDigest(DigestUtil.buildDigest(metadata.getDigest(), metadata.getSize()));
      }
    }
  }

  @Override
  public boolean shouldStoreRemoteOutputMetadataInActionCache() {
    return true;
  }

  @Override
  public RemoteArtifactChecker getRemoteArtifactChecker() {
    return checkNotNull(remoteOutputChecker, "remoteOutputChecker must not be null");
  }

  @Nullable
  @Override
  public BatchStat getBatchStatter() {
    if (channel != null) {
      return new BatchStat() {
        @Override
        public List<FileStatusWithDigest> batchStat(Iterable<PathFragment> paths)
            throws IOException, InterruptedException {
          var request = BatchStatRequest.newBuilder().setBuildId(buildId);
          var outputPath = getOutputPath();
          var execRoot = env.getExecRoot();
          var size = 0;
          for (var path : paths) {
            request.addPaths(execRoot.getRelative(path).relativeTo(outputPath).toString());
            size += 1;
          }
          var result = new ArrayList<FileStatusWithDigest>(size);
          // TODO(chiwang): Handle gRPC error
          var response = newBlockingStub().batchStat(request.build());
          if (response.getResponsesList().size() != size) {
            throw new IOException(
                "Number of StatResponse doesn't equal to the length of BatchStatRequest.paths");
          }

          for (var statResponse : response.getResponsesList()) {
            if (statResponse.hasFileStatus()) {
              result.add(new RemoteOutputServiceFileStatus(statResponse.getFileStatus()));
            } else {
              result.add(null);
            }
          }

          return result;
        }
      };
    }

    return null;
  }

  static class RemoteOutputServiceFileStatus implements FileStatusWithDigest {
    private final FileStatus fileStatus;

    RemoteOutputServiceFileStatus(FileStatus fileStatus) {
      this.fileStatus = fileStatus;
    }

    @Override
    public boolean isFile() {
      return fileStatus.hasFile();
    }

    @Override
    public boolean isDirectory() {
      return fileStatus.hasDirectory();
    }

    @Override
    public boolean isSymbolicLink() {
      return fileStatus.hasSymlink();
    }

    @Override
    public boolean isSpecialFile() {
      return false;
    }

    @Override
    public long getSize() throws IOException {
      if (fileStatus.hasFile()) {
        return fileStatus.getFile().getDigest().getSizeBytes();
      } else if (fileStatus.hasSymlink()) {
        return fileStatus.getSymlink().getTarget().length();
      }
      return 0;
    }

    @Override
    public long getLastModifiedTime() throws IOException {
      if (fileStatus.hasFile()) {
        return fileStatus.getFile().getLastModifiedTime().getNanos() / 1000000L;
      } else if (fileStatus.hasSymlink()) {
        return fileStatus.getSymlink().getLastModifiedTime().getNanos() / 1000000L;
      } else if (fileStatus.hasDirectory()) {
        return fileStatus.getDirectory().getLastModifiedTime().getNanos() / 1000000L;
      }
      throw new IllegalStateException("Not a valid file status");
    }

    @Override
    public long getLastChangeTime() throws IOException {
      return 0;
    }

    @Override
    public long getNodeId() throws IOException {
      return 0;
    }

    @Nullable
    @Override
    public byte[] getDigest() throws IOException {
      if (fileStatus.hasFile()) {
        return DigestUtil.toBinaryDigest(fileStatus.getFile().getDigest());
      }
      return null;
    }
  }

  @Override
  public boolean canCreateSymlinkTree() {
    /* TODO(buchgr): Optimize symlink creation for remote execution */
    return false;
  }

  @Override
  public void createSymlinkTree(
      Map<PathFragment, PathFragment> symlinks, PathFragment symlinkTreeRoot) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clean() {
    if (channel != null) {
      var stub = newBlockingStub();
      var request = CleanRequest.newBuilder().setWorkspaceId(workspaceId).build();
      // TODO(chiwang): Handle gRPC error
      stub.clean(request);
    }
  }

  @Override
  public boolean supportsPathResolverForArtifactValues() {
    return actionFileSystemType() != ActionFileSystemType.DISABLED;
  }

  @Override
  public ArtifactPathResolver createPathResolverForArtifactValues(
      PathFragment execRoot,
      String relativeOutputPath,
      FileSystem fileSystem,
      ImmutableList<Root> pathEntries,
      ActionInputMap actionInputMap,
      Map<Artifact, ImmutableCollection<? extends Artifact>> expandedArtifacts,
      Map<Artifact, ImmutableList<FilesetOutputSymlink>> filesets) {
    FileSystem remoteFileSystem =
        new RemoteActionFileSystem(
            fileSystem,
            execRoot,
            relativeOutputPath,
            actionInputMap,
            ImmutableList.of(),
            fileCacheSupplier.get(),
            actionInputFetcher);
    return ArtifactPathResolver.createPathResolver(remoteFileSystem, fileSystem.getPath(execRoot));
  }

  public RemoteOutputChecker getRemoteOutputChecker() {
    return remoteOutputChecker;
  }

  public boolean hasOutputServiceDaemon() {
    return channel != null;
  }

  public void batchCreate(Iterable<File> files, Iterable<Symlink> symlinks) throws IOException {
    checkState(channel != null);

    var request =
        BatchCreateRequest.newBuilder()
            .setBuildId(buildId)
            .addAllFiles(files)
            .addAllSymlinks(symlinks)
            .build();

    var response = newBlockingStub().batchCreate(request);
    // TODO(chiwang): Handle gRPC error
  }
}
