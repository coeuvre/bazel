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
package com.google.devtools.build.lib.remote;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.devtools.build.lib.remote.util.Utils.createSpawnResult;
import static com.google.devtools.build.lib.remote.util.Utils.getInMemoryOutputPath;
import static com.google.devtools.build.lib.remote.util.Utils.hasFilesToDownload;
import static com.google.devtools.build.lib.remote.util.Utils.shouldDownloadAllSpawnOutputs;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Platform;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ExecException;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.actions.Spawn;
import com.google.devtools.build.lib.actions.SpawnMetrics;
import com.google.devtools.build.lib.actions.SpawnResult;
import com.google.devtools.build.lib.actions.SpawnResult.Status;
import com.google.devtools.build.lib.actions.Spawns;
import com.google.devtools.build.lib.actions.UserExecException;
import com.google.devtools.build.lib.actions.cache.VirtualActionInput;
import com.google.devtools.build.lib.analysis.platform.PlatformUtils;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.events.Event;
import com.google.devtools.build.lib.events.Reporter;
import com.google.devtools.build.lib.exec.SpawnCache;
import com.google.devtools.build.lib.exec.SpawnRunner.ProgressStatus;
import com.google.devtools.build.lib.exec.SpawnRunner.SpawnExecutionContext;
import com.google.devtools.build.lib.profiler.Profiler;
import com.google.devtools.build.lib.profiler.ProfilerTask;
import com.google.devtools.build.lib.profiler.SilentCloseable;
import com.google.devtools.build.lib.remote.common.CacheNotFoundException;
import com.google.devtools.build.lib.remote.common.RemoteCacheClient.ActionKey;
import com.google.devtools.build.lib.remote.merkletree.MerkleTree;
import com.google.devtools.build.lib.remote.options.RemoteOptions;
import com.google.devtools.build.lib.remote.options.RemoteOutputsMode;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.NetworkTime;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import com.google.devtools.build.lib.remote.util.Utils;
import com.google.devtools.build.lib.remote.util.Utils.InMemoryOutput;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import io.grpc.Context;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/** A remote {@link SpawnCache} implementation. */
@ThreadSafe // If the RemoteActionCache implementation is thread-safe.
final class RemoteSpawnCache implements SpawnCache {

  private final Path execRoot;
  private final RemoteOptions options;
  private final boolean verboseFailures;

  private final RemoteCache remoteCache;
  private final String buildRequestId;
  private final String commandId;

  @Nullable private final Reporter cmdlineReporter;

  private final Set<String> reportedErrors = new HashSet<>();

  private final DigestUtil digestUtil;

  /**
   * If {@link RemoteOutputsMode#TOPLEVEL} is specified it contains the artifacts that should be
   * downloaded.
   */
  private final ImmutableSet<ActionInput> filesToDownload;

  RemoteSpawnCache(
      Path execRoot,
      RemoteOptions options,
      boolean verboseFailures,
      RemoteCache remoteCache,
      String buildRequestId,
      String commandId,
      @Nullable Reporter cmdlineReporter,
      DigestUtil digestUtil,
      ImmutableSet<ActionInput> filesToDownload) {
    this.execRoot = execRoot;
    this.options = options;
    this.verboseFailures = verboseFailures;
    this.remoteCache = remoteCache;
    this.cmdlineReporter = cmdlineReporter;
    this.buildRequestId = buildRequestId;
    this.commandId = commandId;
    this.digestUtil = digestUtil;
    this.filesToDownload = Preconditions.checkNotNull(filesToDownload, "filesToDownload");
  }

  public <T> Maybe<T> profile(Maybe<T> maybe, ProfilerTask task, String description) {
    return Maybe.defer(
        () -> {
          AtomicReference<SilentCloseable> closeableRef = new AtomicReference<>(null);
          return maybe
              .doOnSubscribe(
                  (s) -> closeableRef.set(Profiler.instance().profile(task, description)))
              .doFinally(() -> closeableRef.get().close());
        });
  }

  public Completable profile(Completable completable, ProfilerTask task, String description) {
    return Completable.defer(
        () -> {
          AtomicReference<SilentCloseable> closeableRef = new AtomicReference<>(null);
          return completable
              .doOnSubscribe(
                  (s) -> closeableRef.set(Profiler.instance().profile(task, description)))
              .doFinally(() -> closeableRef.get().close());
        });
  }

  public Single<CacheHandle> lookupRx(Spawn spawn, SpawnExecutionContext context)
      throws IOException, UserExecException {
    if (!Spawns.mayBeCached(spawn)
        || (!Spawns.mayBeCachedRemotely(spawn) && useRemoteCache(options))) {
      // returning SpawnCache.NO_RESULT_NO_STORE in case the caching is disabled or in case
      // the remote caching is disabled and the only configured cache is remote.
      return Single.just(SpawnCache.NO_RESULT_NO_STORE);
    }

    NetworkTime networkTime = new NetworkTime();
    Stopwatch totalTime = Stopwatch.createStarted();

    SortedMap<PathFragment, ActionInput> inputMap = context.getInputMapping();
    MerkleTree merkleTree =
        MerkleTree.build(inputMap, context.getMetadataProvider(), execRoot, digestUtil);
    SpawnMetrics.Builder spawnMetrics =
        SpawnMetrics.Builder.forRemoteExec()
            .setInputBytes(merkleTree.getInputBytes())
            .setInputFiles(merkleTree.getInputFiles());
    Digest merkleTreeRoot = merkleTree.getRootDigest();

    // Get the remote platform properties.
    Platform platform = PlatformUtils.getPlatformProto(spawn, options);

    Command command =
        RemoteSpawnRunner.buildCommand(
            spawn.getOutputFiles(),
            spawn.getArguments(),
            spawn.getEnvironment(),
            platform,
            /* workingDirectory= */ null);
    RemoteOutputsMode remoteOutputsMode = options.remoteOutputsMode;
    Action action =
        RemoteSpawnRunner.buildAction(
            digestUtil.compute(command), merkleTreeRoot, context.getTimeout(), true);

    // Look up action cache, and reuse the action output if it is found.
    ActionKey actionKey = digestUtil.computeActionKey(action);
    Context withMetadata =
        TracingMetadataUtils.contextWithMetadata(buildRequestId, commandId, actionKey)
            .withValue(NetworkTime.CONTEXT_KEY, networkTime);

    Profiler prof = Profiler.instance();

    Maybe<CacheHandle> lookup;
    if (options.remoteAcceptCached
        || (options.incompatibleRemoteResultsIgnoreDisk && useDiskCache(options))) {
      context.report(ProgressStatus.CHECKING_CACHE, "remote-cache");

      try {
        lookup =
            withMetadata
                .call(() -> remoteCache.downloadActionResultRx(actionKey, /* inlineOutErr */ false))
                .flatMap(
                    result -> {
                      if (result.getExitCode() == 0) {
                        AtomicReference<InMemoryOutput> inMemoryOutput =
                            new AtomicReference<>(null);
                        boolean downloadOutputs =
                            shouldDownloadAllSpawnOutputs(
                                remoteOutputsMode,
                                /* exitCode = */ 0,
                                hasFilesToDownload(spawn.getOutputFiles(), filesToDownload));
                        Stopwatch fetchTime = Stopwatch.createUnstarted();

                        Completable download;
                        if (downloadOutputs) {
                          download =
                              withMetadata.call(
                                  () ->
                                      remoteCache.downloadRx(
                                          result,
                                          execRoot,
                                          context.getFileOutErr(),
                                          context::lockOutputFiles));
                        } else {
                          PathFragment inMemoryOutputPath = getInMemoryOutputPath(spawn);
                          download =
                              Completable.fromCallable(
                                  () ->
                                      withMetadata.call(
                                          () -> {
                                            try (SilentCloseable c =
                                                prof.profile(
                                                    ProfilerTask.REMOTE_DOWNLOAD,
                                                    "download outputs minimal")) {
                                              // inject output metadata
                                              InMemoryOutput output =
                                                  remoteCache.downloadMinimal(
                                                      actionKey.getDigest().getHash(),
                                                      result,
                                                      spawn.getOutputFiles(),
                                                      inMemoryOutputPath,
                                                      context.getFileOutErr(),
                                                      execRoot,
                                                      context.getMetadataInjector(),
                                                      context::lockOutputFiles);
                                              inMemoryOutput.set(output);
                                            }
                                            return null;
                                          }));
                        }

                        return Completable.complete()
                            .doOnComplete(fetchTime::start)
                            .andThen(download)
                            .toSingle(
                                () -> {
                                  fetchTime.stop();
                                  totalTime.stop();
                                  spawnMetrics
                                      .setFetchTime(fetchTime.elapsed())
                                      .setTotalTime(totalTime.elapsed())
                                      .setNetworkTime(networkTime.getDuration());
                                  SpawnResult spawnResult =
                                      createSpawnResult(
                                          result.getExitCode(),
                                          /*cacheHit=*/ true,
                                          "remote",
                                          inMemoryOutput.get(),
                                          spawnMetrics.build(),
                                          spawn.getMnemonic());

                                  return SpawnCache.success(spawnResult);
                                })
                            .toMaybe();
                      }

                      return Maybe.empty();
                    })
                .onErrorResumeNext(
                    e -> {
                      if (e instanceof CacheNotFoundException) {
                        return Maybe.empty();
                      } else if (e instanceof IOException) {
                        if (BulkTransferException.isOnlyCausedByCacheNotFoundException(
                            (IOException) e)) {
                          // Intentionally left blank
                        } else {
                          String errorMessage;
                          if (!verboseFailures) {
                            errorMessage = Utils.grpcAwareErrorMessage((IOException) e);
                          } else {
                            // On --verbose_failures print the whole stack trace
                            errorMessage = Throwables.getStackTraceAsString(e);
                          }
                          if (isNullOrEmpty(errorMessage)) {
                            errorMessage = e.getClass().getSimpleName();
                          }
                          errorMessage = "Reading from Remote Cache:\n" + errorMessage;
                          report(Event.warn(errorMessage));
                        }

                        return Maybe.empty();
                      }

                      return Maybe.error(e);
                    });
      } catch (Exception e) {
        lookup = Maybe.error(e);
      }
    } else {
      lookup = Maybe.empty();
    }

    return lookup.switchIfEmpty(
        Single.fromCallable(
            () -> {
              context.prefetchInputs();

              if (options.remoteUploadLocalResults
                  || (options.incompatibleRemoteResultsIgnoreDisk && useDiskCache(options))) {
                return new CacheHandle() {
                  @Override
                  public boolean hasResult() {
                    return false;
                  }

                  @Override
                  public SpawnResult getResult() {
                    throw new NoSuchElementException();
                  }

                  @Override
                  public boolean willStore() {
                    return true;
                  }

                  @Override
                  public void store(SpawnResult result) throws ExecException, InterruptedException {
                    boolean uploadResults =
                        Status.SUCCESS.equals(result.status()) && result.exitCode() == 0;
                    if (!uploadResults) {
                      return;
                    }

                    if (options.experimentalGuardAgainstConcurrentChanges) {
                      try (SilentCloseable c =
                          prof.profile("RemoteCache.checkForConcurrentModifications")) {
                        checkForConcurrentModifications();
                      } catch (IOException e) {
                        report(Event.warn(e.getMessage()));
                        return;
                      }
                    }

                    Context previous = withMetadata.attach();
                    Collection<Path> files =
                        RemoteSpawnRunner.resolveActionInputs(execRoot, spawn.getOutputFiles());
                    try (SilentCloseable c =
                        prof.profile(ProfilerTask.UPLOAD_TIME, "upload outputs")) {
                      remoteCache.upload(
                          actionKey, action, command, execRoot, files, context.getFileOutErr());
                    } catch (IOException e) {
                      String errorMessage;
                      if (!verboseFailures) {
                        errorMessage = Utils.grpcAwareErrorMessage(e);
                      } else {
                        // On --verbose_failures print the whole stack trace
                        errorMessage = Throwables.getStackTraceAsString(e);
                      }
                      if (isNullOrEmpty(errorMessage)) {
                        errorMessage = e.getClass().getSimpleName();
                      }
                      errorMessage = "Writing to Remote Cache:\n" + errorMessage;
                      report(Event.warn(errorMessage));
                    } finally {
                      withMetadata.detach(previous);
                    }
                  }

                  @Override
                  public void close() {}

                  private void checkForConcurrentModifications() throws IOException {
                    for (ActionInput input : inputMap.values()) {
                      if (input instanceof VirtualActionInput) {
                        continue;
                      }
                      FileArtifactValue metadata = context.getMetadataProvider().getMetadata(input);
                      Path path = execRoot.getRelative(input.getExecPath());
                      if (metadata.wasModifiedSinceDigest(path)) {
                        throw new IOException(path + " was modified during execution");
                      }
                    }
                  }
                };
              } else {
                return SpawnCache.NO_RESULT_NO_STORE;
              }
            }));
  }

  @Override
  public CacheHandle lookup(Spawn spawn, SpawnExecutionContext context)
      throws InterruptedException, IOException, ExecException {
    try {
      return lookupRx(spawn, context).blockingGet();
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t.getCause(), InterruptedException.class);
      Throwables.throwIfInstanceOf(t.getCause(), IOException.class);
      Throwables.throwIfInstanceOf(t.getCause(), ExecException.class);
      throw t;
    }
  }

  private void report(Event evt) {
    if (cmdlineReporter == null) {
      return;
    }

    synchronized (this) {
      if (reportedErrors.contains(evt.getMessage())) {
        return;
      }
      reportedErrors.add(evt.getMessage());
      cmdlineReporter.handle(evt);
    }
  }

  private static boolean useRemoteCache(RemoteOptions options) {
    return !isNullOrEmpty(options.remoteCache) || !isNullOrEmpty(options.remoteExecutor);
  }

  private static boolean useDiskCache(RemoteOptions options) {
    return options.diskCache != null && !options.diskCache.isEmpty();
  }

  @Override
  public boolean usefulInDynamicExecution() {
    return false;
  }
}
