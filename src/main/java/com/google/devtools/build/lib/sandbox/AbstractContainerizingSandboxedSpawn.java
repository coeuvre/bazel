// Copyright 2018 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.devtools.build.lib.sandbox;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.devtools.build.lib.exec.TreeDeleter;
import com.google.devtools.build.lib.profiler.Profiler;
import com.google.devtools.build.lib.profiler.SilentCloseable;
import com.google.devtools.build.lib.sandbox.SandboxHelpers.SandboxInputs;
import com.google.devtools.build.lib.sandbox.SandboxHelpers.SandboxOutputs;
import com.google.devtools.build.lib.vfs.FileSystemUtils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.RootedPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Implements the general flow of a sandboxed spawn that uses a container directory to build an
 * execution root for a spawn.
 */
public abstract class AbstractContainerizingSandboxedSpawn implements SandboxedSpawn {

  final Path sandboxPath;
  final Path sandboxExecRoot;
  private final ImmutableList<String> arguments;
  private final ImmutableMap<String, String> environment;
  final SandboxInputs inputs;
  final SandboxOutputs outputs;
  private final Set<Path> writableDirs;
  protected final TreeDeleter treeDeleter;
  @Nullable private final Path sandboxDebugPath;
  @Nullable private final Path statisticsPath;
  private final String mnemonic;
  private final ExecutorService inputCreationPool;

  public AbstractContainerizingSandboxedSpawn(
      Path sandboxPath,
      Path sandboxExecRoot,
      ImmutableList<String> arguments,
      ImmutableMap<String, String> environment,
      SandboxInputs inputs,
      SandboxOutputs outputs,
      Set<Path> writableDirs,
      TreeDeleter treeDeleter,
      @Nullable Path sandboxDebugPath,
      @Nullable Path statisticsPath,
      String mnemonic,
      ExecutorService inputCreationPool) {
    this.sandboxPath = sandboxPath;
    this.sandboxExecRoot = sandboxExecRoot;
    this.arguments = arguments;
    this.environment = environment;
    this.inputs = inputs;
    this.outputs = outputs;
    this.writableDirs = writableDirs;
    this.treeDeleter = treeDeleter;
    this.sandboxDebugPath = sandboxDebugPath;
    this.statisticsPath = statisticsPath;
    this.mnemonic = mnemonic;
    this.inputCreationPool = inputCreationPool;
  }

  @Override
  public Path getSandboxExecRoot() {
    return sandboxExecRoot;
  }

  @Override
  public ImmutableList<String> getArguments() {
    return arguments;
  }

  @Override
  public ImmutableMap<String, String> getEnvironment() {
    return environment;
  }

  @Override
  @Nullable
  public Path getSandboxDebugPath() {
    return sandboxDebugPath;
  }

  @Override
  @Nullable
  public Path getStatisticsPath() {
    return statisticsPath;
  }

  @Override
  public String getMnemonic() {
    return mnemonic;
  }

  @Override
  public void createFileSystem() throws IOException, InterruptedException {
    // First compute all the inputs and directories that we need. This is based only on
    // `workerFiles`, `inputs` and `outputs` and won't do any I/O.
    Set<PathFragment> inputsToCreate = new LinkedHashSet<>();
    Set<PathFragment> dirsToCreate = new LinkedHashSet<>();
    Set<PathFragment> writableSandboxDirs =
        writableDirs.stream()
            .filter(p -> p.startsWith(sandboxExecRoot))
            .map(p -> p.relativeTo(sandboxExecRoot))
            .collect(Collectors.toSet());
    try (SilentCloseable c = Profiler.instance().profile("sandbox.populateInputsAndDirsToCreate")) {
      SandboxHelpers.populateInputsAndDirsToCreate(
          writableSandboxDirs,
          inputsToCreate,
          dirsToCreate,
          Iterables.concat(
              ImmutableSet.of(), inputs.getFiles().keySet(), inputs.getSymlinks().keySet()),
          outputs);
    }

    try (SilentCloseable c = Profiler.instance().profile("sandbox.filterInputsAndDirsToCreate")) {
      // Allow subclasses to filter out inputs and dirs that don't need to be created.
      filterInputsAndDirsToCreate(inputsToCreate, dirsToCreate);
    }

    // Finally create what needs creating.
    try (SilentCloseable c = Profiler.instance().profile("sandbox.createDirectories")) {
      SandboxHelpers.createDirectories(dirsToCreate, sandboxExecRoot, /* strict= */ true);
    }
    try (SilentCloseable c = Profiler.instance().profile("sandbox.createInputs")) {
      if (inputs.getFiles().size() + inputs.getSymlinks().size() > 5000) {
        createInputs(inputsToCreate, inputs);
      } else {
        createInputs2(inputsToCreate, inputs);
      }
    }
    SandboxStash.setLastModified(sandboxPath, System.currentTimeMillis());
  }

  protected void filterInputsAndDirsToCreate(
      Set<PathFragment> inputsToCreate, Set<PathFragment> dirsToCreate)
      throws IOException, InterruptedException {}
  private static class WorkEntry {
    Path file1;
    Path file2;
    PathFragment file3;
    public enum Op {
      SYMLINK,
      COPY,
      EMPTY
    };
    Op op;
  }
  private void addBatch(List<Future<Void>> futures, List<WorkEntry> currentBatch) {
    futures.add(inputCreationPool.submit(() -> {
      for (WorkEntry task : currentBatch) {
        if (task.op == WorkEntry.Op.COPY) {
          copyFile(task.file1, task.file2);
        } else if (task.op == WorkEntry.Op.EMPTY) {
          FileSystemUtils.createEmptyFile(task.file1);
        } else {
          task.file1.createSymbolicLink(task.file3);
        }
      }
      return null;
    }));
  }

  void createInputs(Set<PathFragment> inputsToCreateSet, SandboxInputs inputs)
      throws InterruptedException {
    //List<PathFragment> inputsToCreate = new ArrayList<>(inputsToCreateSet);
    //int inputsPerBatch = (int)Math.ceil(inputsToCreate.size() / 100);
    List<Future<Void>> futures = new ArrayList<>();

    Map<PathFragment, List<PathFragment>> inputsToCreate = new HashMap<>();
    for (PathFragment file : inputsToCreateSet) {
      PathFragment parent = file.getParentDirectory();
      inputsToCreate.putIfAbsent(parent, new ArrayList<>());
      inputsToCreate.get(parent).add(file);
    }

    //Collections.shuffle(inputsToCreate);

    List<WorkEntry> currentBatch = new ArrayList<>();
    try (SilentCloseable c = Profiler.instance().profile("sandbox.createInputsLoop")) {
      for (var parentToInputs : inputsToCreate.entrySet()) {
        if (Thread.interrupted()) {
          throw new InterruptedException("Interrupted creating inputs");
        }

        for (PathFragment fragment : parentToInputs.getValue()) {
          Path key = sandboxExecRoot.getRelative(fragment);
          if (inputs.getFiles().containsKey(fragment)) {
            Path fileDest = inputs.getFiles().get(fragment);
            if (fileDest != null) {
              WorkEntry workEntry = new WorkEntry();
              workEntry.op = WorkEntry.Op.COPY;
              workEntry.file1 = fileDest;
              workEntry.file2 = key;
              currentBatch.add(workEntry);
            } else {
              WorkEntry workEntry = new WorkEntry();
              workEntry.op = WorkEntry.Op.EMPTY;
              workEntry.file1 = key;
              currentBatch.add(workEntry);
            }
          } else if (inputs.getSymlinks().containsKey(fragment)) {
            PathFragment symlinkDest = inputs.getSymlinks().get(fragment);
            if (symlinkDest != null) {
              WorkEntry workEntry = new WorkEntry();
              workEntry.op = WorkEntry.Op.SYMLINK;
              workEntry.file1 = key;
              workEntry.file3 = symlinkDest;
              currentBatch.add(workEntry);
            }
          }
        }
        // Divide in batches of at least 100 symlinks. Each directory will appear in the same
        // batch.
        if (currentBatch.size() > 100) {
          addBatch(futures, currentBatch);
          currentBatch = new ArrayList<>();
        }
      }
      if (!currentBatch.isEmpty()) {
        addBatch(futures, currentBatch);
      }
    }
    try (SilentCloseable c = Profiler.instance().profile("sandbox.createInputsFutures")) {
      int i = 1;
      for (Future<Void> future : futures) {
        try {
          //System.out.println("[" + i++ + "] Before future get:" + System.currentTimeMillis());
          future.get();
          //System.out.println("[" + i++ + "] After future get:" + System.currentTimeMillis());
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    }
    //System.out.println("Futures: " + futures.size());
  }

  /**
   * Creates all inputs needed for this spawn's sandbox.
   *
   * @param inputsToCreate The inputs that actually need to be created. Some inputs may already
   *     exist if we're reusing a previously existing sandbox.
   * @param inputs All the inputs for this spawn.
   */
  void createInputs2(Iterable<PathFragment> inputsToCreate, SandboxInputs inputs)
      throws IOException, InterruptedException {
    for (PathFragment fragment : inputsToCreate) {
      if (Thread.interrupted()) {
        throw new InterruptedException("Interrupted creating inputs");
      }
      Path key = sandboxExecRoot.getRelative(fragment);
      if (inputs.getFiles().containsKey(fragment)) {
        Path fileDest = inputs.getFiles().get(fragment);
        if (fileDest != null) {
          copyFile(fileDest, key);
        } else {
          FileSystemUtils.createEmptyFile(key);
        }
      } else if (inputs.getSymlinks().containsKey(fragment)) {
        PathFragment symlinkDest = inputs.getSymlinks().get(fragment);
        if (symlinkDest != null) {
          key.createSymbolicLink(symlinkDest);
        }
      }
    }
  }

  protected abstract void copyFile(Path source, Path target) throws IOException;

  @Override
  public void copyOutputs(Path execRoot) throws IOException {
    SandboxHelpers.moveOutputs(outputs, sandboxExecRoot, execRoot);
  }

  @Override
  public void delete() {
    try {
      treeDeleter.deleteTree(sandboxPath);
    } catch (IOException e) {
      // This usually means that the Spawn itself exited, but still has children running that
      // we couldn't wait for, which now block deletion of the sandbox directory. On Linux this
      // should never happen, as we use PID namespaces and where they are not available the
      // subreaper feature to make sure all children have been reliably killed before returning,
      // but on other OS this might not always work. The SandboxModule will try to delete them
      // again when the build is all done, at which point it hopefully works, so let's just go
      // on here.
    }
  }
}
