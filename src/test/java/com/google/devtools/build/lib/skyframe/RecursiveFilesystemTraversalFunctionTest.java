// Copyright 2015 The Bazel Authors. All rights reserved.
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
package com.google.devtools.build.lib.skyframe;

import static com.google.common.truth.Truth.assertThat;
import static com.google.devtools.build.lib.actions.FilesetTraversalParams.PackageBoundaryMode.CROSS;
import static com.google.devtools.build.lib.actions.FilesetTraversalParams.PackageBoundaryMode.DONT_CROSS;
import static com.google.devtools.build.lib.actions.FilesetTraversalParams.PackageBoundaryMode.REPORT_ERROR;
import static com.google.devtools.build.lib.skyframe.RecursiveFilesystemTraversalValue.ResolvedFileFactory.danglingSymlink;
import static com.google.devtools.build.lib.skyframe.RecursiveFilesystemTraversalValue.ResolvedFileFactory.regularFile;
import static com.google.devtools.build.lib.skyframe.RecursiveFilesystemTraversalValue.ResolvedFileFactory.symlinkToDirectory;
import static com.google.devtools.build.lib.skyframe.RecursiveFilesystemTraversalValue.ResolvedFileFactory.symlinkToFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.ArtifactRoot;
import com.google.devtools.build.lib.actions.ArtifactSkyKey;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.actions.FileStateValue;
import com.google.devtools.build.lib.actions.FileValue;
import com.google.devtools.build.lib.actions.FilesetTraversalParams.DirectTraversalRoot;
import com.google.devtools.build.lib.actions.FilesetTraversalParams.PackageBoundaryMode;
import com.google.devtools.build.lib.analysis.BlazeDirectories;
import com.google.devtools.build.lib.analysis.ConfiguredRuleClassProvider;
import com.google.devtools.build.lib.analysis.ServerDirectories;
import com.google.devtools.build.lib.analysis.util.AnalysisMock;
import com.google.devtools.build.lib.cmdline.PackageIdentifier;
import com.google.devtools.build.lib.events.NullEventHandler;
import com.google.devtools.build.lib.pkgcache.PathPackageLocator;
import com.google.devtools.build.lib.skyframe.ExternalFilesHelper.ExternalFileAction;
import com.google.devtools.build.lib.skyframe.PackageLookupFunction.CrossRepositoryLabelViolationStrategy;
import com.google.devtools.build.lib.skyframe.RecursiveFilesystemTraversalFunction.DanglingSymlinkException;
import com.google.devtools.build.lib.skyframe.RecursiveFilesystemTraversalFunction.FileOperationException;
import com.google.devtools.build.lib.skyframe.RecursiveFilesystemTraversalValue.ResolvedFile;
import com.google.devtools.build.lib.skyframe.RecursiveFilesystemTraversalValue.TraversalRequest;
import com.google.devtools.build.lib.testutil.FoundationTestCase;
import com.google.devtools.build.lib.testutil.TimestampGranularityUtils;
import com.google.devtools.build.lib.util.io.OutErr;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.Root;
import com.google.devtools.build.lib.vfs.RootedPath;
import com.google.devtools.build.skyframe.AbstractSkyKey;
import com.google.devtools.build.skyframe.ErrorInfo;
import com.google.devtools.build.skyframe.EvaluationProgressReceiver;
import com.google.devtools.build.skyframe.EvaluationResult;
import com.google.devtools.build.skyframe.InMemoryMemoizingEvaluator;
import com.google.devtools.build.skyframe.MemoizingEvaluator;
import com.google.devtools.build.skyframe.RecordingDifferencer;
import com.google.devtools.build.skyframe.SequencedRecordingDifferencer;
import com.google.devtools.build.skyframe.SequentialBuildDriver;
import com.google.devtools.build.skyframe.SkyFunction;
import com.google.devtools.build.skyframe.SkyFunctionException;
import com.google.devtools.build.skyframe.SkyFunctionException.Transience;
import com.google.devtools.build.skyframe.SkyFunctionName;
import com.google.devtools.build.skyframe.SkyKey;
import com.google.devtools.build.skyframe.SkyValue;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RecursiveFilesystemTraversalFunction}. */
@RunWith(JUnit4.class)
public final class RecursiveFilesystemTraversalFunctionTest extends FoundationTestCase {
  private static final Integer EMPTY_METADATA = new Integer(0);

  private RecordingEvaluationProgressReceiver progressReceiver;
  private MemoizingEvaluator evaluator;
  private SequentialBuildDriver driver;
  private RecordingDifferencer differencer;
  private AtomicReference<PathPackageLocator> pkgLocator;

  @Before
  public final void setUp() {
    AnalysisMock analysisMock = AnalysisMock.get();
    pkgLocator =
        new AtomicReference<>(
            new PathPackageLocator(
                outputBase,
                ImmutableList.of(Root.fromPath(rootDirectory)),
                BazelSkyframeExecutorConstants.BUILD_FILES_BY_PRIORITY));
    AtomicReference<ImmutableSet<PackageIdentifier>> deletedPackages =
        new AtomicReference<>(ImmutableSet.<PackageIdentifier>of());
    BlazeDirectories directories =
        new BlazeDirectories(
            new ServerDirectories(rootDirectory, outputBase, rootDirectory),
            rootDirectory,
            null,
            analysisMock.getProductName());
    ExternalFilesHelper externalFilesHelper = ExternalFilesHelper.createForTesting(
        pkgLocator, ExternalFileAction.DEPEND_ON_EXTERNAL_PKG_FOR_EXTERNAL_REPO_PATHS, directories);

    ConfiguredRuleClassProvider ruleClassProvider = analysisMock.createRuleClassProvider();
    Map<SkyFunctionName, SkyFunction> skyFunctions = new HashMap<>();
    skyFunctions.put(
        FileStateValue.FILE_STATE,
        new FileStateFunction(new AtomicReference<>(), externalFilesHelper));
    skyFunctions.put(FileValue.FILE, new FileFunction(pkgLocator));
    skyFunctions.put(SkyFunctions.DIRECTORY_LISTING, new DirectoryListingFunction());
    skyFunctions.put(
        SkyFunctions.DIRECTORY_LISTING_STATE,
        new DirectoryListingStateFunction(externalFilesHelper));
    skyFunctions.put(
        SkyFunctions.RECURSIVE_FILESYSTEM_TRAVERSAL, new RecursiveFilesystemTraversalFunction());
    skyFunctions.put(
        SkyFunctions.PACKAGE_LOOKUP,
        new PackageLookupFunction(
            deletedPackages,
            CrossRepositoryLabelViolationStrategy.ERROR,
            BazelSkyframeExecutorConstants.BUILD_FILES_BY_PRIORITY));
    skyFunctions.put(SkyFunctions.BLACKLISTED_PACKAGE_PREFIXES,
        new BlacklistedPackagePrefixesFunction(
            /*hardcodedBlacklistedPackagePrefixes=*/ ImmutableSet.of(),
            /*additionalBlacklistedPackagePrefixesFile=*/ PathFragment.EMPTY_FRAGMENT));
    skyFunctions.put(SkyFunctions.PACKAGE,
        new PackageFunction(null, null, null, null, null, null, null));
    skyFunctions.put(SkyFunctions.WORKSPACE_AST, new WorkspaceASTFunction(ruleClassProvider));
    skyFunctions.put(
        SkyFunctions.WORKSPACE_FILE,
        new WorkspaceFileFunction(
            ruleClassProvider,
            analysisMock
                .getPackageFactoryBuilderForTesting(directories)
                .build(ruleClassProvider),
            directories));
    skyFunctions.put(SkyFunctions.EXTERNAL_PACKAGE, new ExternalPackageFunction());
    skyFunctions.put(SkyFunctions.LOCAL_REPOSITORY_LOOKUP, new LocalRepositoryLookupFunction());
    skyFunctions.put(
        SkyFunctions.FILE_SYMLINK_INFINITE_EXPANSION_UNIQUENESS,
        new FileSymlinkInfiniteExpansionUniquenessFunction());
    skyFunctions.put(
        SkyFunctions.FILE_SYMLINK_CYCLE_UNIQUENESS, new FileSymlinkCycleUniquenessFunction());
    // We use a non-hermetic key to allow us to invalidate the proper artifacts on rebuilds. We
    // could have the artifact depend on the corresponding FileValue, but that would not cover the
    // case of a generated directory, which we have test coverage for.
    skyFunctions.put(Artifact.ARTIFACT, new ArtifactFakeFunction());
    skyFunctions.put(NONHERMETIC_ARTIFACT, new NonHermeticArtifactFakeFunction());

    progressReceiver = new RecordingEvaluationProgressReceiver();
    differencer = new SequencedRecordingDifferencer();
    evaluator = new InMemoryMemoizingEvaluator(skyFunctions, differencer, progressReceiver);
    driver = new SequentialBuildDriver(evaluator);
    PrecomputedValue.BUILD_ID.set(differencer, UUID.randomUUID());
    PrecomputedValue.PATH_PACKAGE_LOCATOR.set(differencer, pkgLocator.get());
  }

  private Artifact sourceArtifact(String path) {
    return new Artifact(
        PathFragment.create(path), ArtifactRoot.asSourceRoot(Root.fromPath(rootDirectory)));
  }

  private Artifact sourceArtifactUnderPackagePath(String path, String packagePath) {
    return new Artifact(
        PathFragment.create(path),
        ArtifactRoot.asSourceRoot(Root.fromPath(rootDirectory.getRelative(packagePath))));
  }

  private Artifact derivedArtifact(String path) {
    PathFragment execPath = PathFragment.create("out").getRelative(path);
    Artifact output =
        new Artifact(
            ArtifactRoot.asDerivedRoot(rootDirectory, rootDirectory.getRelative("out")),
            execPath);
    return output;
  }

  private static RootedPath rootedPath(Artifact artifact) {
    return RootedPath.toRootedPath(artifact.getRoot().getRoot(), artifact.getRootRelativePath());
  }

  private RootedPath rootedPath(String path, String packagePath) {
    return RootedPath.toRootedPath(
        Root.fromPath(rootDirectory.getRelative(packagePath)), PathFragment.create(path));
  }

  private static RootedPath childOf(Artifact artifact, String relative) {
    return RootedPath.toRootedPath(
        artifact.getRoot().getRoot(), artifact.getRootRelativePath().getRelative(relative));
  }

  private static RootedPath childOf(RootedPath path, String relative) {
    return RootedPath.toRootedPath(
        path.getRoot(), path.getRootRelativePath().getRelative(relative));
  }

  private static RootedPath parentOf(RootedPath path) {
    PathFragment parent =
        Preconditions.checkNotNull(path.getRootRelativePath().getParentDirectory());
    return RootedPath.toRootedPath(path.getRoot(), parent);
  }

  private static RootedPath siblingOf(RootedPath path, String relative) {
    PathFragment parent =
        Preconditions.checkNotNull(path.getRootRelativePath().getParentDirectory());
    return RootedPath.toRootedPath(path.getRoot(), parent.getRelative(relative));
  }

  private static RootedPath siblingOf(Artifact artifact, String relative) {
    PathFragment parent =
        Preconditions.checkNotNull(artifact.getRootRelativePath().getParentDirectory());
    return RootedPath.toRootedPath(artifact.getRoot().getRoot(), parent.getRelative(relative));
  }

  private void createFile(Path path, String... contents) throws Exception {
    if (!path.getParentDirectory().exists()) {
      scratch.dir(path.getParentDirectory().getPathString());
    }
    scratch.file(path.getPathString(), contents);
  }

  private void createFile(Artifact artifact, String... contents) throws Exception {
    createFile(artifact.getPath(), contents);
  }

  private RootedPath createFile(RootedPath path, String... contents) throws Exception {
    scratch.dir(parentOf(path).asPath().getPathString());
    createFile(path.asPath(), contents);
    return path;
  }

  private static TraversalRequest fileLikeRoot(Artifact file, PackageBoundaryMode pkgBoundaryMode) {
    return TraversalRequest.create(
        DirectTraversalRoot.forFileOrDirectory(file),
        !file.isSourceArtifact(),
        pkgBoundaryMode,
        false,
        null);
  }

  private static TraversalRequest pkgRoot(
      RootedPath pkgDirectory, PackageBoundaryMode pkgBoundaryMode) {
    return TraversalRequest.create(
        DirectTraversalRoot.forRootedPath(pkgDirectory), false, pkgBoundaryMode, true, null);
  }

  private <T extends SkyValue> EvaluationResult<T> eval(SkyKey key) throws Exception {
    return driver.evaluate(
        ImmutableList.of(key),
        false,
        SkyframeExecutor.DEFAULT_THREAD_COUNT,
        NullEventHandler.INSTANCE);
  }

  private RecursiveFilesystemTraversalValue evalTraversalRequest(TraversalRequest params)
      throws Exception {
    EvaluationResult<RecursiveFilesystemTraversalValue> result = eval(params);
    assertThat(result.hasError()).isFalse();
    return result.get(params);
  }

  /**
   * Asserts that the requested SkyValue can be built and results in the expected set of files.
   *
   * <p>The metadata of files is ignored in comparing the actual results with the expected ones.
   * The returned object however contains the actual metadata.
   */
  @SafeVarargs
  private final RecursiveFilesystemTraversalValue traverseAndAssertFiles(
      TraversalRequest params, ResolvedFile... expectedFilesIgnoringMetadata) throws Exception {
    RecursiveFilesystemTraversalValue result = evalTraversalRequest(params);
    Map<PathFragment, ResolvedFile> nameToActualResolvedFiles = new HashMap<>();
    for (ResolvedFile act : result.getTransitiveFiles()) {
      // We can't compare  directly, since metadata would be different, so we compare
      // by comparing the results of public method calls..
      nameToActualResolvedFiles.put(act.getNameInSymlinkTree(), act);
    }
    assertExpectedResolvedFilesPresent(nameToActualResolvedFiles, expectedFilesIgnoringMetadata);
    return result;
  }

  @SafeVarargs
  private final void assertExpectedResolvedFilesPresent(
      Map<PathFragment, ResolvedFile> nameToActualResolvedFiles,
      ResolvedFile... expectedFilesIgnoringMetadata)
      throws Exception {
    assertEquals(
        "Unequal number of ResolvedFiles in Actual and expected.",
        expectedFilesIgnoringMetadata.length,
        nameToActualResolvedFiles.size());
    for (ResolvedFile expected : expectedFilesIgnoringMetadata) {
      ResolvedFile actual = nameToActualResolvedFiles.get(expected.getNameInSymlinkTree());
      assertEquals(expected.getType(), actual.getType());
      assertEquals(expected.getPath(), actual.getPath());
      assertEquals(expected.getTargetInSymlinkTree(false), actual.getTargetInSymlinkTree(false));
      try {
        expected.getTargetInSymlinkTree(true);
        // No exception thrown, let's safely compare results.
        assertEquals(expected.getTargetInSymlinkTree(true), actual.getTargetInSymlinkTree(true));
      } catch (DanglingSymlinkException e) {
        try {
          actual.getTargetInSymlinkTree(true);
          fail("Expected exception not thrown while requesting resolved symlink.");
        } catch (DanglingSymlinkException e1) {
          // exception thrown by both expected and actual we're all good.
        }
      }
    }
  }

  private void appendToFile(RootedPath rootedPath, SkyKey toInvalidate, String content)
      throws Exception {
    Path path = rootedPath.asPath();
    if (path.exists()) {
      try (OutputStream os = path.getOutputStream(/*append=*/ true)) {
        os.write(content.getBytes(StandardCharsets.UTF_8));
      }
      differencer.invalidate(ImmutableList.of(toInvalidate));
    } else {
      createFile(path, content);
    }
  }

  private void appendToFile(RootedPath rootedPath, String content) throws Exception {
    appendToFile(rootedPath, FileStateValue.key(rootedPath), content);
  }

  private void appendToFile(Artifact file, String content) throws Exception {
    SkyKey key =
        file.isSourceArtifact()
            ? FileStateValue.key(rootedPath(file))
            : new NonHermeticArtifactSkyKey(ArtifactSkyKey.key(file, true));
    appendToFile(rootedPath(file), key, content);
  }

  private void invalidateDirectory(RootedPath path) {
    differencer.invalidate(ImmutableList.of(DirectoryListingStateValue.key(path)));
  }

  private void invalidateOutputArtifact(Artifact output) {
    assertThat(output.isSourceArtifact()).isFalse();
    differencer.invalidate(
        ImmutableList.of(new NonHermeticArtifactSkyKey(ArtifactSkyKey.key(output, true))));
  }

  private void invalidateDirectory(Artifact directoryArtifact) {
    invalidateDirectory(rootedPath(directoryArtifact));
  }

  private static final class RecordingEvaluationProgressReceiver
      extends EvaluationProgressReceiver.NullEvaluationProgressReceiver {
    Set<SkyKey> invalidations;
    Set<SkyKey> evaluations;

    RecordingEvaluationProgressReceiver() {
      clear();
    }

    void clear() {
      invalidations = Sets.newConcurrentHashSet();
      evaluations = Sets.newConcurrentHashSet();
    }

    @Override
    public void invalidated(SkyKey skyKey, InvalidationState state) {
      invalidations.add(skyKey);
    }

    @Override
    public void evaluated(
        SkyKey skyKey,
        @Nullable SkyValue value,
        Supplier<EvaluationSuccessState> evaluationSuccessState,
        EvaluationState state) {
      if (evaluationSuccessState.get().succeeded()) {
        evaluations.add(skyKey);
      }
    }
  }

  private void assertTraversalRootHashesAre(
      boolean equal, RecursiveFilesystemTraversalValue a, RecursiveFilesystemTraversalValue b)
          throws Exception {
    if (equal) {
      assertThat(a.getResolvedRoot().get().hashCode())
          .isEqualTo(b.getResolvedRoot().get().hashCode());
    } else {
      assertThat(a.getResolvedRoot().get().hashCode())
          .isNotEqualTo(b.getResolvedRoot().get().hashCode());
    }
  }

  private void assertTraversalRootHashesAreEqual(
      RecursiveFilesystemTraversalValue a, RecursiveFilesystemTraversalValue b) throws Exception {
    assertTraversalRootHashesAre(true, a, b);
  }

  private void assertTraversalRootHashesAreNotEqual(
      RecursiveFilesystemTraversalValue a, RecursiveFilesystemTraversalValue b) throws Exception {
    assertTraversalRootHashesAre(false, a, b);
  }

  private void assertTraversalOfFile(Artifact rootArtifact) throws Exception {
    TraversalRequest traversalRoot = fileLikeRoot(rootArtifact, DONT_CROSS);
    RootedPath rootedPath = createFile(rootedPath(rootArtifact), "foo");

    // Assert that the SkyValue is built and looks right.
    ResolvedFile expected = regularFile(rootedPath, EMPTY_METADATA);
    RecursiveFilesystemTraversalValue v1 = traverseAndAssertFiles(traversalRoot, expected);
    assertThat(progressReceiver.invalidations).isEmpty();
    assertThat(progressReceiver.evaluations).contains(traversalRoot);
    progressReceiver.clear();

    // Edit the file and verify that the value is rebuilt.
    appendToFile(rootArtifact, "bar");
    RecursiveFilesystemTraversalValue v2 = traverseAndAssertFiles(traversalRoot, expected);
    assertThat(progressReceiver.invalidations).contains(traversalRoot);
    assertThat(progressReceiver.evaluations).contains(traversalRoot);
    assertThat(v2).isNotEqualTo(v1);
    assertTraversalRootHashesAreNotEqual(v1, v2);

    progressReceiver.clear();
  }

  @Test
  public void testTraversalOfSourceFile() throws Exception {
    assertTraversalOfFile(sourceArtifact("foo/bar.txt"));
  }

  @Test
  public void testTraversalOfGeneratedFile() throws Exception {
    assertTraversalOfFile(derivedArtifact("foo/bar.txt"));
  }

  @Test
  public void testTraversalOfSymlinkToFile() throws Exception {
    Artifact linkNameArtifact = sourceArtifact("foo/baz/qux.sym");
    Artifact linkTargetArtifact = sourceArtifact("foo/bar/baz.txt");
    PathFragment linkValue = PathFragment.create("../bar/baz.txt");
    TraversalRequest traversalRoot = fileLikeRoot(linkNameArtifact, DONT_CROSS);
    createFile(linkTargetArtifact);
    scratch.dir(linkNameArtifact.getExecPath().getParentDirectory().getPathString());
    rootDirectory.getRelative(linkNameArtifact.getExecPath()).createSymbolicLink(linkValue);

    // Assert that the SkyValue is built and looks right.
    RootedPath symlinkNamePath = rootedPath(linkNameArtifact);
    RootedPath symlinkTargetPath = rootedPath(linkTargetArtifact);
    ResolvedFile expected =
        symlinkToFile(symlinkTargetPath, symlinkNamePath, linkValue, EMPTY_METADATA);
    RecursiveFilesystemTraversalValue v1 = traverseAndAssertFiles(traversalRoot, expected);
    assertThat(progressReceiver.invalidations).isEmpty();
    assertThat(progressReceiver.evaluations).contains(traversalRoot);
    progressReceiver.clear();

    // Edit the target of the symlink and verify that the value is rebuilt.
    appendToFile(linkTargetArtifact, "bar");
    RecursiveFilesystemTraversalValue v2 = traverseAndAssertFiles(traversalRoot, expected);
    assertThat(progressReceiver.invalidations).contains(traversalRoot);
    assertThat(progressReceiver.evaluations).contains(traversalRoot);
    assertThat(v2).isNotEqualTo(v1);
    assertTraversalRootHashesAreNotEqual(v1, v2);
  }

  @Test
  public void testTraversalOfTransitiveSymlinkToFile() throws Exception {
    Artifact directLinkArtifact = sourceArtifact("direct/file.sym");
    Artifact transitiveLinkArtifact = sourceArtifact("transitive/sym.sym");
    RootedPath fileA = createFile(rootedPath(sourceArtifact("a/file.a")));
    RootedPath directLink = rootedPath(directLinkArtifact);
    RootedPath transitiveLink = rootedPath(transitiveLinkArtifact);
    PathFragment directLinkPath = PathFragment.create("../a/file.a");
    PathFragment transitiveLinkPath = PathFragment.create("../direct/file.sym");

    parentOf(directLink).asPath().createDirectory();
    parentOf(transitiveLink).asPath().createDirectory();
    directLink.asPath().createSymbolicLink(directLinkPath);
    transitiveLink.asPath().createSymbolicLink(transitiveLinkPath);

    traverseAndAssertFiles(
        fileLikeRoot(directLinkArtifact, DONT_CROSS),
        symlinkToFile(fileA, directLink, directLinkPath, EMPTY_METADATA));

    traverseAndAssertFiles(
        fileLikeRoot(transitiveLinkArtifact, DONT_CROSS),
        symlinkToFile(fileA, transitiveLink, transitiveLinkPath, EMPTY_METADATA));
  }

  private void assertTraversalOfDirectory(Artifact directoryArtifact) throws Exception {
    // Create files under the directory.
    // Use the root + root-relative path of the rootArtifact to create these files, rather than
    // using the rootDirectory + execpath of the rootArtifact. The resulting paths are the same
    // but the RootedPaths are different:
    // in the 1st case, it is: RootedPath(/root/execroot, relative), in the second it is
    // in the 2nd case, it is: RootedPath(/root, execroot/relative).
    // Creating the files will also create the parent directories.
    RootedPath file1 = createFile(childOf(directoryArtifact, "bar.txt"));
    RootedPath file2 = createFile(childOf(directoryArtifact, "baz/qux.txt"));

    TraversalRequest traversalRoot = fileLikeRoot(directoryArtifact, DONT_CROSS);

    // Assert that the SkyValue is built and looks right.
    ResolvedFile expected1 = regularFile(file1, EMPTY_METADATA);
    ResolvedFile expected2 = regularFile(file2, EMPTY_METADATA);
    RecursiveFilesystemTraversalValue v1 =
        traverseAndAssertFiles(traversalRoot, expected1, expected2);
    assertThat(progressReceiver.invalidations).isEmpty();
    assertThat(progressReceiver.evaluations).contains(traversalRoot);
    progressReceiver.clear();

    // Add a new file to the directory and see that the value is rebuilt.
    TimestampGranularityUtils.waitForTimestampGranularity(
        directoryArtifact.getPath().stat().getLastChangeTime(), OutErr.SYSTEM_OUT_ERR);
    RootedPath file3 = createFile(childOf(directoryArtifact, "foo.txt"));
    if (directoryArtifact.isSourceArtifact()) {
      invalidateDirectory(directoryArtifact);
    } else {
      invalidateOutputArtifact(directoryArtifact);
    }
    ResolvedFile expected3 = regularFile(file3, EMPTY_METADATA);
    RecursiveFilesystemTraversalValue v2 =
        traverseAndAssertFiles(traversalRoot, expected1, expected2, expected3);
    assertThat(progressReceiver.invalidations).contains(traversalRoot);
    assertThat(progressReceiver.evaluations).contains(traversalRoot);
    // Directories always have the same hash code, but that is fine because their contents are also
    // part of the RecursiveFilesystemTraversalValue, so v1 and v2 are unequal.
    assertThat(v2).isNotEqualTo(v1);
    assertTraversalRootHashesAreEqual(v1, v2);
    progressReceiver.clear();

    // Edit a file in the directory and see that the value is rebuilt.
    RecursiveFilesystemTraversalValue v3;
    if (directoryArtifact.isSourceArtifact()) {
      SkyKey toInvalidate = FileStateValue.key(file1);
      appendToFile(file1, toInvalidate, "bar");
      v3 = traverseAndAssertFiles(traversalRoot, expected1, expected2, expected3);
      assertThat(progressReceiver.invalidations).contains(traversalRoot);
      assertThat(progressReceiver.evaluations).contains(traversalRoot);
      assertThat(v3).isNotEqualTo(v2);
      // Directories always have the same hash code, but that is fine because their contents are
      // also part of the RecursiveFilesystemTraversalValue, so v2 and v3 are unequal.
      assertTraversalRootHashesAreEqual(v2, v3);
      progressReceiver.clear();
    } else {
      // Dependency checking of output directories is unsound. Specifically, the directory mtime
      // is not changed when a contained file is modified.
      v3 = v2;
    }

    // Add a new file *outside* of the directory and see that the value is *not* rebuilt.
    Artifact someFile = sourceArtifact("somewhere/else/a.file");
    createFile(someFile, "new file");
    appendToFile(someFile, "not all changes are treated equal");
    RecursiveFilesystemTraversalValue v4 =
        traverseAndAssertFiles(traversalRoot, expected1, expected2, expected3);
    assertThat(v4).isEqualTo(v3);
    assertTraversalRootHashesAreEqual(v3, v4);
    assertThat(progressReceiver.invalidations).doesNotContain(traversalRoot);
  }

  @Test
  public void testTraversalOfSourceDirectory() throws Exception {
    assertTraversalOfDirectory(sourceArtifact("dir"));
  }

  // Note that in actual Bazel derived artifact directories are not checked for modifications on
  // incremental builds, so this test is testing a feature that Bazel does not have. It's included
  // aspirationally.
  @Test
  public void testTraversalOfGeneratedDirectory() throws Exception {
    assertTraversalOfDirectory(derivedArtifact("dir"));
  }

  @Test
  public void testTraversalOfTransitiveSymlinkToDirectory() throws Exception {
    Artifact directLinkArtifact = sourceArtifact("direct/dir.sym");
    Artifact transitiveLinkArtifact = sourceArtifact("transitive/sym.sym");
    RootedPath fileA = createFile(rootedPath(sourceArtifact("a/file.a")));
    RootedPath directLink = rootedPath(directLinkArtifact);
    RootedPath transitiveLink = rootedPath(transitiveLinkArtifact);
    PathFragment directLinkPath = PathFragment.create("../a");
    PathFragment transitiveLinkPath = PathFragment.create("../direct/dir.sym");

    parentOf(directLink).asPath().createDirectory();
    parentOf(transitiveLink).asPath().createDirectory();
    directLink.asPath().createSymbolicLink(directLinkPath);
    transitiveLink.asPath().createSymbolicLink(transitiveLinkPath);

    // Expect the file as if was a child of the direct symlink, not of the actual directory.
    traverseAndAssertFiles(
        fileLikeRoot(directLinkArtifact, DONT_CROSS),
        symlinkToDirectory(parentOf(fileA), directLink, directLinkPath, EMPTY_METADATA),
        regularFile(childOf(directLinkArtifact, "file.a"), EMPTY_METADATA));

    // Expect the file as if was a child of the transitive symlink, not of the actual directory.
    traverseAndAssertFiles(
        fileLikeRoot(transitiveLinkArtifact, DONT_CROSS),
        symlinkToDirectory(parentOf(fileA), transitiveLink, transitiveLinkPath, EMPTY_METADATA),
        regularFile(childOf(transitiveLinkArtifact, "file.a"), EMPTY_METADATA));
  }

  @Test
  public void testTraversePackage() throws Exception {
    Artifact buildFile = sourceArtifact("pkg/BUILD");
    RootedPath buildFilePath = createFile(rootedPath(buildFile));
    RootedPath file1 = createFile(siblingOf(buildFile, "subdir/file.a"));

    traverseAndAssertFiles(
        pkgRoot(parentOf(buildFilePath), DONT_CROSS),
        regularFile(buildFilePath, EMPTY_METADATA),
        regularFile(file1, EMPTY_METADATA));
  }

  @Test
  public void testTraversalOfSymlinkToDirectory() throws Exception {
    Artifact linkNameArtifact = sourceArtifact("link/foo.sym");
    Artifact linkTargetArtifact = sourceArtifact("dir");
    RootedPath linkName = rootedPath(linkNameArtifact);
    PathFragment linkValue = PathFragment.create("../dir");
    RootedPath file1 = createFile(childOf(linkTargetArtifact, "file.1"));
    createFile(childOf(linkTargetArtifact, "sub/file.2"));
    scratch.dir(parentOf(linkName).asPath().getPathString());
    linkName.asPath().createSymbolicLink(linkValue);

    // Assert that the SkyValue is built and looks right.
    TraversalRequest traversalRoot = fileLikeRoot(linkNameArtifact, DONT_CROSS);
    ResolvedFile expected1 =
        symlinkToDirectory(rootedPath(linkTargetArtifact), linkName, linkValue, EMPTY_METADATA);
    ResolvedFile expected2 = regularFile(childOf(linkNameArtifact, "file.1"), EMPTY_METADATA);
    ResolvedFile expected3 = regularFile(childOf(linkNameArtifact, "sub/file.2"), EMPTY_METADATA);
    // We expect to see all the files from the symlink'd directory, under the symlink's path, not
    // under the symlink target's path.
    RecursiveFilesystemTraversalValue v1 =
        traverseAndAssertFiles(traversalRoot, expected1, expected2, expected3);
    assertThat(progressReceiver.invalidations).isEmpty();
    assertThat(progressReceiver.evaluations).contains(traversalRoot);
    progressReceiver.clear();

    // Add a new file to the directory and see that the value is rebuilt.
    createFile(childOf(linkTargetArtifact, "file.3"));
    invalidateDirectory(linkTargetArtifact);
    ResolvedFile expected4 = regularFile(childOf(linkNameArtifact, "file.3"), EMPTY_METADATA);
    RecursiveFilesystemTraversalValue v2 =
        traverseAndAssertFiles(traversalRoot, expected1, expected2, expected3, expected4);
    assertThat(progressReceiver.invalidations).contains(traversalRoot);
    assertThat(progressReceiver.evaluations).contains(traversalRoot);
    assertThat(v2).isNotEqualTo(v1);
    assertTraversalRootHashesAreNotEqual(v1, v2);
    progressReceiver.clear();

    // Edit a file in the directory and see that the value is rebuilt.
    appendToFile(file1, "bar");
    RecursiveFilesystemTraversalValue v3 =
        traverseAndAssertFiles(traversalRoot, expected1, expected2, expected3, expected4);
    assertThat(progressReceiver.invalidations).contains(traversalRoot);
    assertThat(progressReceiver.evaluations).contains(traversalRoot);
    assertThat(v3).isNotEqualTo(v2);
    assertTraversalRootHashesAreNotEqual(v2, v3);
    progressReceiver.clear();

    // Add a new file *outside* of the directory and see that the value is *not* rebuilt.
    Artifact someFile = sourceArtifact("somewhere/else/a.file");
    createFile(someFile, "new file");
    appendToFile(someFile, "not all changes are treated equal");
    RecursiveFilesystemTraversalValue v4 =
        traverseAndAssertFiles(traversalRoot, expected1, expected2, expected3, expected4);
    assertThat(v4).isEqualTo(v3);
    assertTraversalRootHashesAreEqual(v3, v4);
    assertThat(progressReceiver.invalidations).doesNotContain(traversalRoot);
  }

  @Test
  public void testTraversalOfDanglingSymlink() throws Exception {
    Artifact linkArtifact = sourceArtifact("a/dangling.sym");
    RootedPath link = rootedPath(linkArtifact);
    PathFragment linkTarget = PathFragment.create("non_existent");
    parentOf(link).asPath().createDirectory();
    link.asPath().createSymbolicLink(linkTarget);
    traverseAndAssertFiles(
        fileLikeRoot(linkArtifact, DONT_CROSS), danglingSymlink(link, linkTarget, EMPTY_METADATA));
  }

  @Test
  public void testTraversalOfDanglingSymlinkInADirectory() throws Exception {
    Artifact dirArtifact = sourceArtifact("a");
    RootedPath file = createFile(childOf(dirArtifact, "file.txt"));
    RootedPath link = rootedPath(sourceArtifact("a/dangling.sym"));
    PathFragment linkTarget = PathFragment.create("non_existent");
    parentOf(link).asPath().createDirectory();
    link.asPath().createSymbolicLink(linkTarget);
    traverseAndAssertFiles(
        fileLikeRoot(dirArtifact, DONT_CROSS),
        regularFile(file, EMPTY_METADATA),
        danglingSymlink(link, linkTarget, EMPTY_METADATA));
  }

  private void assertTraverseSubpackages(PackageBoundaryMode traverseSubpackages) throws Exception {
    Artifact pkgDirArtifact = sourceArtifact("pkg1/foo");
    Artifact subpkgDirArtifact = sourceArtifact("pkg1/foo/subdir/subpkg");
    RootedPath pkgBuildFile = childOf(pkgDirArtifact, "BUILD");
    RootedPath subpkgBuildFile = childOf(subpkgDirArtifact, "BUILD");
    scratch.dir(rootedPath(pkgDirArtifact).asPath().getPathString());
    scratch.dir(rootedPath(subpkgDirArtifact).asPath().getPathString());
    createFile(pkgBuildFile);
    createFile(subpkgBuildFile);

    TraversalRequest traversalRoot = pkgRoot(parentOf(pkgBuildFile), traverseSubpackages);

    ResolvedFile expected1 = regularFile(pkgBuildFile, EMPTY_METADATA);
    ResolvedFile expected2 = regularFile(subpkgBuildFile, EMPTY_METADATA);
    switch (traverseSubpackages) {
      case CROSS:
        traverseAndAssertFiles(traversalRoot, expected1, expected2);
        break;
      case DONT_CROSS:
        traverseAndAssertFiles(traversalRoot, expected1);
        break;
      case REPORT_ERROR:
        SkyKey key = traversalRoot;
        EvaluationResult<SkyValue> result = eval(key);
        assertThat(result.hasError()).isTrue();
        assertThat(result.getError().getException())
            .hasMessageThat()
            .contains("crosses package boundary into package rooted at");
        break;
      default:
        throw new IllegalStateException(traverseSubpackages.toString());
    }
  }

  @Test
  public void testTraverseSubpackages() throws Exception {
    assertTraverseSubpackages(CROSS);
  }

  @Test
  public void testDoNotTraverseSubpackages() throws Exception {
    assertTraverseSubpackages(DONT_CROSS);
  }

  @Test
  public void testReportErrorWhenTraversingSubpackages() throws Exception {
    assertTraverseSubpackages(REPORT_ERROR);
  }

  @Test
  public void testSwitchPackageRootsWhenUsingMultiplePackagePaths() throws Exception {
    // Layout:
    //   pp1://a/BUILD
    //   pp1://a/file.a
    //   pp1://a/b.sym -> b/   (only created later)
    //   pp1://a/b/
    //   pp1://a/b/file.fake
    //   pp1://a/subdir/file.b
    //
    //   pp2://a/BUILD
    //   pp2://a/b/
    //   pp2://a/b/BUILD
    //   pp2://a/b/file.a
    //   pp2://a/subdir.fake/
    //   pp2://a/subdir.fake/file.fake
    //
    // Notice that pp1://a/b will be overlaid by pp2://a/b as the latter has a BUILD file and that
    // takes precedence. On the other hand the package definition pp2://a/BUILD will be ignored
    // since package //a is already defined under pp1.
    //
    // Notice also that pp1://a/b.sym is a relative symlink pointing to b/. This should be resolved
    // to the definition of //a/b/ under pp1, not under pp2.

    // Set the package paths.
    pkgLocator.set(
        new PathPackageLocator(
            outputBase,
            ImmutableList.of(
                Root.fromPath(rootDirectory.getRelative("pp1")),
                Root.fromPath(rootDirectory.getRelative("pp2"))),
            BazelSkyframeExecutorConstants.BUILD_FILES_BY_PRIORITY));
    PrecomputedValue.PATH_PACKAGE_LOCATOR.set(differencer, pkgLocator.get());

    Artifact aBuildArtifact = sourceArtifactUnderPackagePath("a/BUILD", "pp1");
    Artifact bBuildArtifact = sourceArtifactUnderPackagePath("a/b/BUILD", "pp2");

    RootedPath pp1aBuild = createFile(rootedPath(aBuildArtifact));
    RootedPath pp1aFileA = createFile(siblingOf(pp1aBuild, "file.a"));
    RootedPath pp1bFileFake = createFile(siblingOf(pp1aBuild, "b/file.fake"));
    RootedPath pp1aSubdirFileB = createFile(siblingOf(pp1aBuild, "subdir/file.b"));

    RootedPath pp2aBuild = createFile(rootedPath("a/BUILD", "pp2"));
    RootedPath pp2bBuild = createFile(rootedPath(bBuildArtifact));
    RootedPath pp2bFileA = createFile(siblingOf(pp2bBuild, "file.a"));
    createFile(siblingOf(pp2aBuild, "subdir.fake/file.fake"));

    // Traverse //a including subpackages. The result should contain the pp1-definition of //a and
    // the pp2-definition of //a/b.
    traverseAndAssertFiles(
        pkgRoot(parentOf(rootedPath(aBuildArtifact)), CROSS),
        regularFile(pp1aBuild, EMPTY_METADATA),
        regularFile(pp1aFileA, EMPTY_METADATA),
        regularFile(pp1aSubdirFileB, EMPTY_METADATA),
        regularFile(pp2bBuild, EMPTY_METADATA),
        regularFile(pp2bFileA, EMPTY_METADATA));

    // Traverse //a excluding subpackages. The result should only contain files from //a and not
    // from //a/b.
    traverseAndAssertFiles(
        pkgRoot(parentOf(rootedPath(aBuildArtifact)), DONT_CROSS),
        regularFile(pp1aBuild, EMPTY_METADATA),
        regularFile(pp1aFileA, EMPTY_METADATA),
        regularFile(pp1aSubdirFileB, EMPTY_METADATA));

    // Create a relative symlink pp1://a/b.sym -> b/. It will be resolved to the subdirectory
    // pp1://a/b, even though a package definition pp2://a/b exists.
    RootedPath pp1aBsym = siblingOf(pp1aFileA, "b.sym");
    pp1aBsym.asPath().createSymbolicLink(PathFragment.create("b"));
    invalidateDirectory(parentOf(pp1aBsym));

    // Traverse //a excluding subpackages. The relative symlink //a/b.sym points to the subdirectory
    // a/b, i.e. the pp1-definition, even though there is a pp2-defined package //a/b and we expect
    // to see b.sym/b.fake (not b/b.fake).
    traverseAndAssertFiles(
        pkgRoot(parentOf(rootedPath(aBuildArtifact)), DONT_CROSS),
        regularFile(pp1aBuild, EMPTY_METADATA),
        regularFile(pp1aFileA, EMPTY_METADATA),
        regularFile(childOf(pp1aBsym, "file.fake"), EMPTY_METADATA),
        symlinkToDirectory(
            parentOf(pp1bFileFake), pp1aBsym, PathFragment.create("b"), EMPTY_METADATA),
        regularFile(pp1aSubdirFileB, EMPTY_METADATA));
  }

  @Test
  public void testFileDigestChangeCausesRebuild() throws Exception {
    Artifact artifact = sourceArtifact("foo/bar.txt");
    RootedPath path = rootedPath(artifact);
    createFile(path, "hello");

    // Assert that the SkyValue is built and looks right.
    TraversalRequest params = fileLikeRoot(artifact, DONT_CROSS);
    ResolvedFile expected = regularFile(path, EMPTY_METADATA);
    RecursiveFilesystemTraversalValue v1 = traverseAndAssertFiles(params, expected);
    assertThat(progressReceiver.evaluations).contains(params);
    progressReceiver.clear();

    // Change the digest of the file. See that the value is rebuilt.
    appendToFile(path, "world");
    RecursiveFilesystemTraversalValue v2 = traverseAndAssertFiles(params, expected);
    assertThat(progressReceiver.invalidations).contains(params);
    assertThat(v2).isNotEqualTo(v1);
    assertTraversalRootHashesAreNotEqual(v1, v2);
  }

  @Test
  public void testFileMtimeChangeDoesNotCauseRebuildIfDigestIsUnchanged() throws Exception {
    Artifact artifact = sourceArtifact("foo/bar.txt");
    RootedPath path = rootedPath(artifact);
    createFile(path, "hello");

    // Assert that the SkyValue is built and looks right.
    TraversalRequest params = fileLikeRoot(artifact, DONT_CROSS);
    ResolvedFile expected = regularFile(path, EMPTY_METADATA);
    RecursiveFilesystemTraversalValue v1 = traverseAndAssertFiles(params, expected);
    assertThat(progressReceiver.evaluations).contains(params);
    progressReceiver.clear();

    // Change the mtime of the file but not the digest. See that the value is *not* rebuilt.
    TimestampGranularityUtils.waitForTimestampGranularity(
        path.asPath().stat().getLastChangeTime(), OutErr.SYSTEM_OUT_ERR);
    path.asPath().setLastModifiedTime(System.currentTimeMillis());
    RecursiveFilesystemTraversalValue v2 = traverseAndAssertFiles(params, expected);
    assertThat(v2).isEqualTo(v1);
    assertTraversalRootHashesAreEqual(v1, v2);
  }

  @Test
  public void testGeneratedDirectoryConflictsWithPackage() throws Exception {
    Artifact genDir = derivedArtifact("a/b");
    createFile(rootedPath(sourceArtifact("a/b/c/file.real")));
    createFile(rootedPath(derivedArtifact("a/b/c/file.fake")));
    createFile(sourceArtifact("a/b/c/BUILD"));

    SkyKey key = fileLikeRoot(genDir, CROSS);
    EvaluationResult<SkyValue> result = eval(key);
    assertThat(result.hasError()).isTrue();
    ErrorInfo error = result.getError(key);
    assertThat(error.isTransitivelyTransient()).isFalse();
    assertThat(error.getException())
        .hasMessageThat()
        .contains("Generated directory a/b/c conflicts with package under the same path.");
  }

  @Test
  public void unboundedSymlinkExpansionError() throws Exception {
    Artifact bazLink = sourceArtifact("foo/baz.sym");
    Path parentDir = scratch.dir("foo");
    bazLink.getPath().createSymbolicLink(parentDir);
    SkyKey key = pkgRoot(parentOf(rootedPath(bazLink)), DONT_CROSS);
    EvaluationResult<SkyValue> result = eval(key);
    assertThat(result.hasError()).isTrue();
    ErrorInfo error = result.getError(key);
    assertThat(error.getException()).isInstanceOf(FileOperationException.class);
    assertThat(error.getException()).hasMessageThat().contains("Infinite symlink expansion");
  }

  @Test
  public void symlinkChainError() throws Exception {
    scratch.dir("a");
    Artifact fooLink = sourceArtifact("a/foo.sym");
    Artifact barLink = sourceArtifact("a/bar.sym");
    Artifact bazLink = sourceArtifact("a/baz.sym");
    fooLink.getPath().createSymbolicLink(barLink.getPath());
    barLink.getPath().createSymbolicLink(bazLink.getPath());
    bazLink.getPath().createSymbolicLink(fooLink.getPath());

    SkyKey key = pkgRoot(parentOf(rootedPath(bazLink)), DONT_CROSS);
    EvaluationResult<SkyValue> result = eval(key);
    assertThat(result.hasError()).isTrue();
    ErrorInfo error = result.getError(key);
    assertThat(error.getException()).isInstanceOf(FileOperationException.class);
    assertThat(error.getException()).hasMessageThat().contains("Symlink cycle");
  }

  private static class NonHermeticArtifactFakeFunction implements SkyFunction {
    @Nullable
    @Override
    public SkyValue compute(SkyKey skyKey, Environment env)
        throws SkyFunctionException, InterruptedException {
      ArtifactSkyKey artifactKey = (ArtifactSkyKey) skyKey.argument();
      Artifact artifact = artifactKey.getArtifact();
      try {
        return FileArtifactValue.create(artifact.getPath());
      } catch (IOException e) {
        throw new SkyFunctionException(e, Transience.PERSISTENT){};
      }
    }

    @Nullable
    @Override
    public String extractTag(SkyKey skyKey) {
      return null;
    }
  }

  private static class ArtifactFakeFunction implements SkyFunction {
    @Nullable
    @Override
    public SkyValue compute(SkyKey skyKey, Environment env)
        throws SkyFunctionException, InterruptedException {
      return env.getValue(new NonHermeticArtifactSkyKey((ArtifactSkyKey) skyKey));
    }

    @Nullable
    @Override
    public String extractTag(SkyKey skyKey) {
      return null;
    }
  }

  private static class NonHermeticArtifactSkyKey extends AbstractSkyKey<ArtifactSkyKey> {
    private NonHermeticArtifactSkyKey(ArtifactSkyKey arg) {
      super(arg);
    }

    @Override
    public SkyFunctionName functionName() {
      return NONHERMETIC_ARTIFACT;
    }
  }

  private static final SkyFunctionName NONHERMETIC_ARTIFACT =
      SkyFunctionName.createNonHermetic("NONHERMETIC_ARTIFACT");
}
