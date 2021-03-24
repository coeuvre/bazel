package com.google.devtools.build.lib.remote;

import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.Artifact.SpecialArtifact;
import com.google.devtools.build.lib.actions.Artifact.TreeFileArtifact;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.remote.RemoteExecution.RemoteFileMetadata;
import com.google.devtools.build.lib.remote.common.RemoteActionFileArtifactValue;
import com.google.devtools.build.lib.skyframe.TreeArtifactValue;
import com.google.devtools.build.lib.vfs.*;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

public class RemoteOutputTree implements OutputTree {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final String METADATA_SUFFIX = ".bzlmd";

  private Path execRoot;

  void setExecRoot(Path execRoot) {
    this.execRoot = execRoot;
  }

  private Path getRemoteMetadataPath(PathFragment execPath) {
    return execRoot.getRelative(execPath.getPathString() + METADATA_SUFFIX);
  }

  @Override
  public void putFileMetadata(Artifact fileArtifact, FileArtifactValue metadata)
      throws IOException {
    putFileMetadata(fileArtifact.getExecPath(), metadata);
  }

  public void putFileMetadata(PathFragment execPath, FileArtifactValue metadata)
      throws IOException {
    Preconditions.checkArgument(metadata instanceof RemoteActionFileArtifactValue);
    RemoteActionFileArtifactValue value = (RemoteActionFileArtifactValue) metadata;

    Path path = getRemoteMetadataPath(execPath);
    checkNotNull(path.getParentDirectory()).createDirectoryAndParents();
    RemoteFileMetadata.newBuilder()
        .setDigest(ByteString.copyFrom(value.getDigest()))
        .setSize(value.getSize())
        .setActionId(value.getActionId())
        .setIsExecutable(value.isExecutable())
        .build()
        .writeTo(path.getOutputStream());
  }

  @Override
  public FileArtifactValue getFileMetadata(Artifact fileArtifact) {
    return getFileMetadata(fileArtifact.getExecPath());
  }

  public FileArtifactValue getFileMetadata(PathFragment execPath) {
    FileArtifactValue metadata = loadLocalFileMetadata(execPath);
    if (metadata == null) {
      metadata = loadRemoteFileMetadata(execPath);
    }
    return metadata;
  }

  private FileArtifactValue loadLocalFileMetadata(PathFragment execPath) {
    // TODO(chiwang): load from local file
    return null;
  }

  private RemoteActionFileArtifactValue loadRemoteFileMetadata(PathFragment execPath) {
    Path path = getRemoteMetadataPath(execPath);
    if (!path.exists()) {
      return null;
    }

    RemoteFileMetadata metadata;
    try {
      metadata = RemoteFileMetadata.parseFrom(path.getInputStream());
    } catch (IOException e) {
      logger.atWarning().log("Failed to read metadata from %s: %s", path, e);
      return null;
    }

    return new RemoteActionFileArtifactValue(
        metadata.getDigest().toByteArray(),
        metadata.getSize(),
        /*locationIndex=*/ 1,
        metadata.getActionId(),
        metadata.getIsExecutable());
  }

  @Override
  public void putTreeMetadata(SpecialArtifact treeArtifact, TreeArtifactValue metadata)
      throws IOException {
    PathFragment execPath = treeArtifact.getExecPath();
    for (Entry<TreeFileArtifact, FileArtifactValue> entry : metadata.getChildValues().entrySet()) {
      TreeFileArtifact key = entry.getKey();
      FileArtifactValue value = entry.getValue();
      putFileMetadata(execPath.getRelative(key.getTreeRelativePathString()), value);
    }
  }

  @Override
  public TreeArtifactValue getTreeMetadata(SpecialArtifact treeArtifact) {
    Path treePath = execRoot.getRelative(treeArtifact.getExecPath());
    if (!treePath.isDirectory(Symlinks.NOFOLLOW)) {
      return null;
    }

    TreeArtifactValue.Builder tree = TreeArtifactValue.newBuilder(treeArtifact);
    try {
      TreeArtifactValue.visitTree(
          treePath,
          ((parentRelativePath, type) -> {
            if (type == Dirent.Type.DIRECTORY) {
              return; // The final TreeArtifactValue does not contain child directories.
            }

            String relative = parentRelativePath.getPathString();
            FileArtifactValue metadata;
            if (relative.endsWith(METADATA_SUFFIX)) {
              String relativeWithoutSuffix =
                  relative.substring(0, relative.length() - METADATA_SUFFIX.length());
              metadata =
                  loadRemoteFileMetadata(
                      treeArtifact.getExecPath().getRelative(relativeWithoutSuffix));
              if (metadata != null) {
                relative = relativeWithoutSuffix;
              } else {
                metadata = loadLocalFileMetadata(treeArtifact.getExecPath().getRelative(relative));
              }
            } else {
              metadata = loadLocalFileMetadata(treeArtifact.getExecPath().getRelative(relative));
            }

            if (metadata != null) {
              TreeFileArtifact child = TreeFileArtifact.createTreeOutput(treeArtifact, relative);
              tree.putChild(child, metadata);
            }
          }));
    } catch (IOException e) {
      logger.atWarning().log("Failed to read tree metadata from %s: %s", treePath, e);
      return null;
    }

    return tree.build();
  }
}
