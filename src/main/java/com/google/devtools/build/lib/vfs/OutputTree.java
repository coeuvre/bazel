package com.google.devtools.build.lib.vfs;

import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.Artifact.SpecialArtifact;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.skyframe.TreeArtifactValue;

import javax.annotation.Nullable;
import java.io.IOException;

public interface OutputTree {

  void putFileMetadata(Artifact fileArtifact, FileArtifactValue metadata) throws IOException;

  @Nullable
  FileArtifactValue getFileMetadata(Artifact fileArtifact);

  void putTreeMetadata(SpecialArtifact treeArtifact, TreeArtifactValue metadata) throws IOException;

  @Nullable
  TreeArtifactValue getTreeMetadata(SpecialArtifact treeArtifact);
}
