/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileSystemBuilderProjectUpdater implements BuilderProjectUpdater {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileSystemBuilderProjectUpdater.class);

  @Override
  public void persistBuilderProjectUpdate(final ExistingConnectorBuilderProjectWithWorkspaceId projectUpdate) {
    try {
      writeJsonNodeToYamlFile(projectUpdate.getBuilderProject().getYamlManifest(), "/connectors", projectUpdate.getBuilderProject().getName());
    } catch (final Exception e) {
      /*
       * While this flow is only meant to be used for local development, we swallow all exceptions to
       * ensure this cannot affect the platform. Users can look through the logs if they suspect this is
       * failing
       */
      LOGGER.warn("Error writing manifest to local filesystem. Exception: {}. Builder Project: {}", e, projectUpdate.getBuilderProject());
    }
  }

  public static void writeJsonNodeToYamlFile(final String manifest, final String basePath, final String projectName) throws IOException {

    // Construct the file path
    final String filePath = Paths.get(basePath, "source-" + projectName, "source_" + projectName, "manifest.yaml").toString();

    final File file = new File(filePath);

    // Only try writing the file already exists
    // This isn't meant to be used for creating new connectors
    // We can revisit the flow in the future
    if (file.exists()) {
      Files.write(Paths.get(filePath), manifest.getBytes());
    }
  }

}
