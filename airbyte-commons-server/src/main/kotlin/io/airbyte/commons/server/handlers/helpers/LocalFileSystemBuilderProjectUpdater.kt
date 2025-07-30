/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths

class LocalFileSystemBuilderProjectUpdater : BuilderProjectUpdater {
  override fun persistBuilderProjectUpdate(projectUpdate: ExistingConnectorBuilderProjectWithWorkspaceId) {
    try {
      writeJsonNodeToYamlFile(projectUpdate.builderProject.yamlManifest, "/connectors", projectUpdate.builderProject.name)
    } catch (e: Exception) {
            /*
             * While this flow is only meant to be used for local development, we swallow all exceptions to
             * ensure this cannot affect the platform. Users can look through the logs if they suspect this is
             * failing
             */
      log.warn { "Error writing manifest to local filesystem. Exception: {}. Builder Project: $e, projectUpdate.builderProject" }
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}

    @Throws(IOException::class)
    fun writeJsonNodeToYamlFile(
      manifest: String,
      basePath: String,
      projectName: String,
    ) {
      // Construct the file path

      val filePath = Paths.get(basePath, "source-$projectName", "source_$projectName", "manifest.yaml").toString()

      val file = File(filePath)

      // Only try writing the file already exists
      // This isn't meant to be used for creating new connectors
      // We can revisit the flow in the future
      if (file.exists()) {
        Files.write(Paths.get(filePath), manifest.toByteArray())
      }
    }
  }
}
