/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import org.slf4j.LoggerFactory

/**
 * Helper class used to apply updates to source-declarative-manifest actor definition versions when
 * new versions of the image become available. This class is used by the bootloader and cron to
 * ensure that the latest compatible versions of the source-declarative-manifest images are used.
 * Versioning for builder sources is different from standard connectors.
 */
class DeclarativeSourceUpdater(
  private val declarativeManifestImageVersionsProvider: DeclarativeManifestImageVersionsProvider,
  private val declarativeManifestImageVersionService: DeclarativeManifestImageVersionService,
  private val actorDefinitionService: ActorDefinitionService,
) {
  companion object {
    private val log = LoggerFactory.getLogger(DeclarativeSourceUpdater::class.java)
  }

  fun apply() {
    val currentDeclarativeManifestImageVersions = declarativeManifestImageVersionService.listDeclarativeManifestImageVersions()
    val latestVersionsByMajor = declarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions()
    log.info("Latest source-declarative-manifest images by major: $latestVersionsByMajor")

    // Versions which are not currently in the DB - either a version for a new major or a different version for an existing major
    val versionsToPersist =
      latestVersionsByMajor.filterNot {
          (major, latestVersion) ->
        currentDeclarativeManifestImageVersions.any { it.majorVersion == major && it.imageVersion == latestVersion }
      }

    versionsToPersist.forEach { (major, newVersion) ->
      declarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(major, newVersion)
      val previousVersion = currentDeclarativeManifestImageVersions.find { it.majorVersion == major }?.imageVersion
      if (previousVersion == null) {
        log.info("Persisted declarative manifest image version for new major version $major: $newVersion")
      } else {
        log.info("Updating declarative manifest image version for major $major from $previousVersion to $newVersion")
        val numUpdated = actorDefinitionService.updateDeclarativeActorDefinitionVersions(previousVersion, newVersion)
        log.info("Updated $numUpdated declarative actor definitions from $previousVersion to $newVersion")
      }
    }
  }
}
