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
  private val airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator,
) {
  companion object {
    private val log = LoggerFactory.getLogger(DeclarativeSourceUpdater::class.java)
  }

  fun apply() {
    val currentDeclarativeManifestImageVersions = declarativeManifestImageVersionService.listDeclarativeManifestImageVersions()
    val latestDeclarativeManifestImageVersions = declarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions()

    // Versions which are not currently in the DB - either a version for a new major or a different version for an existing major
    val versionsToPersist =
      latestDeclarativeManifestImageVersions.filterNot { latestVersion ->
        currentDeclarativeManifestImageVersions.any { it.majorVersion == latestVersion.majorVersion && it.imageVersion == latestVersion.imageVersion }
      }

    versionsToPersist.filter { newVersion ->
      airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(newVersion.imageVersion).isValid
    }.forEach { newVersion ->
      declarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(newVersion)
      val previousVersion = currentDeclarativeManifestImageVersions.find { it.majorVersion == newVersion.majorVersion }?.imageVersion
      if (previousVersion == null) {
        log.info("Persisted declarative manifest image version for new major version ${newVersion.majorVersion}: ${newVersion.imageVersion}")
      } else {
        log.info(
          "Updating declarative manifest image version for major ${newVersion.majorVersion} from $previousVersion to ${newVersion.imageVersion}",
        )
        val numUpdated = actorDefinitionService.updateDeclarativeActorDefinitionVersions(previousVersion, newVersion.imageVersion)
        log.info("Updated $numUpdated declarative actor definitions from $previousVersion to ${newVersion.imageVersion}")
      }
    }
  }
}
