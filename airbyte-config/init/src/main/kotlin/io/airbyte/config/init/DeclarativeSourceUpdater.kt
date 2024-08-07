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

    // Versions which are not currently in the DB or for which the SHA for an existing version has changed
    val versionsToPersist =
      latestDeclarativeManifestImageVersions.filterNot { latestVersion ->
        currentDeclarativeManifestImageVersions.any {
          it.majorVersion == latestVersion.majorVersion &&
            it.imageVersion == latestVersion.imageVersion &&
            it.imageSha == latestVersion.imageSha
        }
      }

    versionsToPersist.filter { newVersion ->
      airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(newVersion.imageVersion).isValid
    }.forEach { newVersion ->
      declarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(newVersion)
      val previousVersion = currentDeclarativeManifestImageVersions.find { it.majorVersion == newVersion.majorVersion }
      if (previousVersion == null) {
        log.info(
          "Persisted new declarative manifest image version for new major version ${newVersion.majorVersion}: ${newVersion.imageVersion}" +
            " with sha ${newVersion.imageSha}",
        )
      } else if (previousVersion.imageVersion == newVersion.imageVersion) {
        log.info(
          "Updated sha for declarative manifest image version ${newVersion.imageVersion} from ${previousVersion.imageSha} to ${newVersion.imageSha}",
        )
      } else {
        log.info(
          "Updated declarative manifest image version for major ${newVersion.majorVersion}" +
            " from ${previousVersion.imageVersion} to ${newVersion.imageVersion}, with sha ${newVersion.imageSha}",
        )
        val numUpdated = actorDefinitionService.updateDeclarativeActorDefinitionVersions(previousVersion.imageVersion, newVersion.imageVersion)
        log.info("Updated $numUpdated declarative actor definitions from ${previousVersion.imageVersion} to ${newVersion.imageVersion}")
      }
    }
  }
}
