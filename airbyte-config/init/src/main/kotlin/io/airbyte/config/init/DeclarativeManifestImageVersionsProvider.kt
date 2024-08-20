package io.airbyte.config.init

import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion

interface DeclarativeManifestImageVersionsProvider {
  fun getLatestDeclarativeManifestImageVersions(): List<DeclarativeManifestImageVersion>
}
