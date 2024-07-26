package io.airbyte.config.init

import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("localDeclarativeManifestImageVersionsProvider")
class LocalDeclarativeManifestImageVersionsProvider : DeclarativeManifestImageVersionsProvider {
  override fun getLatestDeclarativeManifestImageVersions(): List<DeclarativeManifestImageVersion> {
    return listOf(
      DeclarativeManifestImageVersion(0, "0.90.0"),
      DeclarativeManifestImageVersion(1, "1.0.1"),
      DeclarativeManifestImageVersion(2, "2.0.0"),
    )
  }
}
