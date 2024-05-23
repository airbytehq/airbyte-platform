package io.airbyte.config.init

import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("localDeclarativeManifestImageVersionsProvider")
class LocalDeclarativeManifestImageVersionsProvider : DeclarativeManifestImageVersionsProvider {
  override fun getLatestDeclarativeManifestImageVersions(): Map<Int, String> {
    return mapOf(
      0 to "0.90.0",
      1 to "1.0.1",
    )
  }
}
