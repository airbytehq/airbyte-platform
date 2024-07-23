package io.airbyte.config.init

interface DeclarativeManifestImageVersionsProvider {
  fun getLatestDeclarativeManifestImageVersions(): Map<Int, String>
}
