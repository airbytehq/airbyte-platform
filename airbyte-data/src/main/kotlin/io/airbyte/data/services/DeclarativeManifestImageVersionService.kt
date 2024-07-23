package io.airbyte.data.services

import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion

interface DeclarativeManifestImageVersionService {
  fun writeDeclarativeManifestImageVersion(
    majorVersion: Int,
    imageVersion: String,
  ): DeclarativeManifestImageVersion

  fun getImageVersionByMajorVersion(majorVersion: Int): String

  fun listDeclarativeManifestImageVersions(): List<DeclarativeManifestImageVersion>
}
