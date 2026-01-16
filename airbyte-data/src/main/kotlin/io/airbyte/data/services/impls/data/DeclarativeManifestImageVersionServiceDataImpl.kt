/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.DeclarativeManifestImageVersionRepository
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import jakarta.inject.Singleton

@Singleton
class DeclarativeManifestImageVersionServiceDataImpl(
  private val repository: DeclarativeManifestImageVersionRepository,
) : DeclarativeManifestImageVersionService {
  override fun writeDeclarativeManifestImageVersion(
    declarativeManifestImageVersion: DeclarativeManifestImageVersion,
  ): DeclarativeManifestImageVersion {
    if (repository.existsById(declarativeManifestImageVersion.majorVersion)) {
      return repository.update(declarativeManifestImageVersion)
    }
    return repository.save(declarativeManifestImageVersion)
  }

  override fun getDeclarativeManifestImageVersionByMajorVersion(majorVersion: Int): DeclarativeManifestImageVersion =
    repository.findById(majorVersion).orElseThrow {
      IllegalStateException("No declarative manifest image version found in database for major version $majorVersion")
    }

  override fun listDeclarativeManifestImageVersions(): List<DeclarativeManifestImageVersion> = repository.findAll()
}
