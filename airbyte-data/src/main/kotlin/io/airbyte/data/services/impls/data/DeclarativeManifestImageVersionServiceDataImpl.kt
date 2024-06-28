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
    majorVersion: Int,
    imageVersion: String,
  ): DeclarativeManifestImageVersion {
    val version = DeclarativeManifestImageVersion(majorVersion, imageVersion)
    if (repository.existsById(majorVersion)) {
      return repository.update(version)
    }
    return repository.save(version)
  }

  override fun getImageVersionByMajorVersion(majorVersion: Int): String {
    val resolvedVersion =
      repository.findById(majorVersion).orElseThrow {
        IllegalStateException("No declarative manifest image version found in database for major version $majorVersion")
      }
    return resolvedVersion.imageVersion
  }

  override fun listDeclarativeManifestImageVersions(): List<DeclarativeManifestImageVersion> {
    return repository.findAll()
  }
}
