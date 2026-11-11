/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.release

import io.airbyte.data.repositories.AirbyteReleaseRepository
import io.airbyte.domain.models.release.AirbyteRelease
import io.airbyte.domain.models.release.AirbyteReleaseConflictException
import io.airbyte.domain.models.release.InvalidAirbyteReleaseException
import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.Connection
import java.util.UUID
import io.airbyte.data.repositories.entities.AirbyteRelease as AirbyteReleaseEntity

private const val MAX_VERSION_LENGTH = 256
private val FULL_COMMIT_SHA = Regex("^[0-9a-f]{40}$")

@Singleton
class AirbyteReleaseService(
  private val airbyteReleaseRepository: AirbyteReleaseRepository,
  @param:Named("config") private val configTransactionOperations: TransactionOperations<Connection>,
) {
  /**
   * Records a published release once per version. A retried publication of the same commit returns the
   * original row, so its first publication time is kept.
   */
  fun record(release: AirbyteRelease): AirbyteRelease {
    validate(release)

    return configTransactionOperations.executeWrite { _ ->
      airbyteReleaseRepository.insertIfAbsent(UUID.randomUUID(), release.version, release.sourceCommit, release.publishedAt)
      val stored =
        checkNotNull(airbyteReleaseRepository.findByVersion(release.version)) { "Release ${release.version} is missing after insert" }

      if (stored.sourceCommit != release.sourceCommit) throw AirbyteReleaseConflictException(release.version)
      stored.toDomain()
    }
  }

  private fun validate(release: AirbyteRelease) {
    if (release.version.isBlank() || release.version != release.version.trim()) {
      throw InvalidAirbyteReleaseException("version must be non-blank without surrounding whitespace")
    }
    if (release.version.length > MAX_VERSION_LENGTH) {
      throw InvalidAirbyteReleaseException("version must be at most $MAX_VERSION_LENGTH characters")
    }
    if (!FULL_COMMIT_SHA.matches(release.sourceCommit)) {
      throw InvalidAirbyteReleaseException("sourceCommit must be a full 40-character lowercase commit SHA")
    }
  }
}

private fun AirbyteReleaseEntity.toDomain() = AirbyteRelease(version = version, sourceCommit = sourceCommit, publishedAt = publishedAt)
