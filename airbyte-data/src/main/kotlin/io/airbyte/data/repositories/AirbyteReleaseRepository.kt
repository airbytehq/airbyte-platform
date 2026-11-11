/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.AirbyteRelease
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.CrudRepository
import java.time.OffsetDateTime
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface AirbyteReleaseRepository : CrudRepository<AirbyteRelease, UUID> {
  fun findByVersion(version: String): AirbyteRelease?

  fun findByVersionIn(versions: Collection<String>): List<AirbyteRelease>

  /** Returns 0 when the version is already recorded, so a retried publication keeps its original row. */
  @Query(
    """
    INSERT INTO airbyte_release (id, version, source_commit, published_at)
    VALUES (:id, :version, :sourceCommit, :publishedAt)
    ON CONFLICT (version) DO NOTHING
    """,
  )
  fun insertIfAbsent(
    id: UUID,
    version: String,
    sourceCommit: String,
    publishedAt: OffsetDateTime,
  ): Long
}
