/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.SecretConfig
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface SecretConfigRepository : PageableRepository<SecretConfig, UUID> {
  fun findBySecretStorageIdAndExternalCoordinate(
    secretStorageId: UUID,
    externalCoordinate: String,
  ): SecretConfig?

  @Query(
    """
    SELECT sc.* FROM secret_config sc 
    LEFT JOIN secret_reference sr ON sc.id = sr.secret_config_id 
    WHERE sr.secret_config_id IS NULL
    AND sc.created_at < :excludeCreatedAfter
    AND sc.airbyte_managed = true
    ORDER BY sc.created_at ASC
    LIMIT :limit
  """,
  )
  fun findAirbyteManagedConfigsWithoutReferences(
    excludeCreatedAfter: java.time.OffsetDateTime,
    limit: Int,
  ): List<SecretConfig>

  fun deleteByIdIn(ids: List<UUID>)
}
