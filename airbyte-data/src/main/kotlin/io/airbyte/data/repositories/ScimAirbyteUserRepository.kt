/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScimAirbyteUser
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ScimAirbyteUserRepository : PageableRepository<ScimAirbyteUser, UUID> {
  @Query(
    """
    SELECT pg_advisory_xact_lock(hashtextextended(lower(:email), 0)) IS NULL
    """,
  )
  fun acquireGlobalEmailLock(email: String): Boolean

  @Query(
    """
    SELECT id, name, email
    FROM "user"
    WHERE lower(email) = lower(:email)
    ORDER BY id
    FOR UPDATE
    """,
  )
  fun findByEmailIgnoreCaseForUpdate(email: String): List<ScimAirbyteUser>
}
