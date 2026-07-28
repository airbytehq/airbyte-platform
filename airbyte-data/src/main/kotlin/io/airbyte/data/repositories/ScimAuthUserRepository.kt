/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScimAuthUser
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ScimAuthUserRepository : PageableRepository<ScimAuthUser, UUID> {
  @Query(
    """
    SELECT pg_advisory_xact_lock(hashtextextended('auth-user:' || :authUserId, 0)) IS NULL
    """,
  )
  fun acquireIdentityLock(authUserId: String): Boolean

  @Query(
    """
    SELECT *
    FROM auth_user
    WHERE auth_user_id = :authUserId
    ORDER BY user_id, auth_provider
    FOR UPDATE
    """,
  )
  fun findByAuthUserIdForUpdate(authUserId: String): List<ScimAuthUser>
}
