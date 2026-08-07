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
  // TODO: drop this legacy (unprefixed) lock next release, once no pods from before the
  // 'email:' prefix rename (see acquireGlobalEmailLockCurrent) remain in a rolling deploy.
  @Query(
    """
    SELECT pg_advisory_xact_lock(hashtextextended(lower(:email), 0)) IS NULL
    """,
  )
  fun acquireGlobalEmailLockLegacy(email: String): Boolean

  @Query(
    """
    SELECT pg_advisory_xact_lock(hashtextextended('email:' || lower(:email), 0)) IS NULL
    """,
  )
  fun acquireGlobalEmailLockCurrent(email: String): Boolean

  /**
   * Acquires the global email advisory lock for this one release's rolling-deploy bridge: old pods
   * (pre-rename) only ever take the legacy key, so a new pod must take both keys to serialize
   * against them. The legacy key is always acquired before the current key here, matching the
   * order UserPersistence.writeUser uses across its (possibly multi-email) lock acquisitions, so
   * two new pods can never deadlock against each other over these locks.
   */
  fun acquireGlobalEmailLock(email: String): Boolean {
    acquireGlobalEmailLockLegacy(email)
    return acquireGlobalEmailLockCurrent(email)
  }

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
