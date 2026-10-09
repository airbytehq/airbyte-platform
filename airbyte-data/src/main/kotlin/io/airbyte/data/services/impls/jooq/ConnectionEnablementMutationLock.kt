/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.Connection
import java.util.UUID

/**
 * Thrown by [ConnectionEnablementMutationLock.withLock] instead of an API problem type: the data layer does not
 * depend on `problems-api`. The domain service translates this into a 409.
 */
class ConnectionEnablementLockException : RuntimeException("MUTATION_IN_PROGRESS")

/**
 * Serializes mutations to one connection's Fusion enablement state. [withLock] takes a Postgres
 * advisory lock for the length of the outer transaction; reads and writes made by `action` run in
 * that same transaction (via Micronaut's connection-scoped "config" datasource) and commit with it
 * on success, or roll back with it together on failure.
 */
@Singleton
class ConnectionEnablementMutationLock(
  @param:Named("config") private val transactions: TransactionOperations<Connection>,
) {
  fun <T> withLock(
    organizationId: UUID,
    connectionId: UUID,
    action: () -> T,
  ): T =
    transactions.executeWrite { status ->
      val acquired =
        status.connection.prepareStatement("SELECT pg_try_advisory_xact_lock(hashtextextended(?, 0))").use { statement ->
          statement.queryTimeout = 5
          statement.setString(1, "fusion-connection-enablement:$organizationId:$connectionId")
          statement.executeQuery().use { result ->
            result.next()
            result.getBoolean(1)
          }
        }
      if (!acquired) throw ConnectionEnablementLockException()
      action()
    }
}
