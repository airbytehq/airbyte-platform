/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.UUID

class ConnectionEnablementMutationLockTest {
  private val transactions = mockk<TransactionOperations<Connection>>()
  private val status = mockk<TransactionStatus<Connection>>()
  private val connection = mockk<Connection>(relaxed = true)
  private val statement = mockk<PreparedStatement>(relaxed = true)
  private val result = mockk<ResultSet>()
  private val organization = UUID.randomUUID()
  private val connectionId = UUID.randomUUID()
  private val lock = ConnectionEnablementMutationLock(transactions)

  init {
    every { status.connection } returns connection
    every { connection.prepareStatement(any<String>()) } returns statement
    every { statement.executeQuery() } returns result
    every { result.next() } returns true
    every { result.getBoolean(1) } returns true
    every { result.close() } returns Unit
    every { transactions.executeWrite<Any>(any()) } answers { firstArg<TransactionCallback<Connection, Any>>().call(status) }
  }

  @Test fun `holding the advisory lock runs the action in the same transaction`() {
    assertEquals("ran", lock.withLock(organization, connectionId) { "ran" })
    verify {
      statement.queryTimeout = 5
      statement.setString(1, "fusion-connection-enablement:$organization:$connectionId")
    }
  }

  @Test fun `database contention is a mutation-in-progress, and the action never runs`() {
    every { result.getBoolean(1) } returns false
    assertThrows(ConnectionEnablementLockException::class.java) { lock.withLock(organization, connectionId) { error("must not run") } }
  }
}
