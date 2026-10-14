/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.fusion

import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.jooq.ConnectionEnablementLockException
import io.airbyte.data.services.impls.jooq.ConnectionEnablementMutationLock
import io.airbyte.data.services.impls.jooq.ConnectionEnablementPersistence
import io.airbyte.data.services.impls.jooq.ConnectionEnablementRecord
import io.airbyte.domain.models.fusion.ConnectionEnablementFlags
import io.airbyte.domain.models.fusion.ConnectionEnablementState
import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionDefinition
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.util.UUID

class ConnectionEnablementServiceTest {
  private val persistence = mockk<ConnectionEnablementPersistence>()
  private val mutationLock = mockk<ConnectionEnablementMutationLock>()
  private val workspaceService = mockk<WorkspaceService>()
  private val transactions = mockk<TransactionOperations<Connection>>()
  private val service =
    ConnectionEnablementService(
      persistence,
      mutationLock,
      mockk(),
      mockk(),
      mockk(),
      workspaceService,
      transactions,
    )
  private val state =
    ConnectionEnablementState(
      UUID.randomUUID(),
      UUID.randomUUID(),
      UUID.randomUUID(),
      UUID.randomUUID(),
      UUID.randomUUID(),
      ConnectionEnablementFlags(),
    )

  @Test
  fun `PUT compares expected state`() {
    val current = state.copy(flags = ConnectionEnablementFlags(true))
    every {
      persistence.compareAndSet(state.organizationId, state.workspaceId, state.connectionId, current.flags, ConnectionEnablementFlags())
    } returns true
    assertEquals(ConnectionEnablementFlags(), service.update(current, current.flags, ConnectionEnablementFlags()).flags)
    every { persistence.compareAndSet(any(), any(), any(), ConnectionEnablementFlags(), any()) } returns false
    assertThrows<StateConflictProblem> { service.update(current, ConnectionEnablementFlags(), ConnectionEnablementFlags()) }
  }

  @Test
  fun `read cannot cross tenant boundary and database failures propagate`() {
    every { persistence.read(state.organizationId, state.connectionId) } returns null
    assertThrows<ResourceNotFoundProblem> { service.read(state.organizationId, state.connectionId) }
    every { persistence.read(state.organizationId, state.connectionId) } throws IllegalStateException("database unavailable")
    assertThrows<IllegalStateException> { service.read(state.organizationId, state.connectionId) }
  }

  @Test
  fun `withLock translates a mutation-in-progress into a state conflict and otherwise delegates`() {
    every { mutationLock.withLock<Any>(any(), any(), any()) } answers { thirdArg<() -> Any>().invoke() }
    assertEquals("ran", service.withLock(state.organizationId, state.connectionId) { "ran" })
    every { mutationLock.withLock<Any>(any(), any(), any()) } throws ConnectionEnablementLockException()
    assertThrows<StateConflictProblem> { service.withLock(state.organizationId, state.connectionId) { "unused" } }
  }

  @Test
  fun `disableLocally commits the compare-and-set in its own REQUIRES_NEW transaction`() {
    val current = state.copy(flags = ConnectionEnablementFlags(true))
    val innerStatus = mockk<TransactionStatus<Connection>>()
    every { transactions.execute<Any>(any(), any()) } answers {
      val definition = firstArg<TransactionDefinition>()
      assertEquals(TransactionDefinition.Propagation.REQUIRES_NEW, definition.propagationBehavior)
      secondArg<TransactionCallback<Connection, Any>>().call(innerStatus)
    }
    every {
      persistence.compareAndSet(state.organizationId, state.workspaceId, state.connectionId, current.flags, ConnectionEnablementFlags())
    } returns true
    assertEquals(ConnectionEnablementFlags(), service.disableLocally(current, current.flags, ConnectionEnablementFlags()).flags)
  }

  @Test
  fun `resolveConnectionWorkspace delegates to the shared connection-to-workspace lookup and never leaks which lookup failed`() {
    val workspace = StandardWorkspace().withWorkspaceId(state.workspaceId).withOrganizationId(state.organizationId)
    every { workspaceService.getStandardWorkspaceFromConnection(state.connectionId, false) } returns workspace
    assertEquals(workspace, service.resolveConnectionWorkspace(state.connectionId))

    every { workspaceService.getStandardWorkspaceFromConnection(state.connectionId, false) } throws
      ConfigNotFoundException("connection", state.connectionId.toString())
    assertThrows<ResourceNotFoundProblem> { service.resolveConnectionWorkspace(state.connectionId) }
  }

  @Test
  fun `requireEnabledForBackfill rejects a missing or disabled row and otherwise returns it`() {
    every { persistence.read(state.organizationId, state.connectionId) } returns null
    val missing = assertThrows<StateConflictProblem> { service.requireEnabledForBackfill(state.organizationId, state.connectionId) }
    assertEquals("Search indexing is not enabled for this connection.", missing.problem.getDetail())

    // read()'s own query already excludes a deprecated connection and a tombstoned source,
    // destination, or workspace, so each of those collapses into the same null-row case above --
    // there's nothing further for this method to check once a row comes back.
    val disabled = state.copy(flags = ConnectionEnablementFlags(enableIndexing = false))
    every { persistence.read(state.organizationId, state.connectionId) } returns
      ConnectionEnablementRecord(disabled.connectionId, disabled.workspaceId, disabled.sourceId, disabled.destinationId, disabled.flags)
    val notEnabled = assertThrows<StateConflictProblem> { service.requireEnabledForBackfill(state.organizationId, state.connectionId) }
    assertEquals("Search indexing is not enabled for this connection.", notEnabled.problem.getDetail())

    val enabled = state.copy(flags = ConnectionEnablementFlags(enableIndexing = true))
    every { persistence.read(state.organizationId, state.connectionId) } returns
      ConnectionEnablementRecord(enabled.connectionId, enabled.workspaceId, enabled.sourceId, enabled.destinationId, enabled.flags)
    assertEquals(enabled.flags, service.requireEnabledForBackfill(state.organizationId, state.connectionId).flags)
  }
}
