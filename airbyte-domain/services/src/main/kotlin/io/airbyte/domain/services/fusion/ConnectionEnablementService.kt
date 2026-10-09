/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.fusion

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.impls.jooq.ConnectionEnablementLockException
import io.airbyte.data.services.impls.jooq.ConnectionEnablementMutationLock
import io.airbyte.data.services.impls.jooq.ConnectionEnablementPersistence
import io.airbyte.domain.models.fusion.ConnectionEnablementFlags
import io.airbyte.domain.models.fusion.ConnectionEnablementState
import io.micronaut.transaction.TransactionDefinition
import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.Connection
import java.util.UUID

@Singleton
class ConnectionEnablementService(
  private val persistence: ConnectionEnablementPersistence,
  private val mutationLock: ConnectionEnablementMutationLock,
  private val workspaceRepository: WorkspaceRepository,
  private val dataplaneGroupService: DataplaneGroupService,
  private val organizationRepository: OrganizationRepository,
  @param:Named("config") private val transactions: TransactionOperations<Connection>,
) {
  fun isAgenticOrganization(organizationId: UUID): Boolean =
    organizationRepository.findByIdAndTombstoneFalse(organizationId).orElseThrow { ResourceNotFoundProblem() }.isAgentic

  fun read(
    organizationId: UUID,
    connectionId: UUID,
  ): ConnectionEnablementState {
    val record = persistence.read(organizationId, connectionId) ?: throw ResourceNotFoundProblem()
    return ConnectionEnablementState(organizationId, record.workspaceId, connectionId, record.sourceId, record.destinationId, record.flags)
  }

  fun getRegion(
    organizationId: UUID,
    workspaceId: UUID,
  ): DataplaneGroup {
    val workspace = workspaceRepository.findByIdAndOrganizationIdAndTombstoneFalse(workspaceId, organizationId) ?: throw ResourceNotFoundProblem()
    return dataplaneGroupService.getDataplaneGroup(workspace.dataplaneGroupId)
  }

  fun update(
    current: ConnectionEnablementState,
    expected: ConnectionEnablementFlags,
    desired: ConnectionEnablementFlags,
  ): ConnectionEnablementState {
    if (!persistence.compareAndSet(current.organizationId, current.workspaceId, current.connectionId, expected, desired)) {
      throw StateConflictProblem(ProblemMessageData().message("fusion_connection_enablement_state_conflict"))
    }
    return current.copy(flags = desired)
  }

  /**
   * Serializes reads and writes for one connection's enablement so concurrent mutations cannot
   * interleave.
   */
  fun <T> withLock(
    organizationId: UUID,
    connectionId: UUID,
    action: () -> T,
  ): T =
    try {
      mutationLock.withLock(organizationId, connectionId, action)
    } catch (e: ConnectionEnablementLockException) {
      throw StateConflictProblem(ProblemMessageData().message("fusion_connection_enablement_mutation_in_progress"))
    }

  /**
   * Persists [desired] in a new transaction that commits (or rolls back) independently of the lock
   * transaction [mutate][io.airbyte.server.wrapped.sonar.FusionConnectionEnablementHandler.mutate]
   * is running in. Used by the fail-closed disable path: the local flag must be written and durably
   * committed before Agents is called, so a failed sync never leaves the local flag -- and the sync
   * gate it feeds (see #19795) -- still reporting indexing as enabled.
   */
  fun disableLocally(
    current: ConnectionEnablementState,
    expected: ConnectionEnablementFlags,
    desired: ConnectionEnablementFlags,
  ): ConnectionEnablementState =
    transactions.execute(TransactionDefinition.of(TransactionDefinition.Propagation.REQUIRES_NEW)) {
      update(current, expected, desired)
    }
}
