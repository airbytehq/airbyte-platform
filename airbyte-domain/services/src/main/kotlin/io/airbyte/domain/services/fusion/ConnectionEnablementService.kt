/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.fusion

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.WorkspaceService
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
  private val workspaceService: WorkspaceService,
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

  /**
   * Resolves a connection's workspace via its source, purely to find the organization and
   * workspace a caller (e.g. a backfill request) needs before it can even ask about enablement
   * -- [read] itself needs the organization ID as an input, so this has to come first and outside
   * any lock. Throws [ResourceNotFoundProblem] for an unknown connection, a tombstoned source, or
   * an unknown workspace -- the caller cannot tell these apart from each other, which is the
   * point: it keeps an unauthorized caller from learning anything about a connection that is not
   * theirs.
   */
  fun resolveConnectionWorkspace(connectionId: UUID): StandardWorkspace =
    try {
      workspaceService.getStandardWorkspaceFromConnection(connectionId, false)
    } catch (_: ConfigNotFoundException) {
      throw ResourceNotFoundProblem()
    }

  /**
   * Requires the connection to be enabled for Fusion indexing, for a caller (e.g. a backfill
   * request) that must never proceed against a connection that isn't (or is no longer) properly
   * enabled. [read]'s query already excludes a deprecated connection and a tombstoned source,
   * destination, or workspace, so a null result here collapses all of those into the same
   * conflict as an explicitly disabled connection. Disabling is what deletes a connection's
   * indexes, so this must run inside [withLock] together with whatever enqueues work against the
   * result -- the same lock [io.airbyte.server.wrapped.sonar.FusionConnectionEnablementHandler]
   * takes to disable -- otherwise a disable landing between this read and the enqueue would still
   * let a backfill through against indexes that are already gone.
   */
  fun requireEnabledForBackfill(
    organizationId: UUID,
    connectionId: UUID,
  ): ConnectionEnablementState {
    val enablement =
      try {
        read(organizationId, connectionId)
      } catch (_: ResourceNotFoundProblem) {
        throw StateConflictProblem(
          "Search indexing is not enabled for this connection.",
          ProblemMessageData().message("fusion_connection_not_enabled"),
        )
      }
    if (!enablement.flags.enableIndexing) {
      throw StateConflictProblem("Search indexing is not enabled for this connection.", ProblemMessageData().message("fusion_connection_not_enabled"))
    }
    return enablement
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
