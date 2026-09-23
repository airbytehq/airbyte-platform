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
import io.airbyte.data.services.impls.jooq.ActorEnablementPersistence
import io.airbyte.domain.models.fusion.ActorEnablementFlags
import io.airbyte.domain.models.fusion.ActorEnablementState
import io.airbyte.domain.models.fusion.EnablementActorType
import io.airbyte.domain.models.fusion.SyncEnablementState
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class ActorEnablementService(
  private val persistence: ActorEnablementPersistence,
  private val workspaceRepository: WorkspaceRepository,
  private val dataplaneGroupService: DataplaneGroupService,
  private val organizationRepository: OrganizationRepository,
) {
  fun isAgenticOrganization(organizationId: UUID): Boolean =
    organizationRepository.findByIdAndTombstoneFalse(organizationId).orElseThrow { ResourceNotFoundProblem() }.isAgentic

  fun read(
    organizationId: UUID,
    actorId: UUID,
    type: EnablementActorType,
  ): ActorEnablementState {
    val record = persistence.read(organizationId, actorId, type.name.lowercase()) ?: throw ResourceNotFoundProblem()
    return ActorEnablementState(organizationId, record.workspaceId, actorId, type, record.flags)
  }

  fun getRegion(
    organizationId: UUID,
    workspaceId: UUID,
  ): DataplaneGroup {
    val workspace = workspaceRepository.findByIdAndOrganizationIdAndTombstoneFalse(workspaceId, organizationId) ?: throw ResourceNotFoundProblem()
    return dataplaneGroupService.getDataplaneGroup(workspace.dataplaneGroupId)
  }

  fun update(
    current: ActorEnablementState,
    expected: ActorEnablementFlags,
    desired: ActorEnablementFlags,
  ): ActorEnablementState {
    if (!persistence.compareAndSet(
        current.organizationId,
        current.workspaceId,
        current.actorId,
        current.actorType.name.lowercase(),
        expected,
        desired,
      )
    ) {
      throw StateConflictProblem(ProblemMessageData().message("fusion_enablement_state_conflict"))
    }
    return current.copy(flags = desired)
  }

  fun assignedSyncInput(
    organizationId: UUID,
    workspaceId: UUID,
    connectionId: UUID,
    sourceId: UUID,
    destinationId: UUID,
    workloadId: String,
    dataplaneGroupId: UUID,
    serviceAccountId: UUID,
  ): SyncEnablementState? =
    persistence
      .assignedSyncInput(
        organizationId,
        workspaceId,
        connectionId,
        sourceId,
        destinationId,
        workloadId,
        dataplaneGroupId,
        serviceAccountId,
      )?.let {
        SyncEnablementState(it.inputPayload, it.sourceSearchIndexing, it.destinationSearchIndexing)
      }
}
