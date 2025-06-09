/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.DestinationCreate
import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.data.repositories.ConnectionTemplateRepository
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.WorkspaceId
import jakarta.inject.Singleton

@Singleton
class EmbeddedWorkspacesHandler(
  private val workspacesHandler: WorkspacesHandler,
  private val workspaceRepository: WorkspaceRepository,
  private val destinationHandler: DestinationHandler,
  private val connectionTemplateRepository: ConnectionTemplateRepository,
) {
  fun getOrCreate(
    organizationId: OrganizationId,
    workspaceName: String,
  ): WorkspaceId {
    val existingWorkspaces =
      workspaceRepository.findByNameAndOrganizationIdAndTombstoneFalse(
        name = workspaceName,
        organizationId = organizationId.value,
      )

    if (existingWorkspaces.isNotEmpty()) {
      if (existingWorkspaces.size > 1) {
        throw IllegalStateException("Found multiple workspaces with the same name: $workspaceName. This is unexpected!")
      }
      return WorkspaceId(existingWorkspaces[0].id!!)
    } else {
      // FIXME: we'll eventually want to make webhooks and notifications configurable
      // https://github.com/airbytehq/airbyte-internal-issues/issues/12785
      val workspaceRead = workspacesHandler.createWorkspace(WorkspaceCreate().name(workspaceName).organizationId(organizationId.value))
      val workspaceId = WorkspaceId(workspaceRead.workspaceId)

      createDestinationsFromConnectionTemplates(organizationId, workspaceId)

      return workspaceId
    }
  }

  private fun createDestinationsFromConnectionTemplates(
    organizationId: OrganizationId,
    workspaceId: WorkspaceId,
  ) {
    val connectionTemplates = connectionTemplateRepository.findByOrganizationIdAndTombstoneFalse(organizationId.value)
    for (connectionTemplate in connectionTemplates) {
      destinationHandler.createDestination(
        DestinationCreate()
          .name(connectionTemplate.destinationName)
          .workspaceId(workspaceId.value)
          .destinationDefinitionId(connectionTemplate.destinationDefinitionId)
          .connectionConfiguration(connectionTemplate.destinationConfig),
        // FIXME .resourceAllocation() We don't support scoped resource allocations yet. https://github.com/airbytehq/airbyte-internal-issues/issues/12792
      )
    }
  }
}
