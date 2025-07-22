/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.DestinationCreate
import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.data.repositories.ConnectionTemplateRepository
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.WorkspaceId
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class EmbeddedWorkspacesHandler(
  private val workspacesHandler: WorkspacesHandler,
  private val workspaceRepository: WorkspaceRepository,
  private val destinationHandler: DestinationHandler,
  private val connectionTemplateRepository: ConnectionTemplateRepository,
  private val dataplaneGroupService: DataplaneGroupService,
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

      val workspaceCreateRequest = WorkspaceCreate().name(workspaceName).organizationId(organizationId.value)
      // Create workspaces in the EU region if the organization is one of the embedded EU organizations.
      // Today, we hardcoded Genetica EU and Merkatus
      // We'll implement a proper solution soon https://github.com/airbytehq/sonar/issues/506
      if (embeddedEUOrganizations.contains(organizationId.value)) {
        val dataplaneGroupIdForEU = dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, "EU")
        workspaceCreateRequest.dataplaneGroupId(dataplaneGroupIdForEU.id)
      }
      val workspaceRead = workspacesHandler.createWorkspace(workspaceCreateRequest)
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

  companion object {
    val MERKATUS_ORGANIZATION_ID = UUID.fromString("05f31eaf-4a82-4289-8477-f13816aef696")
    val GENETICA_EU_ORGANIZATION_ID = UUID.fromString("ae42ae7c-b2dd-40e1-98cf-a44ea17c6b23")
    val embeddedEUOrganizations =
      listOf(
        MERKATUS_ORGANIZATION_ID,
        GENETICA_EU_ORGANIZATION_ID,
      )
  }
}
