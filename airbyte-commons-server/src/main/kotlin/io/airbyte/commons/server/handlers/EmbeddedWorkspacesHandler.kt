/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.WorkspaceId
import jakarta.inject.Singleton

@Singleton
class EmbeddedWorkspacesHandler(
  private val workspacesHandler: WorkspacesHandler,
  private val workspaceRepository: WorkspaceRepository,
) {
  fun getOrCreate(
    organizationId: OrganizationId,
    workspaceName: String,
  ): WorkspaceId {
    val existingWorkspaces =
      workspaceRepository.findByNameAndOrganizationId(
        name = workspaceName,
        organizationId = organizationId.value,
      )

    if (existingWorkspaces.isNotEmpty()) {
      if (existingWorkspaces.size > 1) {
        throw IllegalStateException("Found multiple workspaces with the same name: $workspaceName. This is unexpected!")
      }
      return WorkspaceId(existingWorkspaces[0].id!!)
    } else {
      // FIXME: we'll eventually want to make webhooks, notifications, and default geography configurable
      // https://github.com/airbytehq/airbyte-internal-issues/issues/12785
      val workspaceRead = workspacesHandler.createWorkspace(WorkspaceCreate().name(workspaceName).organizationId(organizationId.value))

      // TODO: After creating a workspace, we'll create destinations from the connection templates
      return WorkspaceId(workspaceRead.workspaceId)
    }
  }
}
