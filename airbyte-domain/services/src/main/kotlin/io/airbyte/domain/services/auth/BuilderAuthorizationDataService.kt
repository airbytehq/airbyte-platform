/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.shared.ConnectorBuilderProjectMetadata
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.BuilderProjectAuthorizationMetadata
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

@Singleton
class BuilderAuthorizationDataService(
  private val data: AuthorizationDataService,
  private val projects: ConnectorBuilderService,
) {
  fun saved(
    id: UUID,
    workspace: UUID,
  ): BuilderProjectAuthorizationMetadata {
    val project =
      try {
        projects.getConnectorBuilderProjectMetadata(id)
      } catch (
        _: ConfigNotFoundException,
      ) {
        throw AuthorizationResourceNotFoundException()
      }
    if (project.id != id || project.workspace.isEmpty || project.workspace.orElseThrow() != workspace) throw AuthorizationResourceNotFoundException()
    return metadata(project)
  }

  fun list(workspace: UUID): List<BuilderProjectAuthorizationMetadata> =
    projects.listConnectorBuilderProjectMetadata(workspace).map { project ->
      if (project.workspace.isEmpty || project.workspace.orElseThrow() != workspace) throw AuthorizationResourceNotFoundException()
      metadata(project)
    }

  fun forDefinition(
    definition: UUID,
    owner: WorkspaceAndOrganization,
  ): Optional<BuilderProjectAuthorizationMetadata> =
    projects.getConnectorBuilderProjectIdForActorDefinitionId(definition).map { id ->
      val project =
        try {
          projects.getConnectorBuilderProjectMetadata(id)
        } catch (_: ConfigNotFoundException) {
          throw AuthorizationResourceNotFoundException()
        }
      if (project.id != id || project.workspace.isEmpty || project.definition.orElse(null) != definition) {
        throw AuthorizationResourceNotFoundException()
      }
      val projectOwner = data.workspaceAndOrganization(project.workspace.orElseThrow())
      if (projectOwner.organizationId != owner.organizationId) throw AuthorizationResourceNotFoundException()
      metadata(project)
    }

  private fun metadata(project: ConnectorBuilderProjectMetadata) =
    BuilderProjectAuthorizationMetadata(project.definition, project.baseVersion, project.hasDraft)

  fun definitionUsable(
    definition: UUID,
    owner: WorkspaceAndOrganization,
  ): Boolean {
    data.requireActorDefinitionType(definition, true)
    return data.canUseActorDefinition(definition, owner)
  }

  fun baseUsable(
    version: UUID,
    owner: WorkspaceAndOrganization,
  ): Boolean = definitionUsable(data.savedDefinitionVersionParent(version), owner)

  fun forkUsable(
    definition: UUID,
    owner: WorkspaceAndOrganization,
  ): Boolean {
    data.requireActorDefinitionType(definition, true)
    data.requireDefinitionDefaultVersion(definition)
    return data.canUseActorDefinition(definition, owner)
  }
}
