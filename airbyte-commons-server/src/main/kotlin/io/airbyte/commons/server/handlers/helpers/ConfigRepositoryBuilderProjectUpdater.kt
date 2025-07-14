/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectorBuilderService
import java.io.IOException

class ConfigRepositoryBuilderProjectUpdater(
  private val connectorBuilderService: ConnectorBuilderService,
) : BuilderProjectUpdater {
  @Throws(IOException::class, ConfigNotFoundException::class)
  override fun persistBuilderProjectUpdate(projectUpdate: ExistingConnectorBuilderProjectWithWorkspaceId) {
    val connectorBuilderProject =
      connectorBuilderService.getConnectorBuilderProject(projectUpdate.builderProjectId, false)

    if (connectorBuilderProject.actorDefinitionId != null) {
      connectorBuilderService.updateBuilderProjectAndActorDefinition(
        projectUpdate.builderProjectId,
        projectUpdate.workspaceId,
        projectUpdate.builderProject.name,
        projectUpdate.builderProject.draftManifest,
        projectUpdate.builderProject.componentsFileContent,
        projectUpdate.builderProject.baseActorDefinitionVersionId,
        projectUpdate.builderProject.contributionPullRequestUrl,
        projectUpdate.builderProject.contributionActorDefinitionId,
        connectorBuilderProject.actorDefinitionId,
      )
    } else {
      connectorBuilderService.writeBuilderProjectDraft(
        projectUpdate.builderProjectId,
        projectUpdate.workspaceId,
        projectUpdate.builderProject.name,
        projectUpdate.builderProject.draftManifest,
        projectUpdate.builderProject.componentsFileContent,
        projectUpdate.builderProject.baseActorDefinitionVersionId,
        projectUpdate.builderProject.contributionPullRequestUrl,
        projectUpdate.builderProject.contributionActorDefinitionId,
      )
    }
  }
}
