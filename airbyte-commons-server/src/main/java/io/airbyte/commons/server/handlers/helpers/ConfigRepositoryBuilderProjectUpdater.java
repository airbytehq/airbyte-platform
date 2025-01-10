/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectorBuilderService;
import java.io.IOException;

public class ConfigRepositoryBuilderProjectUpdater implements BuilderProjectUpdater {

  private final ConnectorBuilderService connectorBuilderService;

  public ConfigRepositoryBuilderProjectUpdater(final ConnectorBuilderService connectorBuilderService) {

    this.connectorBuilderService = connectorBuilderService;
  }

  @Override
  public void persistBuilderProjectUpdate(final ExistingConnectorBuilderProjectWithWorkspaceId projectUpdate)
      throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject connectorBuilderProject =
        connectorBuilderService.getConnectorBuilderProject(projectUpdate.getBuilderProjectId(), false);

    if (connectorBuilderProject.getActorDefinitionId() != null) {
      connectorBuilderService.updateBuilderProjectAndActorDefinition(projectUpdate.getBuilderProjectId(),
          projectUpdate.getWorkspaceId(),
          projectUpdate.getBuilderProject().getName(),
          projectUpdate.getBuilderProject().getDraftManifest(),
          projectUpdate.getBuilderProject().getBaseActorDefinitionVersionId(),
          projectUpdate.getBuilderProject().getContributionPullRequestUrl(),
          projectUpdate.getBuilderProject().getContributionActorDefinitionId(),
          connectorBuilderProject.getActorDefinitionId());
    } else {
      connectorBuilderService.writeBuilderProjectDraft(projectUpdate.getBuilderProjectId(),
          projectUpdate.getWorkspaceId(),
          projectUpdate.getBuilderProject().getName(),
          projectUpdate.getBuilderProject().getDraftManifest(),
          projectUpdate.getBuilderProject().getBaseActorDefinitionVersionId(),
          projectUpdate.getBuilderProject().getContributionPullRequestUrl(),
          projectUpdate.getBuilderProject().getContributionActorDefinitionId());
    }

  }

}
