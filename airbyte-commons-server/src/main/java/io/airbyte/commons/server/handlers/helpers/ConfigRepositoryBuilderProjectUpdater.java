/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import java.io.IOException;

public class ConfigRepositoryBuilderProjectUpdater implements BuilderProjectUpdater {

  private final ConfigRepository configRepository;

  public ConfigRepositoryBuilderProjectUpdater(final ConfigRepository configRepository) {

    this.configRepository = configRepository;
  }

  @Override
  public void persistBuilderProjectUpdate(ExistingConnectorBuilderProjectWithWorkspaceId projectUpdate) throws ConfigNotFoundException, IOException {
    final ConnectorBuilderProject connectorBuilderProject = configRepository.getConnectorBuilderProject(projectUpdate.getBuilderProjectId(), false);

    if (connectorBuilderProject.getActorDefinitionId() != null) {
      configRepository.updateBuilderProjectAndActorDefinition(projectUpdate.getBuilderProjectId(),
          projectUpdate.getWorkspaceId(),
          projectUpdate.getBuilderProject().getName(),
          projectUpdate.getBuilderProject().getDraftManifest(),
          connectorBuilderProject.getActorDefinitionId());
    } else {
      configRepository.writeBuilderProjectDraft(projectUpdate.getBuilderProjectId(),
          projectUpdate.getWorkspaceId(),
          projectUpdate.getBuilderProject().getName(),
          projectUpdate.getBuilderProject().getDraftManifest());
    }

  }

}
