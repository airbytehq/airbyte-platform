/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * This service is responsible for managing the lifecycle of connector builder projects.
 */
public interface ConnectorBuilderService {

  ConnectorBuilderProject getConnectorBuilderProject(UUID builderProjectId, boolean fetchManifestDraft) throws IOException, ConfigNotFoundException;

  Optional<UUID> getConnectorBuilderProjectIdForActorDefinitionId(UUID actorDefinitionId) throws IOException;

  ConnectorBuilderProjectVersionedManifest getVersionedConnectorBuilderProject(UUID builderProjectId, Long version)
      throws ConfigNotFoundException, IOException;

  Stream<ConnectorBuilderProject> getConnectorBuilderProjectsByWorkspace(UUID workspaceId) throws IOException;

  boolean deleteBuilderProject(UUID builderProjectId) throws IOException;

  void updateBuilderProjectTestingValues(UUID projectId, JsonNode testingValues) throws IOException;

  void writeBuilderProjectDraft(UUID projectId,
                                UUID workspaceId,
                                String name,
                                JsonNode manifestDraft,
                                UUID baseActorDefinitionVersionId,
                                String contributionUrl,
                                UUID contributionActorDefinitionId)
      throws IOException;

  void deleteBuilderProjectDraft(UUID projectId) throws IOException;

  void deleteManifestDraftForActorDefinition(UUID actorDefinitionId, UUID workspaceId) throws IOException;

  void updateBuilderProjectAndActorDefinition(UUID projectId,
                                              UUID workspaceId,
                                              String name,
                                              JsonNode manifestDraft,
                                              UUID baseActorDefinitionVersionId,
                                              String contributionUrl,
                                              UUID contributionActorDefinitionId,
                                              UUID actorDefinitionId)
      throws IOException;

  void assignActorDefinitionToConnectorBuilderProject(UUID builderProjectId, UUID actorDefinitionId) throws IOException;

  void createDeclarativeManifestAsActiveVersion(DeclarativeManifest declarativeManifest,
                                                ActorDefinitionConfigInjection configInjection,
                                                ConnectorSpecification connectorSpecification,
                                                String cdkVersion)
      throws IOException;

  void setDeclarativeSourceActiveVersion(UUID sourceDefinitionId,
                                         Long version,
                                         ActorDefinitionConfigInjection configInjection,
                                         ConnectorSpecification connectorSpecification,
                                         String cdkVersion)
      throws IOException;

  Stream<ActorDefinitionConfigInjection> getActorDefinitionConfigInjections(UUID actorDefinitionId) throws IOException;

  void writeActorDefinitionConfigInjectionForPath(ActorDefinitionConfigInjection actorDefinitionConfigInjection) throws IOException;

  void insertDeclarativeManifest(DeclarativeManifest declarativeManifest) throws IOException;

  void insertActiveDeclarativeManifest(DeclarativeManifest declarativeManifest) throws IOException;

  Stream<DeclarativeManifest> getDeclarativeManifestsByActorDefinitionId(UUID actorDefinitionId) throws IOException;

  DeclarativeManifest getDeclarativeManifestByActorDefinitionIdAndVersion(UUID actorDefinitionId, long version)
      throws IOException, ConfigNotFoundException;

  DeclarativeManifest getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(UUID actorDefinitionId) throws IOException, ConfigNotFoundException;

}
