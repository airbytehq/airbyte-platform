/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.data.ConfigNotFoundException;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.data.services.shared.SourceAndDefinition;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * A service that manages sources.
 */
public interface SourceService {

  StandardSourceDefinition getStandardSourceDefinition(UUID sourceDefinitionId) throws JsonValidationException, IOException, ConfigNotFoundException;

  StandardSourceDefinition getStandardSourceDefinition(UUID sourceDefinitionId, boolean includeTombstone)
      throws JsonValidationException, IOException, ConfigNotFoundException;

  StandardSourceDefinition getSourceDefinitionFromSource(UUID sourceId);

  StandardSourceDefinition getSourceDefinitionFromConnection(UUID connectionId);

  List<StandardSourceDefinition> listStandardSourceDefinitions(boolean includeTombstone) throws IOException;

  List<StandardSourceDefinition> listPublicSourceDefinitions(boolean includeTombstone) throws IOException;

  List<StandardSourceDefinition> listGrantedSourceDefinitions(UUID workspaceId, boolean includeTombstones) throws IOException;

  List<Map.Entry<StandardSourceDefinition, Boolean>> listGrantableSourceDefinitions(UUID workspaceId, boolean includeTombstones) throws IOException;

  void updateStandardSourceDefinition(StandardSourceDefinition sourceDefinition) throws IOException, JsonValidationException, ConfigNotFoundException;

  SourceConnection getSourceConnection(UUID sourceId) throws JsonValidationException, ConfigNotFoundException, IOException;

  Optional<SourceConnection> getSourceConnectionIfExists(UUID sourceId);

  List<SourceConnection> listSourceConnection() throws IOException;

  List<SourceConnection> listWorkspaceSourceConnection(UUID workspaceId) throws IOException;

  Boolean isSourceActive(UUID sourceId) throws IOException;

  List<SourceConnection> listWorkspacesSourceConnections(ResourcesQueryPaginated resourcesQueryPaginated) throws IOException;

  List<SourceConnection> listSourcesForDefinition(UUID definitionId) throws IOException;

  List<SourceAndDefinition> getSourceAndDefinitionsFromSourceIds(List<UUID> sourceIds) throws IOException;

  List<SourceConnection> listSourcesWithIds(final List<UUID> sourceIds) throws IOException;

  void writeConnectorMetadata(final StandardSourceDefinition sourceDefinition,
                              final ActorDefinitionVersion actorDefinitionVersion,
                              final List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException;

  void writeCustomConnectorMetadata(final StandardSourceDefinition sourceDefinition,
                                    final ActorDefinitionVersion defaultVersion,
                                    final UUID scopeId,
                                    final io.airbyte.config.ScopeType scopeType)
      throws IOException;

  void writeSourceConnectionNoSecrets(SourceConnection partialSource) throws IOException;

  void tombstoneSource(
                       final String name,
                       final UUID workspaceId,
                       final UUID sourceId)
      throws ConfigNotFoundException, JsonValidationException, IOException;

}
