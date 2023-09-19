/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.data.services.shared.SourceAndDefinition;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * A service that manages sources.
 */
public interface SourceService {

  StandardSourceDefinition getStandardSourceDefinition(UUID sourceDefinitionId) throws JsonValidationException, IOException, ConfigNotFoundException;

  StandardSourceDefinition getSourceDefinitionFromSource(UUID sourceId);

  StandardSourceDefinition getSourceDefinitionFromConnection(UUID connectionId);

  List<StandardSourceDefinition> listStandardSourceDefinitions(boolean includeTombstone) throws IOException;

  Stream<StandardSourceDefinition> sourceDefQuery(Optional<UUID> sourceDefId, boolean includeTombstone) throws IOException;

  List<StandardSourceDefinition> listPublicSourceDefinitions(boolean includeTombstone) throws IOException;

  List<StandardSourceDefinition> listGrantedSourceDefinitions(UUID workspaceId, boolean includeTombstones) throws IOException;

  List<Map.Entry<StandardSourceDefinition, Boolean>> listGrantableSourceDefinitions(UUID workspaceId, boolean includeTombstones) throws IOException;

  void updateStandardSourceDefinition(StandardSourceDefinition sourceDefinition) throws IOException, JsonValidationException, ConfigNotFoundException;

  void writeSourceDefinitionAndDefaultVersion(StandardSourceDefinition sourceDefinition,
                                              ActorDefinitionVersion actorDefinitionVersion,
                                              List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException;

  void writeSourceDefinitionAndDefaultVersion(StandardSourceDefinition sourceDefinition, ActorDefinitionVersion actorDefinitionVersion)
      throws IOException;

  void writeCustomSourceDefinitionAndDefaultVersion(StandardSourceDefinition sourceDefinition,
                                                    ActorDefinitionVersion defaultVersion,
                                                    UUID scopeId,
                                                    io.airbyte.config.ScopeType scopeType)
      throws IOException;

  Stream<SourceConnection> listSourceQuery(Optional<UUID> configId) throws IOException;

  SourceConnection getSourceConnection(UUID sourceId) throws JsonValidationException, ConfigNotFoundException, IOException;

  void writeSourceConnectionNoSecrets(SourceConnection partialSource) throws IOException;

  void writeSourceConnection(List<SourceConnection> configs);

  boolean deleteSource(UUID sourceId) throws JsonValidationException, ConfigNotFoundException, IOException;

  List<SourceConnection> listSourceConnection() throws IOException;

  List<SourceConnection> listWorkspaceSourceConnection(UUID workspaceId) throws IOException;

  List<SourceConnection> listWorkspacesSourceConnections(ResourcesQueryPaginated resourcesQueryPaginated) throws IOException;

  List<SourceConnection> listSourcesForDefinition(UUID definitionId) throws IOException;

  List<SourceAndDefinition> getSourceAndDefinitionsFromSourceIds(List<UUID> sourceIds) throws IOException;

}
