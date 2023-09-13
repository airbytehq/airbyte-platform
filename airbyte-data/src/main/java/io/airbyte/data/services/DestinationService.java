/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.data.services.shared.DestinationAndDefinition;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * This service is used to interact with destinations.
 */
public interface DestinationService {

  Stream<StandardDestinationDefinition> destDefQuery(Optional<UUID> destDefId, boolean includeTombstone) throws IOException;

  StandardDestinationDefinition getStandardDestinationDefinition(UUID destinationDefinitionId)
      throws JsonValidationException, IOException, ConfigNotFoundException;

  StandardDestinationDefinition getDestinationDefinitionFromDestination(UUID destinationId);

  StandardDestinationDefinition getDestinationDefinitionFromConnection(UUID connectionId);

  List<StandardDestinationDefinition> listStandardDestinationDefinitions(boolean includeTombstone) throws IOException;

  List<StandardDestinationDefinition> listPublicDestinationDefinitions(boolean includeTombstone) throws IOException;

  List<StandardDestinationDefinition> listGrantedDestinationDefinitions(UUID workspaceId, boolean includeTombstones) throws IOException;

  List<Map.Entry<StandardDestinationDefinition, Boolean>> listGrantableDestinationDefinitions(UUID workspaceId, boolean includeTombstones)
      throws IOException;

  void updateStandardDestinationDefinition(StandardDestinationDefinition destinationDefinition)
      throws IOException, JsonValidationException, ConfigNotFoundException;

  void writeDestinationDefinitionAndDefaultVersion(StandardDestinationDefinition destinationDefinition,
                                                   ActorDefinitionVersion actorDefinitionVersion,
                                                   List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException;

  void writeDestinationDefinitionAndDefaultVersion(StandardDestinationDefinition destinationDefinition, ActorDefinitionVersion actorDefinitionVersion)
      throws IOException;

  void writeCustomDestinationDefinitionAndDefaultVersion(StandardDestinationDefinition destinationDefinition,
                                                         ActorDefinitionVersion defaultVersion,
                                                         UUID scopeId,
                                                         io.airbyte.config.ScopeType scopeType)
      throws IOException;

  Stream<DestinationConnection> listDestinationQuery(Optional<UUID> configId);

  DestinationConnection getDestinationConnection(UUID destinationId) throws JsonValidationException, IOException, ConfigNotFoundException;

  void writeDestinationConnectionNoSecrets(DestinationConnection partialDestination) throws IOException;

  void writeDestinationConnection(List<DestinationConnection> configs);

  List<DestinationConnection> listDestinationConnection() throws IOException;

  List<DestinationConnection> listWorkspaceDestinationConnection(UUID workspaceId) throws IOException;

  List<DestinationConnection> listWorkspacesDestinationConnections(ResourcesQueryPaginated resourcesQueryPaginated) throws IOException;

  List<DestinationConnection> listDestinationsForDefinition(UUID definitionId) throws IOException;

  List<DestinationAndDefinition> getDestinationAndDefinitionsFromDestinationIds(List<UUID> destinationIds) throws IOException;

}
