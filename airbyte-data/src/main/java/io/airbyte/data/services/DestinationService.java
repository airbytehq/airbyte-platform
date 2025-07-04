/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.data.ConfigNotFoundException;
import io.airbyte.data.services.shared.DestinationAndDefinition;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * This service is used to interact with destinations.
 */
public interface DestinationService {

  StandardDestinationDefinition getStandardDestinationDefinition(UUID destinationDefinitionId)
      throws JsonValidationException, IOException, ConfigNotFoundException;

  StandardDestinationDefinition getStandardDestinationDefinition(UUID destinationDefinitionId, boolean includeTombstone)
      throws JsonValidationException, IOException, ConfigNotFoundException;

  StandardDestinationDefinition getDestinationDefinitionFromDestination(UUID destinationId);

  Boolean isDestinationActive(UUID destinationId) throws IOException;

  StandardDestinationDefinition getDestinationDefinitionFromConnection(UUID connectionId);

  List<StandardDestinationDefinition> listStandardDestinationDefinitions(boolean includeTombstone) throws IOException;

  List<StandardDestinationDefinition> listPublicDestinationDefinitions(boolean includeTombstone) throws IOException;

  List<StandardDestinationDefinition> listGrantedDestinationDefinitions(UUID workspaceId, boolean includeTombstones) throws IOException;

  List<Map.Entry<StandardDestinationDefinition, Boolean>> listGrantableDestinationDefinitions(UUID workspaceId, boolean includeTombstones)
      throws IOException;

  void updateStandardDestinationDefinition(StandardDestinationDefinition destinationDefinition)
      throws IOException, JsonValidationException, ConfigNotFoundException;

  DestinationConnection getDestinationConnection(UUID destinationId) throws JsonValidationException, IOException, ConfigNotFoundException;

  Optional<DestinationConnection> getDestinationConnectionIfExists(UUID destinationId);

  void writeDestinationConnectionNoSecrets(DestinationConnection partialDestination) throws IOException;

  List<DestinationConnection> listDestinationConnection() throws IOException;

  List<DestinationConnection> listWorkspaceDestinationConnection(UUID workspaceId) throws IOException;

  List<DestinationConnection> listWorkspacesDestinationConnections(ResourcesQueryPaginated resourcesQueryPaginated) throws IOException;

  List<DestinationConnection> listDestinationsForDefinition(UUID definitionId) throws IOException;

  List<DestinationAndDefinition> getDestinationAndDefinitionsFromDestinationIds(List<UUID> destinationIds) throws IOException;

  void writeCustomConnectorMetadata(final StandardDestinationDefinition destinationDefinition,
                                    final ActorDefinitionVersion defaultVersion,
                                    final UUID scopeId,
                                    final io.airbyte.config.ScopeType scopeType)
      throws IOException;

  void writeConnectorMetadata(final StandardDestinationDefinition destinationDefinition,
                              final ActorDefinitionVersion actorDefinitionVersion,
                              final List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException;

  List<DestinationConnection> listDestinationsWithIds(final List<UUID> destinationIds) throws IOException;

  void tombstoneDestination(
                            final String name,
                            final UUID workspaceId,
                            final UUID destinationId)
      throws ConfigNotFoundException, JsonValidationException, IOException;

}
