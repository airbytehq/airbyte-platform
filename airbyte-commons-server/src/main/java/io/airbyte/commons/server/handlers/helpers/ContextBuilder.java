/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.config.ActorContext;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectionContext;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Intended to be used by the server to build context objects so that temporal workflows/activities
 * have access to relevant IDs.
 */
@Slf4j
public class ContextBuilder {

  private final WorkspaceService workspaceService;
  private final DestinationService destinationService;
  private final ConnectionService connectionService;

  public ContextBuilder(final WorkspaceService workspaceService,
                        final DestinationService destinationService,
                        final ConnectionService connectionService) {

    this.workspaceService = workspaceService;
    this.destinationService = destinationService;
    this.connectionService = connectionService;
  }

  /**
   * Returns a ConnectionContext using best-effort at fetching IDs. When data can't be fetched, we
   * shouldn't fail here. The worker should determine if it's missing crucial data and fail itself.
   *
   * @param connectionId connection ID
   * @return ConnectionContext
   */
  public ConnectionContext fromConnectionId(final UUID connectionId) {
    StandardSync connection = null;
    StandardWorkspace workspace = null;
    try {
      connection = connectionService.getStandardSync(connectionId);
      final DestinationConnection destination = destinationService.getDestinationConnection(connection.getDestinationId());
      workspace = workspaceService.getStandardWorkspaceNoSecrets(destination.getWorkspaceId(), false);
    } catch (final JsonValidationException | IOException | ConfigNotFoundException e) {
      log.error("Failed to get connection information for connection id: {}", connectionId, e);
    }

    final ConnectionContext context = new ConnectionContext();
    if (connection != null) {
      context.withSourceId(connection.getSourceId())
          .withDestinationId(connection.getDestinationId())
          .withConnectionId(connection.getConnectionId());
    }

    if (workspace != null) {
      context.withWorkspaceId(workspace.getWorkspaceId())
          .withOrganizationId(workspace.getOrganizationId());
    }

    return context;
  }

  /**
   * Returns an ActorContext using best-effort at fetching IDs. When data can't be fetched, we
   * shouldn't fail here. The worker should determine if it's missing crucial data and fail itself.
   *
   * @param source Full source model
   * @return ActorContext
   */
  public ActorContext fromSource(final SourceConnection source) {
    UUID organizationId = null;
    try {
      organizationId = workspaceService.getStandardWorkspaceNoSecrets(source.getWorkspaceId(), false).getOrganizationId();
    } catch (final ConfigNotFoundException | IOException | JsonValidationException e) {
      log.error("Failed to get organization id for source id: {}", source.getSourceId(), e);
    }
    return new ActorContext()
        .withActorId(source.getSourceId())
        .withActorDefinitionId(source.getSourceDefinitionId())
        .withWorkspaceId(source.getWorkspaceId())
        .withOrganizationId(organizationId)
        .withActorType(ActorType.SOURCE);
  }

  /**
   * Returns an ActorContext using best-effort at fetching IDs. When data can't be fetched, we
   * shouldn't fail here. The worker should determine if it's missing crucial data and fail itself.
   *
   * @param destination Full destination model
   * @return ActorContext
   */
  public ActorContext fromDestination(final DestinationConnection destination) {
    UUID organizationId = null;
    try {
      organizationId = workspaceService.getStandardWorkspaceNoSecrets(destination.getWorkspaceId(), false).getOrganizationId();
    } catch (final ConfigNotFoundException | IOException | JsonValidationException e) {
      log.error("Failed to get organization id for destination id: {}", destination.getDestinationId(), e);
    }
    return new ActorContext()
        .withActorId(destination.getDestinationId())
        .withActorDefinitionId(destination.getDestinationDefinitionId())
        .withWorkspaceId(destination.getWorkspaceId())
        .withOrganizationId(organizationId)
        .withActorType(ActorType.DESTINATION);
  }

}
