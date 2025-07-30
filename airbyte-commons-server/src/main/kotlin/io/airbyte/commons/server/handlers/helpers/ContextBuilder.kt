/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.config.ActorContext
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectionContext
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

/**
 * Intended to be used by the server to build context objects so that temporal workflows/activities
 * have access to relevant IDs.
 */
@Singleton
class ContextBuilder(
  private val workspaceService: WorkspaceService,
  private val destinationService: DestinationService,
  private val connectionService: ConnectionService,
  private val sourceService: SourceService,
) {
  /**
   * Returns a ConnectionContext using best-effort at fetching IDs. When data can't be fetched, we
   * shouldn't fail here. The worker should determine if it's missing crucial data and fail itself.
   *
   * @param connectionId connection ID
   * @return ConnectionContext
   */
  fun fromConnectionId(connectionId: UUID): ConnectionContext {
    var connection: StandardSync? = null
    var workspace: StandardWorkspace? = null
    var destination: DestinationConnection? = null
    var source: SourceConnection? = null
    try {
      connection = connectionService.getStandardSync(connectionId)
      source = sourceService.getSourceConnection(connection.sourceId)
      destination = destinationService.getDestinationConnection(connection.destinationId)
      workspace = workspaceService.getStandardWorkspaceNoSecrets(destination.workspaceId, false)
    } catch (e: JsonValidationException) {
      log.error("Failed to get connection information for connection id: {}", connectionId, e)
    } catch (e: IOException) {
      log.error("Failed to get connection information for connection id: {}", connectionId, e)
    } catch (e: ConfigNotFoundException) {
      log.error("Failed to get connection information for connection id: {}", connectionId, e)
    }

    val context = ConnectionContext()
    if (connection != null) {
      context
        .withSourceId(connection.sourceId)
        .withDestinationId(connection.destinationId)
        .withConnectionId(connection.connectionId)
    }

    if (workspace != null) {
      context
        .withWorkspaceId(workspace.workspaceId)
        .withOrganizationId(workspace.organizationId)
    }

    if (destination != null) {
      context.destinationDefinitionId = destination.destinationDefinitionId
    }

    if (source != null) {
      context.sourceDefinitionId = source.sourceDefinitionId
    }

    return context
  }

  /**
   * Returns an ActorContext using best-effort at fetching IDs. When data can't be fetched, we
   * shouldn't fail here. The worker should determine if it's missing crucial data and fail itself.
   *
   * @param source Full source model
   * @return ActorContext
   */
  fun fromSource(source: SourceConnection): ActorContext {
    var organizationId: UUID? = null
    try {
      organizationId = workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, false).organizationId
    } catch (e: ConfigNotFoundException) {
      log.error("Failed to get organization id for source id: {}", source.sourceId, e)
    } catch (e: IOException) {
      log.error("Failed to get organization id for source id: {}", source.sourceId, e)
    } catch (e: JsonValidationException) {
      log.error("Failed to get organization id for source id: {}", source.sourceId, e)
    }
    return ActorContext()
      .withActorId(source.sourceId)
      .withActorDefinitionId(source.sourceDefinitionId)
      .withWorkspaceId(source.workspaceId)
      .withOrganizationId(organizationId)
      .withActorType(ActorType.SOURCE)
  }

  /**
   * Returns an ActorContext using best-effort at fetching IDs. When data can't be fetched, we
   * shouldn't fail here. The worker should determine if it's missing crucial data and fail itself.
   *
   * @param destination Full destination model
   * @return ActorContext
   */
  fun fromDestination(destination: DestinationConnection): ActorContext {
    var organizationId: UUID? = null
    try {
      organizationId = workspaceService.getStandardWorkspaceNoSecrets(destination.workspaceId, false).organizationId
    } catch (e: ConfigNotFoundException) {
      log.error("Failed to get organization id for destination id: {}", destination.destinationId, e)
    } catch (e: IOException) {
      log.error("Failed to get organization id for destination id: {}", destination.destinationId, e)
    } catch (e: JsonValidationException) {
      log.error("Failed to get organization id for destination id: {}", destination.destinationId, e)
    }
    return ActorContext()
      .withActorId(destination.destinationId)
      .withActorDefinitionId(destination.destinationDefinitionId)
      .withWorkspaceId(destination.workspaceId)
      .withOrganizationId(organizationId)
      .withActorType(ActorType.DESTINATION)
  }

  fun fromActorDefinitionId(
    actorDefinitionId: UUID?,
    actorType: ActorType?,
    workspaceId: UUID,
  ): ActorContext {
    var organizationId: UUID? = null
    try {
      organizationId = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false).organizationId
    } catch (e: ConfigNotFoundException) {
      log.error("Failed to get organization id for workspace id: {}", workspaceId, e)
    } catch (e: IOException) {
      log.error("Failed to get organization id for workspace id: {}", workspaceId, e)
    } catch (e: JsonValidationException) {
      log.error("Failed to get organization id for workspace id: {}", workspaceId, e)
    }
    return ActorContext()
      .withActorDefinitionId(actorDefinitionId)
      .withWorkspaceId(workspaceId)
      .withOrganizationId(organizationId)
      .withActorType(actorType)
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
