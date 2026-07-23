/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
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
  private val metricClient: MetricClient,
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
      log.error(e) { "Failed to get connection information for connection id: $connectionId" }
    } catch (e: IOException) {
      log.error(e) { "Failed to get connection information for connection id: $connectionId" }
    } catch (e: ConfigNotFoundException) {
      log.error(e) { "Failed to get connection information for connection id: $connectionId" }
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
   * Emits a metric when organization ID cannot be fetched.
   *
   * @param source Full source model
   * @return ActorContext
   */
  fun fromSource(source: SourceConnection): ActorContext {
    var organizationId: UUID? = null
    try {
      val workspace = workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, false)
      organizationId = workspace.organizationId

      if (organizationId == null) {
        log.warn { "Organization ID is null for workspace ${source.workspaceId}, source ${source.sourceId}" }
        metricClient.count(
          OssMetricsRegistry.MISSING_ORGANIZATION_ID,
          1L,
          MetricAttribute(MetricTags.CONNECTOR_TYPE, "source"),
          MetricAttribute(MetricTags.WORKSPACE_ID, source.workspaceId.toString()),
          MetricAttribute(MetricTags.SOURCE_ID, source.sourceId.toString()),
        )
      }
    } catch (e: Exception) {
      handleOrgIdException(e, "source", source.workspaceId, source.sourceId, "source id: ${source.sourceId}")
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
   * Emits a metric when organization ID cannot be fetched.
   *
   * @param destination Full destination model
   * @return ActorContext
   */
  fun fromDestination(destination: DestinationConnection): ActorContext {
    var organizationId: UUID? = null
    try {
      val workspace = workspaceService.getStandardWorkspaceNoSecrets(destination.workspaceId, false)
      organizationId = workspace.organizationId

      if (organizationId == null) {
        log.warn { "Organization ID is null for workspace ${destination.workspaceId}, destination ${destination.destinationId}" }
        metricClient.count(
          OssMetricsRegistry.MISSING_ORGANIZATION_ID,
          1L,
          MetricAttribute(MetricTags.CONNECTOR_TYPE, "destination"),
          MetricAttribute(MetricTags.WORKSPACE_ID, destination.workspaceId.toString()),
          MetricAttribute(MetricTags.DESTINATION_ID, destination.destinationId.toString()),
        )
      }
    } catch (e: Exception) {
      handleOrgIdException(e, "destination", destination.workspaceId, destination.destinationId, "destination id: ${destination.destinationId}")
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
      val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)
      organizationId = workspace.organizationId

      if (organizationId == null) {
        log.warn { "Organization ID is null for workspace $workspaceId" }
        val connectorType = actorType?.toString()?.lowercase() ?: MetricTags.UNKNOWN
        metricClient.count(
          OssMetricsRegistry.MISSING_ORGANIZATION_ID,
          1L,
          MetricAttribute(MetricTags.CONNECTOR_TYPE, connectorType),
          MetricAttribute(MetricTags.WORKSPACE_ID, workspaceId.toString()),
        )
      }
    } catch (e: Exception) {
      val connectorType = actorType?.toString()?.lowercase() ?: MetricTags.UNKNOWN
      handleOrgIdException(e, connectorType, workspaceId, null, "workspace id: $workspaceId")
    }
    return ActorContext()
      .withActorDefinitionId(actorDefinitionId)
      .withWorkspaceId(workspaceId)
      .withOrganizationId(organizationId)
      .withActorType(actorType)
  }

  private fun handleOrgIdException(
    e: Exception,
    actorType: String,
    workspaceId: UUID,
    actorId: UUID?,
    logContext: String,
  ) {
    when (e) {
      is ConfigNotFoundException, is IOException, is JsonValidationException -> {
        log.error(e) { "Failed to get organization id for $logContext" }
        val attributes =
          mutableListOf(
            MetricAttribute(MetricTags.CONNECTOR_TYPE, actorType),
            MetricAttribute(MetricTags.WORKSPACE_ID, workspaceId.toString()),
          )
        if (actorId != null) {
          val actorIdTag = if (actorType == "source") MetricTags.SOURCE_ID else MetricTags.DESTINATION_ID
          attributes.add(MetricAttribute(actorIdTag, actorId.toString()))
        }
        metricClient.count(OssMetricsRegistry.MISSING_ORGANIZATION_ID, 1L, *attributes.toTypedArray())
      }
      else -> throw e
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
