/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.apis.PublicConnectionsApi
import io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest
import io.airbyte.publicApi.server.generated.models.ConnectionPatchRequest
import io.airbyte.publicApi.server.generated.models.ConnectionResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.CONNECTIONS_PATH
import io.airbyte.server.apis.publicapi.constants.CONNECTIONS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.constants.PUT
import io.airbyte.server.apis.publicapi.helpers.AirbyteCatalogHelper
import io.airbyte.server.apis.publicapi.services.ConnectionService
import io.airbyte.server.apis.publicapi.services.DestinationService
import io.airbyte.server.apis.publicapi.services.SourceService
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.validation.Valid
import jakarta.validation.constraints.NotNull
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class ConnectionsController(
  private val connectionService: ConnectionService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val trackingHelper: TrackingHelper,
  private val roleResolver: RoleResolver,
  private val currentUserService: CurrentUserService,
) : PublicConnectionsApi {
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateConnection(connectionCreateRequest: ConnectionCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId

    val validConnectionCreateRequest =
      trackingHelper.callWithTracker({
        connectionCreateRequest.schedule?.let { AirbyteCatalogHelper.validateCronConfiguration(it) }
        ConnectionCreateRequest(
          name = connectionCreateRequest.name,
          dataResidency = connectionCreateRequest.dataResidency,
          configurations = connectionCreateRequest.configurations,
          namespaceDefinition = connectionCreateRequest.namespaceDefinition,
          status = connectionCreateRequest.status,
          namespaceFormat = connectionCreateRequest.namespaceFormat,
          schedule = AirbyteCatalogHelper.normalizeCronExpression(connectionCreateRequest.schedule),
          prefix = connectionCreateRequest.prefix,
          nonBreakingSchemaUpdatesBehavior = connectionCreateRequest.nonBreakingSchemaUpdatesBehavior,
          destinationId = connectionCreateRequest.destinationId,
          sourceId = connectionCreateRequest.sourceId,
          tags = connectionCreateRequest.tags,
        )
      }, CONNECTIONS_PATH, POST, userId)

    // get destination response to retrieve workspace id as well as input for destination sync modes
    val destinationRead: DestinationRead =
      trackingHelper.callWithTracker(
        { destinationService.getDestinationRead(validConnectionCreateRequest.destinationId) },
        CONNECTIONS_PATH,
        POST,
        userId,
      )

    // get source schema for catalog id and airbyte catalog
    val schemaResponse: SourceDiscoverSchemaRead =
      trackingHelper.callWithTracker(
        { sourceService.getSourceSchema(validConnectionCreateRequest.sourceId, false) },
        CONNECTIONS_PATH,
        POST,
        userId,
      )
    val catalogId = schemaResponse.catalogId

    val airbyteCatalogFromDiscoverSchema = schemaResponse.catalog

    val validDestinationSyncModes =
      trackingHelper.callWithTracker(
        { destinationService.getDestinationSyncModes(destinationRead) },
        CONNECTIONS_PATH,
        POST,
        userId,
      ) as List<DestinationSyncMode>

    // refer to documentation to understand what we need to do for the catalog
    // https://docs.airbyte.com/understanding-airbyte/airbyte-protocol/#catalog
    var configuredCatalog = AirbyteCatalog()

    if (AirbyteCatalogHelper.hasStreamConfigurations(validConnectionCreateRequest.configurations)) {
      trackingHelper.callWithTracker(
        {
          configuredCatalog.streams =
            AirbyteCatalogHelper.getValidConfiguredStreams(
              airbyteCatalogFromDiscoverSchema,
              validConnectionCreateRequest.configurations!!,
              validDestinationSyncModes,
            )
        },
        CONNECTIONS_PATH,
        POST,
        userId,
      )
    } else {
      // no user supplied stream configs, return all streams with full refresh overwrite
      configuredCatalog = AirbyteCatalogHelper.updateAllStreamsFullRefreshOverwrite(airbyteCatalogFromDiscoverSchema!!)
    }

    // Create connection with updated config.
    val finalConfiguredCatalog = configuredCatalog
    val connectionResponse: Any =
      trackingHelper.callWithTracker({
        connectionService.createConnection(
          validConnectionCreateRequest,
          catalogId!!,
          finalConfiguredCatalog,
          destinationRead.workspaceId,
        )
      }, CONNECTIONS_PATH, POST, userId)!!
    trackingHelper.trackSuccess(
      CONNECTIONS_PATH,
      POST,
      userId,
      destinationRead.workspaceId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(connectionResponse)
      .build()
  }

  @Path("$CONNECTIONS_PATH/{connectionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteConnection(connectionId: String): Response {
    val userId: UUID = currentUserService.currentUser.userId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.CONNECTION_ID, connectionId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    val connectionResponse: Any =
      trackingHelper.callWithTracker(
        {
          connectionService.deleteConnection(
            UUID.fromString(connectionId),
          )
        },
        CONNECTIONS_WITH_ID_PATH,
        DELETE,
        userId,
      )!!
    trackingHelper.trackSuccess(
      CONNECTIONS_WITH_ID_PATH,
      DELETE,
      userId,
    )
    return Response
      .status(Response.Status.NO_CONTENT.statusCode)
      .entity(connectionResponse)
      .build()
  }

  @Path("$CONNECTIONS_PATH/{connectionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetConnection(connectionId: String): Response {
    val userId: UUID = currentUserService.currentUser.userId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.CONNECTION_ID, connectionId)
      .requireRole(AuthRoleConstants.WORKSPACE_READER)

    val connectionResponse: Any =
      trackingHelper.callWithTracker({
        connectionService.getConnection(
          UUID.fromString(connectionId),
        )
      }, CONNECTIONS_PATH, GET, userId)!!
    trackingHelper.trackSuccess(
      CONNECTIONS_WITH_ID_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(connectionResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun listConnections(
    workspaceIds: List<UUID>?,
    tagIds: List<UUID>?,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    // If workspace IDs were given, then verify the user has access to those workspaces.
    // If none were given, then the ConnectionService will determine the workspaces for the current user.
    if (!workspaceIds.isNullOrEmpty()) {
      roleResolver
        .Request()
        .withCurrentUser()
        .withWorkspaces(workspaceIds)
        .requireRole(AuthRoleConstants.WORKSPACE_READER)
    }

    val safeWorkspaceIds = workspaceIds ?: emptyList()
    val safeTagIds = tagIds ?: emptyList()

    val connections =
      trackingHelper.callWithTracker({
        connectionService.listConnectionsForWorkspaces(
          safeWorkspaceIds,
          safeTagIds,
          limit,
          offset,
          includeDeleted,
        )
      }, CONNECTIONS_PATH, GET, userId)!!
    trackingHelper.trackSuccess(
      CONNECTIONS_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(connections)
      .build()
  }

  @Patch
  @Path("$CONNECTIONS_PATH/{connectionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun patchConnection(
    @PathParam(value = "connectionId") connectionId: String,
    @Valid @Body @NotNull connectionPatchRequest: ConnectionPatchRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.CONNECTION_ID, connectionId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    // validate cron timing configurations
    val validConnectionPatchRequest =
      trackingHelper.callWithTracker(
        {
          connectionPatchRequest.schedule?.let { AirbyteCatalogHelper.validateCronConfiguration(it) }
          ConnectionPatchRequest(
            name = connectionPatchRequest.name,
            dataResidency = connectionPatchRequest.dataResidency,
            configurations = connectionPatchRequest.configurations,
            namespaceDefinition = connectionPatchRequest.namespaceDefinition,
            status = connectionPatchRequest.status,
            namespaceFormat = connectionPatchRequest.namespaceFormat,
            schedule = AirbyteCatalogHelper.normalizeCronExpression(connectionPatchRequest.schedule),
            prefix = connectionPatchRequest.prefix,
            nonBreakingSchemaUpdatesBehavior = connectionPatchRequest.nonBreakingSchemaUpdatesBehavior,
            tags = connectionPatchRequest.tags,
          )
        },
        CONNECTIONS_WITH_ID_PATH,
        PUT,
        userId,
      )

    val currentConnection: ConnectionResponse =
      trackingHelper.callWithTracker(
        { connectionService.getConnection(UUID.fromString(connectionId)) },
        CONNECTIONS_WITH_ID_PATH,
        PUT,
        userId,
      ) as ConnectionResponse

    // get destination response to retrieve workspace id as well as input for destination sync modes
    val destinationRead: DestinationRead =
      trackingHelper.callWithTracker(
        { destinationService.getDestinationRead(UUID.fromString(currentConnection.destinationId)) },
        CONNECTIONS_WITH_ID_PATH,
        PUT,
        userId,
      )

    // refer to documentation to understand what we need to do for the catalog
    // https://docs.airbyte.com/understanding-airbyte/airbyte-protocol/#catalog
    var newConfiguredCatalog: AirbyteCatalog? = null
    var catalogId: UUID? = null

    if (AirbyteCatalogHelper.hasStreamConfigurations(validConnectionPatchRequest.configurations)) {
      // get source schema for catalog id and airbyte catalog
      val schemaResponse =
        trackingHelper.callWithTracker(
          { sourceService.getSourceSchema(UUID.fromString(currentConnection.sourceId), false) },
          CONNECTIONS_WITH_ID_PATH,
          PUT,
          userId,
        )
      catalogId = schemaResponse.catalogId

      val airbyteCatalogFromDiscoverSchema = schemaResponse.catalog

      val validDestinationSyncModes =
        trackingHelper.callWithTracker(
          { destinationService.getDestinationSyncModes(destinationRead) },
          CONNECTIONS_PATH,
          POST,
          userId,
        ) as List<DestinationSyncMode>

      newConfiguredCatalog = AirbyteCatalog()
      trackingHelper.callWithTracker(
        {
          newConfiguredCatalog.streams =
            AirbyteCatalogHelper.getValidConfiguredStreams(
              airbyteCatalogFromDiscoverSchema,
              validConnectionPatchRequest.configurations!!,
              validDestinationSyncModes,
            )
        },
        CONNECTIONS_PATH,
        POST,
        userId,
      )
    }

    val connectionResponse: Any =
      trackingHelper.callWithTracker(
        {
          connectionService.updateConnection(
            UUID.fromString(connectionId),
            validConnectionPatchRequest,
            catalogId,
            newConfiguredCatalog,
            destinationRead.workspaceId,
          )
        },
        CONNECTIONS_WITH_ID_PATH,
        PUT,
        userId,
      )!!

    trackingHelper.trackSuccess(
      CONNECTIONS_WITH_ID_PATH,
      PUT,
      userId,
      destinationRead.workspaceId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(connectionResponse)
      .build()
  }
}
