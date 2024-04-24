/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.public_api.generated.PublicConnectionsApi
import io.airbyte.public_api.model.generated.ConnectionCreateRequest
import io.airbyte.public_api.model.generated.ConnectionResponse
import io.airbyte.public_api.model.generated.DestinationResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
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
import java.util.Objects
import java.util.UUID

@Controller(CONNECTIONS_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class ConnectionsController(
  private val connectionService: ConnectionService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val trackingHelper: TrackingHelper,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : PublicConnectionsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateConnection(connectionCreateRequest: ConnectionCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(connectionCreateRequest.destinationId.toString()),
      Scope.DESTINATION,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    trackingHelper.callWithTracker({
      connectionCreateRequest.schedule?.let {
        AirbyteCatalogHelper.validateCronConfiguration(it)
      }
    }, CONNECTIONS_PATH, POST, userId)

    // get destination response to retrieve workspace id as well as input for destination sync modes
    val destinationResponse: DestinationResponse =
      trackingHelper.callWithTracker(
        { destinationService.getDestination(connectionCreateRequest.destinationId) },
        CONNECTIONS_PATH,
        POST,
        userId,
      ) as DestinationResponse

    // get source schema for catalog id and airbyte catalog
    val schemaResponse: SourceDiscoverSchemaRead =
      trackingHelper.callWithTracker(
        { sourceService.getSourceSchema(connectionCreateRequest.sourceId, false) },
        CONNECTIONS_PATH,
        POST,
        userId,
      )
    val catalogId = schemaResponse.catalogId

    val airbyteCatalogFromDiscoverSchema = schemaResponse.catalog

    // refer to documentation to understand what we need to do for the catalog
    // https://docs.airbyte.com/understanding-airbyte/airbyte-protocol/#catalog
    var configuredCatalog: AirbyteCatalog? = AirbyteCatalog()

    val validStreams: Map<String, AirbyteStreamAndConfiguration> =
      AirbyteCatalogHelper.getValidStreams(
        Objects.requireNonNull(airbyteCatalogFromDiscoverSchema),
      )

    // check user configs
    if (AirbyteCatalogHelper.hasStreamConfigurations(connectionCreateRequest.configurations)) {
      // validate user inputs
      trackingHelper.callWithTracker(
        {
          AirbyteCatalogHelper.validateStreams(
            airbyteCatalogFromDiscoverSchema!!,
            connectionCreateRequest.configurations,
          )
        },
        CONNECTIONS_PATH,
        POST,
        userId,
      )

      // set user inputs
      for (streamConfiguration in connectionCreateRequest.configurations.streams) {
        val validStreamAndConfig = validStreams[streamConfiguration.name]
        val schemaStream = validStreamAndConfig!!.stream
        val updatedValidStreamAndConfig = AirbyteStreamAndConfiguration()
        updatedValidStreamAndConfig.stream = schemaStream
        updatedValidStreamAndConfig.config =
          AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
            validStreamAndConfig.config,
            schemaStream,
            streamConfiguration,
          )

        val validDestinationSyncModes =
          trackingHelper.callWithTracker(
            { destinationService.getDestinationSyncModes(destinationResponse) },
            CONNECTIONS_PATH,
            POST,
            userId,
          ) as List<DestinationSyncMode>

        // set user configs
        trackingHelper.callWithTracker(
          {
            AirbyteCatalogHelper.validateStreamConfig(
              streamConfiguration = streamConfiguration,
              validDestinationSyncModes = validDestinationSyncModes,
              airbyteStream = schemaStream,
            )
          },
          CONNECTIONS_PATH,
          POST,
          userId,
        )
        configuredCatalog!!.addStreamsItem(updatedValidStreamAndConfig)
      }
    } else {
      // no user supplied stream configs, return all streams with full refresh overwrite
      configuredCatalog = AirbyteCatalogHelper.updateAllStreamsFullRefreshOverwrite(airbyteCatalogFromDiscoverSchema!!)
    }

    val finalConfiguredCatalog = configuredCatalog
    val connectionResponse: Any =
      trackingHelper.callWithTracker({
        connectionService.createConnection(
          connectionCreateRequest,
          catalogId!!,
          finalConfiguredCatalog!!,
          destinationResponse.workspaceId,
        )
      }, CONNECTIONS_PATH, POST, userId)!!
    trackingHelper.trackSuccess(
      CONNECTIONS_PATH,
      POST,
      userId,
      destinationResponse.workspaceId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(connectionResponse)
      .build()
  }

  @Path("/{connectionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteConnection(connectionId: UUID): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(connectionId.toString()),
      Scope.CONNECTION,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    val connectionResponse: Any =
      trackingHelper.callWithTracker(
        {
          connectionService.deleteConnection(
            connectionId,
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

  @Path("/{connectionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetConnection(connectionId: UUID): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(connectionId.toString()),
      Scope.CONNECTION,
      userId,
      PermissionType.WORKSPACE_READER,
    )

    val connectionResponse: Any =
      trackingHelper.callWithTracker({
        connectionService.getConnection(
          connectionId,
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
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      workspaceIds?.map { it.toString() } ?: emptyList(),
      Scope.WORKSPACES,
      userId,
      PermissionType.WORKSPACE_READER,
    )

    val safeWorkspaceIds = workspaceIds ?: emptyList()
    val connections =
      trackingHelper.callWithTracker({
        connectionService.listConnectionsForWorkspaces(
          safeWorkspaceIds,
          limit!!,
          offset!!,
          includeDeleted!!,
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
  @Path("/{connectionId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun patchConnection(
    @PathParam(value = "connectionId") connectionId: UUID,
    @Valid @Body @NotNull connectionPatchRequest:
      @Valid @NotNull
      io.airbyte.public_api.model.generated.ConnectionPatchRequest,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(connectionId.toString()),
      Scope.CONNECTION,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    // validate cron timing configurations
    trackingHelper.callWithTracker(
      {
        connectionPatchRequest.schedule?.let {
          AirbyteCatalogHelper.validateCronConfiguration(it)
        }
      },
      CONNECTIONS_WITH_ID_PATH,
      PUT,
      userId,
    )

    val currentConnection: ConnectionResponse =
      trackingHelper.callWithTracker(
        { connectionService.getConnection(connectionId) },
        CONNECTIONS_WITH_ID_PATH,
        PUT,
        userId,
      ) as ConnectionResponse

    // get destination response to retrieve workspace id as well as input for destination sync modes
    val destinationResponse: DestinationResponse =
      trackingHelper.callWithTracker(
        { destinationService.getDestination(currentConnection.destinationId) },
        CONNECTIONS_WITH_ID_PATH,
        PUT,
        userId,
      ) as DestinationResponse

    // get source schema for catalog id and airbyte catalog
    val schemaResponse =
      trackingHelper.callWithTracker(
        { sourceService.getSourceSchema(currentConnection.sourceId, false) },
        CONNECTIONS_PATH,
        POST,
        userId,
      )
    val catalogId = schemaResponse.catalogId

    val airbyteCatalogFromDiscoverSchema = schemaResponse.catalog

    // refer to documentation to understand what we need to do for the catalog
    // https://docs.airbyte.com/understanding-airbyte/airbyte-protocol/#catalog
    var configuredCatalog: AirbyteCatalog? = AirbyteCatalog()

    val validStreams: Map<String, AirbyteStreamAndConfiguration> =
      AirbyteCatalogHelper.getValidStreams(
        Objects.requireNonNull(airbyteCatalogFromDiscoverSchema),
      )

    // check user configs
    if (AirbyteCatalogHelper.hasStreamConfigurations(connectionPatchRequest.configurations)) {
      // validate user inputs
      trackingHelper.callWithTracker(
        {
          AirbyteCatalogHelper.validateStreams(
            airbyteCatalogFromDiscoverSchema!!,
            connectionPatchRequest.configurations,
          )
        },
        CONNECTIONS_PATH,
        POST,
        userId,
      )

      // set user inputs
      for (streamConfiguration in connectionPatchRequest.configurations.streams) {
        val validStreamAndConfig = validStreams[streamConfiguration.name]
        val schemaStream = validStreamAndConfig!!.stream
        val updatedValidStreamAndConfig = AirbyteStreamAndConfiguration()
        updatedValidStreamAndConfig.stream = schemaStream
        updatedValidStreamAndConfig.config =
          AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
            validStreamAndConfig.config,
            schemaStream,
            streamConfiguration,
          )

        val validDestinationSyncModes =
          trackingHelper.callWithTracker(
            { destinationService.getDestinationSyncModes(destinationResponse) },
            CONNECTIONS_PATH,
            POST,
            userId,
          ) as List<DestinationSyncMode>

        // set user configs
        trackingHelper.callWithTracker(
          {
            AirbyteCatalogHelper.validateStreamConfig(
              streamConfiguration = streamConfiguration,
              validDestinationSyncModes = validDestinationSyncModes,
              airbyteStream = schemaStream,
            )
          },
          CONNECTIONS_PATH,
          POST,
          userId,
        )
        configuredCatalog!!.addStreamsItem(updatedValidStreamAndConfig)
      }
    } else {
      // no user supplied stream configs, return all existing streams
      configuredCatalog = null
    }

    val finalConfiguredCatalog = configuredCatalog
    val connectionResponse: Any =
      trackingHelper.callWithTracker(
        {
          connectionService.updateConnection(
            connectionId,
            connectionPatchRequest,
            catalogId!!,
            finalConfiguredCatalog,
            destinationResponse.workspaceId,
          )
        },
        CONNECTIONS_PATH,
        POST,
        userId,
      )!!

    trackingHelper.trackSuccess(
      CONNECTIONS_WITH_ID_PATH,
      PUT,
      userId,
      destinationResponse.workspaceId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(connectionResponse)
      .build()
  }
}
