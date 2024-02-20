/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package controllers

import authorization.AirbyteApiAuthorizationHelper
import authorization.Scope
import io.airbyte.airbyte_api.model.generated.ConnectionCreateRequest
import io.airbyte.airbyte_api.model.generated.ConnectionPatchRequest
import io.airbyte.airbyte_api.model.generated.ConnectionResponse
import io.airbyte.airbyte_api.model.generated.DestinationResponse
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.server.apiTracking.TrackingHelper
import io.airbyte.api.server.constants.CONNECTIONS_PATH
import io.airbyte.api.server.constants.CONNECTIONS_WITH_ID_PATH
import io.airbyte.api.server.constants.DELETE
import io.airbyte.api.server.constants.GET
import io.airbyte.api.server.constants.POST
import io.airbyte.api.server.constants.PUT
import io.airbyte.api.server.controllers.interfaces.ConnectionsApi
import io.airbyte.api.server.helpers.AirbyteCatalogHelper
import io.airbyte.api.server.helpers.getLocalUserInfoIfNull
import io.airbyte.api.server.services.ConnectionService
import io.airbyte.api.server.services.DestinationService
import io.airbyte.api.server.services.SourceService
import io.airbyte.commons.server.support.CurrentUserService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.Objects
import java.util.UUID
import javax.ws.rs.Path
import javax.ws.rs.core.Response

@Controller(CONNECTIONS_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class ConnectionsController(
  private val connectionService: ConnectionService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val trackingHelper: TrackingHelper,
  private val airbyteApiAuthorizationHelper: AirbyteApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : ConnectionsApi {
  override fun createConnection(
    connectionCreateRequest: ConnectionCreateRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(connectionCreateRequest.destinationId.toString()),
      Scope.DESTINATION,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    val validUserInfo: String? = getLocalUserInfoIfNull(userInfo)

    trackingHelper.callWithTracker({
      AirbyteCatalogHelper.validateCronConfiguration(
        connectionCreateRequest.schedule,
      )
    }, CONNECTIONS_PATH, POST, userId)

    // get destination response to retrieve workspace id as well as input for destination sync modes
    val destinationResponse: DestinationResponse =
      trackingHelper.callWithTracker(
        { destinationService.getDestination(connectionCreateRequest.destinationId, validUserInfo) },
        CONNECTIONS_PATH,
        POST,
        userId,
      ) as DestinationResponse

    // get source schema for catalog id and airbyte catalog
    val schemaResponse: SourceDiscoverSchemaRead =
      trackingHelper.callWithTracker(
        { sourceService.getSourceSchema(connectionCreateRequest.sourceId, false, validUserInfo) },
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
        Objects.requireNonNull<AirbyteCatalog?>(airbyteCatalogFromDiscoverSchema),
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
        val schemaConfig = validStreamAndConfig.config

        val validDestinationSyncModes =
          trackingHelper.callWithTracker(
            { destinationService.getDestinationSyncModes(destinationResponse, validUserInfo) },
            CONNECTIONS_PATH,
            POST,
            userId,
          ) as List<DestinationSyncMode>

        // set user configs
        trackingHelper.callWithTracker(
          {
            AirbyteCatalogHelper.setAndValidateStreamConfig(
              streamConfiguration,
              validDestinationSyncModes,
              schemaStream!!,
              schemaConfig!!,
            )
          },
          CONNECTIONS_PATH,
          POST,
          userId,
        )
        configuredCatalog!!.addStreamsItem(validStreamAndConfig)
      }
    } else {
      // no user supplied stream configs, return all streams with full refresh overwrite
      configuredCatalog = airbyteCatalogFromDiscoverSchema
      AirbyteCatalogHelper.setAllStreamsFullRefreshOverwrite(configuredCatalog!!)
    }

    val finalConfiguredCatalog = configuredCatalog
    val connectionResponse: Any =
      trackingHelper.callWithTracker({
        connectionService.createConnection(
          connectionCreateRequest,
          catalogId!!,
          finalConfiguredCatalog!!,
          destinationResponse.workspaceId,
          validUserInfo,
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
  override fun deleteConnection(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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
            getLocalUserInfoIfNull(userInfo),
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
  override fun getConnection(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(connectionId.toString()),
      Scope.CONNECTION,
      userId,
      PermissionType.WORKSPACE_READER,
    )

    val connectionResponse: Any =
      trackingHelper.callWithTracker({
        connectionService.getConnection(
          connectionId,
          getLocalUserInfoIfNull(userInfo),
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

  override fun listConnections(
    workspaceIds: List<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
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
          getLocalUserInfoIfNull(userInfo),
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
  override fun patchConnection(
    connectionId: UUID,
    connectionPatchRequest: ConnectionPatchRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(connectionId.toString()),
      Scope.CONNECTION,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )
    val validUserInfo: String? = getLocalUserInfoIfNull(userInfo)

    // validate cron timing configurations
    trackingHelper.callWithTracker(
      {
        AirbyteCatalogHelper.validateCronConfiguration(
          connectionPatchRequest.schedule,
        )
      },
      CONNECTIONS_WITH_ID_PATH,
      PUT,
      userId,
    )

    val currentConnection: ConnectionResponse =
      trackingHelper.callWithTracker(
        { connectionService.getConnection(connectionId, validUserInfo) },
        CONNECTIONS_WITH_ID_PATH,
        PUT,
        userId,
      ) as ConnectionResponse

    // get destination response to retrieve workspace id as well as input for destination sync modes
    val destinationResponse: DestinationResponse =
      trackingHelper.callWithTracker(
        { destinationService.getDestination(currentConnection.destinationId, validUserInfo) },
        CONNECTIONS_WITH_ID_PATH,
        PUT,
        userId,
      ) as DestinationResponse

    // get source schema for catalog id and airbyte catalog
    val schemaResponse =
      trackingHelper.callWithTracker(
        { sourceService.getSourceSchema(currentConnection.sourceId, false, validUserInfo) },
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
        Objects.requireNonNull<AirbyteCatalog?>(airbyteCatalogFromDiscoverSchema),
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
        val schemaConfig = validStreamAndConfig.config

        val validDestinationSyncModes =
          trackingHelper.callWithTracker(
            { destinationService.getDestinationSyncModes(destinationResponse, validUserInfo) },
            CONNECTIONS_PATH,
            POST,
            userId,
          ) as List<DestinationSyncMode>

        // set user configs
        trackingHelper.callWithTracker(
          {
            AirbyteCatalogHelper.setAndValidateStreamConfig(
              streamConfiguration,
              validDestinationSyncModes,
              schemaStream!!,
              schemaConfig!!,
            )
          },
          CONNECTIONS_PATH,
          POST,
          userId,
        )
        configuredCatalog!!.addStreamsItem(validStreamAndConfig)
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
            validUserInfo,
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
