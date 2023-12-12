/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.controllers

import io.airbyte.airbyte_api.generated.ConnectionsApi
import io.airbyte.airbyte_api.model.generated.ConnectionCreateRequest
import io.airbyte.airbyte_api.model.generated.ConnectionPatchRequest
import io.airbyte.airbyte_api.model.generated.ConnectionResponse
import io.airbyte.airbyte_api.model.generated.DestinationResponse
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.server.apiTracking.TrackingHelper
import io.airbyte.api.server.constants.CONNECTIONS_PATH
import io.airbyte.api.server.constants.CONNECTIONS_WITH_ID_PATH
import io.airbyte.api.server.constants.DELETE
import io.airbyte.api.server.constants.GET
import io.airbyte.api.server.constants.POST
import io.airbyte.api.server.constants.PUT
import io.airbyte.api.server.helpers.AirbyteCatalogHelper
import io.airbyte.api.server.helpers.getLocalUserInfoIfNull
import io.airbyte.api.server.services.ConnectionService
import io.airbyte.api.server.services.DestinationService
import io.airbyte.api.server.services.SourceService
import io.airbyte.api.server.services.UserService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import java.util.Objects
import java.util.UUID
import javax.ws.rs.core.Response

@Controller(CONNECTIONS_PATH)
open class ConnectionsController(
  private val connectionService: ConnectionService,
  private val userService: UserService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val trackingHelper: TrackingHelper,
) : ConnectionsApi {
  override fun createConnection(
    connectionCreateRequest: ConnectionCreateRequest?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)
    val validUserInfo: String? = getLocalUserInfoIfNull(userInfo)

    trackingHelper.callWithTracker({
      AirbyteCatalogHelper.validateCronConfiguration(
        connectionCreateRequest!!.schedule,
      )
    }, CONNECTIONS_PATH, POST, userId)

    // get destination response to retrieve workspace id as well as input for destination sync modes
    val destinationResponse: DestinationResponse =
      trackingHelper.callWithTracker(
        { destinationService.getDestination(connectionCreateRequest!!.destinationId, authorization, validUserInfo) },
        CONNECTIONS_PATH,
        POST,
        userId,
      ) as DestinationResponse

    // get source schema for catalog id and airbyte catalog
    val schemaResponse: SourceDiscoverSchemaRead =
      trackingHelper.callWithTracker(
        { sourceService.getSourceSchema(connectionCreateRequest!!.sourceId, false, authorization, validUserInfo) },
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
    if (AirbyteCatalogHelper.hasStreamConfigurations(connectionCreateRequest!!.configurations)) {
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
            { destinationService.getDestinationSyncModes(destinationResponse, authorization, validUserInfo) },
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
          authorization,
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

  override fun deleteConnection(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val connectionResponse: Any =
      trackingHelper.callWithTracker(
        {
          connectionService.deleteConnection(
            connectionId,
            authorization,
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

  override fun getConnection(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val connectionResponse: Any =
      trackingHelper.callWithTracker({
        connectionService.getConnection(
          connectionId,
          authorization,
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
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val safeWorkspaceIds = workspaceIds ?: emptyList()
    val connections =
      trackingHelper.callWithTracker({
        connectionService.listConnectionsForWorkspaces(
          safeWorkspaceIds,
          limit!!,
          offset!!,
          includeDeleted!!,
          authorization,
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
  override fun patchConnection(
    connectionId: UUID,
    connectionPatchRequest: ConnectionPatchRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)
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
        { connectionService.getConnection(connectionId, authorization, validUserInfo) },
        CONNECTIONS_WITH_ID_PATH,
        PUT,
        userId,
      ) as ConnectionResponse

    // get destination response to retrieve workspace id as well as input for destination sync modes
    val destinationResponse: DestinationResponse =
      trackingHelper.callWithTracker(
        { destinationService.getDestination(currentConnection.destinationId, authorization, validUserInfo) },
        CONNECTIONS_WITH_ID_PATH,
        PUT,
        userId,
      ) as DestinationResponse

    // get source schema for catalog id and airbyte catalog
    val schemaResponse =
      trackingHelper.callWithTracker(
        { sourceService.getSourceSchema(currentConnection.sourceId, false, authorization, validUserInfo) },
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
            { destinationService.getDestinationSyncModes(destinationResponse, authorization, validUserInfo) },
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
            authorization,
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
