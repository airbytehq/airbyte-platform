/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.apis.PublicStreamsApi
import io.airbyte.publicApi.server.generated.models.ConnectionSyncModeEnum
import io.airbyte.publicApi.server.generated.models.StreamProperties
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.STREAMS_PATH
import io.airbyte.server.apis.publicapi.services.DestinationService
import io.airbyte.server.apis.publicapi.services.SourceService
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
class StreamsController(
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val trackingHelper: TrackingHelper,
  private val roleResolver: RoleResolver,
  private val currentUserService: CurrentUserService,
) : PublicStreamsApi {
  companion object {
    private val log: org.slf4j.Logger? = LoggerFactory.getLogger(StreamsController::class.java)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun getStreamProperties(
    sourceId: String,
    destinationId: String?,
    ignoreCache: Boolean,
  ): Response {
    // Check permission for source and destination
    val userId: UUID = currentUserService.currentUser.userId

    val authReq =
      roleResolver
        .Request()
        .withCurrentUser()
        .withRef(AuthenticationId.SOURCE_ID, sourceId)

    if (destinationId != null) {
      authReq.withRef(AuthenticationId.DESTINATION_ID_, destinationId)
    }

    authReq.requireRole(AuthRoleConstants.WORKSPACE_READER)

    val httpResponse =
      trackingHelper.callWithTracker(
        {
          sourceService.getSourceSchema(
            UUID.fromString(sourceId),
            ignoreCache,
          )
        },
        STREAMS_PATH,
        GET,
        userId,
      )
    val destinationSyncModes: List<DestinationSyncMode> =
      if (destinationId != null) {
        trackingHelper.callWithTracker(
          {
            destinationService.getDestinationSyncModes(
              UUID.fromString(destinationId),
            )
          },
          STREAMS_PATH,
          GET,
          userId,
        )
      } else {
        emptyList()
      }
    val streamList =
      httpResponse.catalog!!
        .streams
        .map { it.stream }
    val listOfStreamProperties =
      streamList.map { airbyteStream ->
        StreamProperties(
          streamName = airbyteStream.name,
          streamnamespace = airbyteStream.namespace,
          syncModes = getValidSyncModes(sourceSyncModes = airbyteStream!!.supportedSyncModes!!, destinationSyncModes = destinationSyncModes),
          defaultCursorField = airbyteStream.defaultCursorField,
          sourceDefinedPrimaryKey = airbyteStream.sourceDefinedPrimaryKey,
          sourceDefinedCursorField = airbyteStream.sourceDefinedCursor != null && airbyteStream.sourceDefinedCursor!!,
          propertyFields = getStreamFields(connectorSchema = airbyteStream.jsonSchema),
        )
      }
    trackingHelper.trackSuccess(
      STREAMS_PATH,
      GET,
      userId,
    )
    return Response
      .status(
        HttpStatus.OK.code,
      ).entity(listOfStreamProperties)
      .build()
  }

  /**
   * Parses a connectorSchema to retrieve all the possible stream fields.
   *
   * @param connectorSchema source or destination schema
   * @return A list of stream fields, which are represented as list of strings since they can be
   * nested fields.
   */
  private fun getStreamFields(connectorSchema: JsonNode?): List<List<String>> {
    val yamlMapper = ObjectMapper(YAMLFactory())
    val streamFields: MutableList<List<String>> = ArrayList()
    val spec: JsonNode =
      try {
        yamlMapper.readTree(connectorSchema!!.traverse())
      } catch (e: IOException) {
        log?.error("Error getting stream fields from schema", e)
        throw UnexpectedProblem()
      }
    val fields = spec.fields()
    while (fields.hasNext()) {
      val (key, paths) = fields.next()
      if ("properties" == key) {
        val propertyFields = paths.fields()
        while (propertyFields.hasNext()) {
          val (propertyName, nestedProperties) = propertyFields.next()
          streamFields.add(listOf(propertyName))

          // retrieve nested paths
          for (entry in getStreamFields(nestedProperties)) {
            if (entry.isEmpty()) {
              continue
            }
            val streamFieldPath: MutableList<String> = ArrayList(mutableListOf(propertyName))
            streamFieldPath.addAll(entry)
            streamFields.add(streamFieldPath)
          }
        }
      }
    }
    return streamFields
  }

  private fun getValidSyncModes(
    sourceSyncModes: List<SyncMode>,
    destinationSyncModes: List<DestinationSyncMode>,
  ): List<ConnectionSyncModeEnum>? {
    if (destinationSyncModes.isEmpty()) {
      return null
    }
    val connectionSyncModes: MutableList<ConnectionSyncModeEnum> = emptyList<ConnectionSyncModeEnum>().toMutableList()
    if (sourceSyncModes.contains(SyncMode.FULL_REFRESH)) {
      if (destinationSyncModes.contains(DestinationSyncMode.APPEND)) {
        connectionSyncModes.add(ConnectionSyncModeEnum.FULL_REFRESH_APPEND)
      }
      if (destinationSyncModes.contains(DestinationSyncMode.OVERWRITE)) {
        connectionSyncModes.add(ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE)
      }
    }
    if (sourceSyncModes.contains(SyncMode.INCREMENTAL)) {
      if (destinationSyncModes.contains(DestinationSyncMode.APPEND)) {
        connectionSyncModes.add(ConnectionSyncModeEnum.INCREMENTAL_APPEND)
      }
      if (destinationSyncModes.contains(DestinationSyncMode.APPEND_DEDUP)) {
        connectionSyncModes.add(ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY)
      }
    }
    return connectionSyncModes
  }
}
