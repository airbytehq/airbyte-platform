/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.controllers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import io.airbyte.airbyte_api.model.generated.ConnectionSyncModeEnum
import io.airbyte.airbyte_api.model.generated.StreamProperties
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.api.server.apiTracking.TrackingHelper
import io.airbyte.api.server.constants.GET
import io.airbyte.api.server.constants.STREAMS_PATH
import io.airbyte.api.server.controllers.interfaces.StreamsApi
import io.airbyte.api.server.helpers.getLocalUserInfoIfNull
import io.airbyte.api.server.problems.UnexpectedProblem
import io.airbyte.api.server.services.DestinationService
import io.airbyte.api.server.services.SourceService
import io.airbyte.api.server.services.UserService
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import jakarta.ws.rs.core.Response
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.Objects
import java.util.UUID

@Controller(STREAMS_PATH)
class StreamsController(
  private val userService: UserService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val trackingHelper: TrackingHelper,
) : StreamsApi {
  companion object {
    private val log: org.slf4j.Logger? = LoggerFactory.getLogger(StreamsController::class.java)
  }

  override fun getStreamProperties(
    sourceId: UUID,
    destinationId: UUID,
    ignoreCache: Boolean?,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)
    val httpResponse =
      trackingHelper.callWithTracker(
        {
          sourceService.getSourceSchema(
            sourceId,
            ignoreCache!!,
            authorization,
            getLocalUserInfoIfNull(userInfo),
          )
        },
        STREAMS_PATH,
        GET,
        userId,
      )
    val destinationSyncModes =
      trackingHelper.callWithTracker(
        {
          destinationService.getDestinationSyncModes(
            destinationId,
            authorization,
            getLocalUserInfoIfNull(userInfo),
          )
        },
        STREAMS_PATH,
        GET,
        userId,
      )
    val streamList =
      httpResponse.catalog!!.streams.stream()
        .map { obj: AirbyteStreamAndConfiguration -> obj.stream }
        .toList()
    val listOfStreamProperties: MutableList<KStreamProperties> = emptyList<KStreamProperties>().toMutableList()
    for (airbyteStream in streamList) {
      val sourceSyncModes = airbyteStream!!.supportedSyncModes!!
      val streamProperties =
        KStreamProperties(
          streamName = airbyteStream.name,
          syncModes = getValidSyncModes(sourceSyncModes = sourceSyncModes, destinationSyncModes = destinationSyncModes).toMutableList(),
          defaultCursorField = airbyteStream.defaultCursorField?.toMutableList(),
          sourceDefinedPrimaryKey = airbyteStream.sourceDefinedPrimaryKey?.toMutableList(),
          sourceDefinedCursorField = airbyteStream.sourceDefinedCursor != null && airbyteStream.sourceDefinedCursor!!,
          propertyFields = getStreamFields(airbyteStream.jsonSchema).toMutableList(),
        )
      listOfStreamProperties.add(streamProperties)
    }
    trackingHelper.trackSuccess(
      STREAMS_PATH,
      GET,
      userId,
    )
    return Response
      .status(
        HttpStatus.OK.code,
      )
      .entity(listOfStreamProperties)
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
        yamlMapper.readTree<JsonNode>(connectorSchema!!.traverse())
      } catch (e: IOException) {
        log?.error("Error getting stream fields from schema", e)
        throw UnexpectedProblem(HttpStatus.INTERNAL_SERVER_ERROR)
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
  ): List<ConnectionSyncModeEnum> {
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

/**
 * Copy of the [StreamProperties] generated class to overcome issues with KSP stub
 * generation.
 */
class KStreamProperties(
  val streamName: String? = null,
  val syncModes: MutableList<ConnectionSyncModeEnum>? = mutableListOf(),
  val defaultCursorField: MutableList<String>? = mutableListOf(),
  val sourceDefinedCursorField: Boolean? = false,
  val sourceDefinedPrimaryKey: MutableList<List<String>>? = mutableListOf(),
  val propertyFields: MutableList<List<String>>? = mutableListOf(),
) {
  override fun equals(o: Any?): Boolean {
    if (this === o) {
      return true
    }
    if (o == null || javaClass != o.javaClass) {
      return false
    }
    val streamProperties = o as KStreamProperties
    return this.streamName == streamProperties.streamName && (this.syncModes == streamProperties.syncModes) &&
      (this.defaultCursorField == streamProperties.defaultCursorField) &&
      (this.sourceDefinedCursorField == streamProperties.sourceDefinedCursorField) &&
      (this.sourceDefinedPrimaryKey == streamProperties.sourceDefinedPrimaryKey) &&
      (this.propertyFields == streamProperties.propertyFields)
  }

  override fun hashCode(): Int {
    return Objects.hash(streamName, syncModes, defaultCursorField, sourceDefinedCursorField, sourceDefinedPrimaryKey, propertyFields)
  }

  override fun toString(): String {
    val sb = StringBuilder()
    sb.append("class StreamProperties {\n")

    sb.append("    streamName: ").append(toIndentedString(streamName)).append("\n")
    sb.append("    syncModes: ").append(toIndentedString(syncModes)).append("\n")
    sb.append("    defaultCursorField: ").append(toIndentedString(defaultCursorField)).append("\n")
    sb.append("    sourceDefinedCursorField: ").append(toIndentedString(sourceDefinedCursorField)).append("\n")
    sb.append("    sourceDefinedPrimaryKey: ").append(toIndentedString(sourceDefinedPrimaryKey)).append("\n")
    sb.append("    propertyFields: ").append(toIndentedString(propertyFields)).append("\n")
    sb.append("}")
    return sb.toString()
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private fun toIndentedString(o: Any?): String {
    if (o == null) {
      return "null"
    }
    return o.toString().replace("\n", "\n    ")
  }
}
