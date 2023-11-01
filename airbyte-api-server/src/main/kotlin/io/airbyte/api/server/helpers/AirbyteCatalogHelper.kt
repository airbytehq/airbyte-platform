/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.helpers

import com.cronutils.model.Cron
import com.cronutils.model.CronType.QUARTZ
import com.cronutils.model.definition.CronDefinition
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.parser.CronParser
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import io.airbyte.airbyte_api.model.generated.ConnectionSchedule
import io.airbyte.airbyte_api.model.generated.ConnectionSyncModeEnum
import io.airbyte.airbyte_api.model.generated.ScheduleTypeEnum
import io.airbyte.airbyte_api.model.generated.StreamConfiguration
import io.airbyte.airbyte_api.model.generated.StreamConfigurations
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStream
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.api.server.mappers.ConnectionReadMapper
import io.airbyte.api.server.problems.ConnectionConfigurationProblem
import io.airbyte.api.server.problems.ConnectionConfigurationProblem.Companion.duplicateStream
import io.airbyte.api.server.problems.ConnectionConfigurationProblem.Companion.invalidStreamName
import io.airbyte.api.server.problems.UnexpectedProblem
import io.micronaut.http.HttpStatus
import org.slf4j.LoggerFactory
import java.io.IOException

/**
 * Does everything necessary to both build and validate the AirbyteCatalog.
 */
object AirbyteCatalogHelper {
  private val cronDefinition: CronDefinition = CronDefinitionBuilder.instanceDefinitionFor(QUARTZ)
  private val parser: CronParser = CronParser(cronDefinition)
  private val log = LoggerFactory.getLogger(AirbyteCatalogHelper.javaClass)
  private const val MAX_LENGTH_OF_CRON = 7

  /**
   * Check whether stream configurations exist.
   *
   * @param streamConfigurations StreamConfigurations from conneciton create/update request
   * @return true if they exist, false if they don't
   */
  fun hasStreamConfigurations(streamConfigurations: StreamConfigurations?): Boolean {
    return !streamConfigurations?.streams.isNullOrEmpty()
  }

  /**
   * Just set a config to be full refresh overwrite.
   *
   * @param config config to be set
   */
  fun setConfigDefaultFullRefreshOverwrite(config: AirbyteStreamConfiguration?) {
    config!!.syncMode = SyncMode.FULL_REFRESH
    config.destinationSyncMode = DestinationSyncMode.OVERWRITE
  }

  /**
   * Given an airbyte catalog, set all streams to be full refresh overwrite.
   *
   * @param airbyteCatalog The catalog to be modified
   */
  fun setAllStreamsFullRefreshOverwrite(airbyteCatalog: AirbyteCatalog) {
    for (schemaStreams in airbyteCatalog.streams) {
      val config = schemaStreams.config!!
      setConfigDefaultFullRefreshOverwrite(config)
    }
  }

  /**
   * Given a reference catalog and a user's passed in streamConfigurations, ensure valid streams or
   * throw a problem to be returned to the user.
   *
   * @param referenceCatalog - catalog, usually from discoverSourceSchema
   * @param streamConfigurations - configurations passed in by the user.
   * @return boolean so we can callWithTracker
   */
  fun validateStreams(
    referenceCatalog: AirbyteCatalog,
    streamConfigurations: StreamConfigurations,
  ): Boolean {
    val validStreams = getValidStreams(referenceCatalog)
    val alreadyConfiguredStreams: MutableSet<String> = HashSet()
    for (streamConfiguration in streamConfigurations.streams) {
      if (!validStreams.containsKey(streamConfiguration.name)) {
        throw invalidStreamName(validStreams.keys)
      } else if (alreadyConfiguredStreams.contains(streamConfiguration.name)) {
        throw duplicateStream(streamConfiguration.name)
      }
      alreadyConfiguredStreams.add(streamConfiguration.name)
    }
    return true
  }

  /**
   * Given an AirbyteCatalog, return a map of valid streams where key == name and value == the stream
   * config.
   *
   * @param airbyteCatalog Airbyte catalog to pull streams out of
   * @return map of stream name: stream config
   */
  fun getValidStreams(airbyteCatalog: AirbyteCatalog): Map<String, AirbyteStreamAndConfiguration> {
    val validStreams: MutableMap<String, AirbyteStreamAndConfiguration> = HashMap()
    for (schemaStream in airbyteCatalog.streams) {
      validStreams[schemaStream.stream!!.name] = schemaStream
    }
    return validStreams
  }

  /**
   * Validate cron configuration for a given connectionschedule.
   *
   * @param connectionSchedule the schedule to validate
   * @return boolean, but mostly so we can callwithTracker.
   */
  fun validateCronConfiguration(connectionSchedule: ConnectionSchedule?): Boolean {
    if (connectionSchedule != null) {
      if (connectionSchedule.scheduleType != null && connectionSchedule.scheduleType === ScheduleTypeEnum.CRON) {
        if (connectionSchedule.cronExpression == null) {
          throw ConnectionConfigurationProblem.missingCronExpression()
        }
        try {
          if (connectionSchedule.cronExpression.endsWith("UTC")) {
            connectionSchedule.cronExpression = connectionSchedule.cronExpression.replace("UTC", "").trim()
          }
          val cron: Cron = parser.parse(connectionSchedule.cronExpression)
          cron.validate()
          val cronStrings: List<String> = cron.asString().split(" ")
          // Ensure first value is not `*`, could be seconds or minutes value
          Integer.valueOf(cronStrings[0])
          if (cronStrings.size == MAX_LENGTH_OF_CRON) {
            // Ensure minutes value is not `*`
            Integer.valueOf(cronStrings[1])
          }
        } catch (e: NumberFormatException) {
          log.debug("Invalid cron expression: " + connectionSchedule.cronExpression)
          log.debug("NumberFormatException: $e")
          throw ConnectionConfigurationProblem.invalidCronExpressionUnderOneHour(connectionSchedule.cronExpression)
        } catch (e: IllegalArgumentException) {
          log.debug("Invalid cron expression: " + connectionSchedule.cronExpression)
          log.debug("IllegalArgumentException: $e")
          throw ConnectionConfigurationProblem.invalidCronExpression(connectionSchedule.cronExpression, e.message)
        }
      }
    }
    return true
    // validate that the cron expression is not more often than every hour due to product specs
    // check that the first seconds and hour values are not *
  }

  /**
   * Validates a stream's configurations and sets those configurations in the
   * `AirbyteStreamConfiguration` object. Logic comes from
   * https://docs.airbyte.com/understanding-airbyte/airbyte-protocol/#configuredairbytestream.
   *
   * @param streamConfiguration The configuration input of a specific stream provided by the caller.
   * @param validDestinationSyncModes All the valid destination sync modes for a destination
   * @param airbyteStream The immutable schema defined by the source
   * @param config The configuration of a stream consumed by the config-api
   * @return True if no exceptions. Needed so it can be used inside TrackingHelper.callWithTracker
   */
  fun setAndValidateStreamConfig(
    streamConfiguration: StreamConfiguration,
    validDestinationSyncModes: List<DestinationSyncMode>,
    airbyteStream: AirbyteStream,
    config: AirbyteStreamConfiguration,
  ): Boolean {
    // Set stream config as selected
    config.selected = true
    if (streamConfiguration.syncMode == null) {
      setConfigDefaultFullRefreshOverwrite(config)
      return true
    }

    // validate that sync and destination modes are valid
    val validCombinedSyncModes: Set<ConnectionSyncModeEnum> = validCombinedSyncModes(airbyteStream.supportedSyncModes, validDestinationSyncModes)
    if (!validCombinedSyncModes.contains(streamConfiguration.syncMode)) {
      throw ConnectionConfigurationProblem.handleSyncModeProblem(
        streamConfiguration.syncMode,
        streamConfiguration.name,
        validCombinedSyncModes,
      )
    }
    when (streamConfiguration.syncMode) {
      ConnectionSyncModeEnum.FULL_REFRESH_APPEND -> {
        config.syncMode = SyncMode.FULL_REFRESH
        config.destinationSyncMode = DestinationSyncMode.APPEND
      }

      ConnectionSyncModeEnum.INCREMENTAL_APPEND -> {
        config.syncMode = SyncMode.INCREMENTAL
        config.destinationSyncMode = DestinationSyncMode.APPEND
        setAndValidateCursorField(streamConfiguration.cursorField, airbyteStream, config)
      }

      ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY -> {
        config.syncMode = SyncMode.INCREMENTAL
        config.destinationSyncMode = DestinationSyncMode.APPEND_DEDUP
        setAndValidateCursorField(streamConfiguration.cursorField, airbyteStream, config)
        setAndValidatePrimaryKey(streamConfiguration.primaryKey, airbyteStream, config)
      }

      else -> {
        // always valid
        setConfigDefaultFullRefreshOverwrite(config)
      }
    }
    return true
  }

  private fun setAndValidateCursorField(
    cursorField: List<String>?,
    airbyteStream: AirbyteStream,
    config: AirbyteStreamConfiguration,
  ) {
    if (airbyteStream.sourceDefinedCursor != null && airbyteStream.sourceDefinedCursor!!) {
      if (!cursorField.isNullOrEmpty()) {
        // if cursor given is not empty and is NOT the same as the default, throw error
        if (java.util.Set.copyOf<String>(cursorField) != java.util.Set.copyOf<String>(airbyteStream.defaultCursorField)) {
          throw ConnectionConfigurationProblem.sourceDefinedCursorFieldProblem(airbyteStream.name, airbyteStream.defaultCursorField!!)
        }
      }
      config.cursorField = airbyteStream.defaultCursorField // this probably isn't necessary and should be already set
    } else {
      if (!cursorField.isNullOrEmpty()) {
        // validate cursor field
        val validCursorFields: List<List<String>> = getStreamFields(airbyteStream.jsonSchema!!)
        if (!validCursorFields.contains(cursorField)) {
          throw ConnectionConfigurationProblem.invalidCursorField(airbyteStream.name, validCursorFields)
        }
        config.cursorField = cursorField
      } else {
        // no default or given cursor field
        if (airbyteStream.defaultCursorField == null || airbyteStream.defaultCursorField!!.isEmpty()) {
          throw ConnectionConfigurationProblem.missingCursorField(airbyteStream.name)
        }
        config.cursorField = airbyteStream.defaultCursorField // this probably isn't necessary and should be already set
      }
    }
  }

  private fun setAndValidatePrimaryKey(
    primaryKey: List<List<String>>?,
    airbyteStream: AirbyteStream,
    config: AirbyteStreamConfiguration,
  ) {
    // if no source defined primary key
    if (airbyteStream.sourceDefinedPrimaryKey == null || airbyteStream.sourceDefinedPrimaryKey!!.isEmpty()) {
      if (!primaryKey.isNullOrEmpty()) {
        // validate primary key
        val validPrimaryKey: List<List<String>> = getStreamFields(airbyteStream.jsonSchema!!)

        // todo maybe check that they don't provide the same primary key twice?
        for (singlePrimaryKey in primaryKey) {
          if (!validPrimaryKey.contains(singlePrimaryKey)) { // todo double check if the .contains() for list of strings works as intended
            throw ConnectionConfigurationProblem.invalidPrimaryKey(airbyteStream.name, validPrimaryKey)
          }
        }
        config.primaryKey = primaryKey
      } else {
        throw ConnectionConfigurationProblem.missingPrimaryKey(airbyteStream.name)
      }
    } else {
      // source defined primary key exists
      if (!primaryKey.isNullOrEmpty()) {
        throw ConnectionConfigurationProblem.primaryKeyAlreadyDefined(airbyteStream.name)
      } else {
        config.primaryKey = airbyteStream.sourceDefinedPrimaryKey // this probably isn't necessary and should be already set
      }
    }
  }

  /**
   * Fetch a set off the valid combined sync modes given the valid source/destination sync modes.
   *
   * @param validSourceSyncModes - List of valid source sync modes
   * @param validDestinationSyncModes - list of valid destination sync modes
   * @return Set of valid ConnectionSyncModeEnum values
   */
  fun validCombinedSyncModes(
    validSourceSyncModes: List<SyncMode?>?,
    validDestinationSyncModes: List<DestinationSyncMode?>,
  ): Set<ConnectionSyncModeEnum> {
    val validCombinedSyncModes: MutableSet<ConnectionSyncModeEnum> = HashSet<ConnectionSyncModeEnum>()
    for (sourceSyncMode in validSourceSyncModes!!) {
      for (destinationSyncMode in validDestinationSyncModes) {
        val combinedSyncMode: ConnectionSyncModeEnum? =
          ConnectionReadMapper.syncModesToConnectionSyncModeEnum(sourceSyncMode, destinationSyncMode)
        // This is true when the supported sync modes include full_refresh and the destination supports
        // append_deduped
        // or when the sync modes include incremental and the destination supports overwrite
        if (combinedSyncMode != null) {
          validCombinedSyncModes.add(combinedSyncMode)
        }
      }
    }
    return validCombinedSyncModes
  }

  /**
   * Parses a connectorSchema to retrieve all the possible stream fields.
   *
   * @param connectorSchema source or destination schema
   * @return A list of stream fields, which are represented as list of strings since they can be
   * nested fields.
   */
  fun getStreamFields(connectorSchema: JsonNode): List<List<String>> {
    val yamlMapper = ObjectMapper(YAMLFactory())
    val streamFields: MutableList<List<String>> = ArrayList()
    val spec: JsonNode
    spec =
      try {
        yamlMapper.readTree<JsonNode>(connectorSchema.traverse())
      } catch (e: IOException) {
        log.error("Error getting stream fields from schema", e)
        throw UnexpectedProblem(HttpStatus.INTERNAL_SERVER_ERROR)
      }
    val fields = spec.fields()
    while (fields.hasNext()) {
      val (key, paths) = fields.next()
      if ("properties" == key) {
        val propertyFields = paths.fields()
        while (propertyFields.hasNext()) {
          val (propertyName, nestedProperties) = propertyFields.next()
          streamFields.add(java.util.List.of(propertyName))

          // retrieve nested paths
          for (entry in getStreamFields(nestedProperties)) {
            if (entry.isEmpty()) {
              continue
            }
            val streamFieldPath: MutableList<String> = ArrayList(java.util.List.of(propertyName))
            streamFieldPath.addAll(entry)
            streamFields.add(streamFieldPath)
          }
        }
      }
    }
    return streamFields.toList()
  }
}
