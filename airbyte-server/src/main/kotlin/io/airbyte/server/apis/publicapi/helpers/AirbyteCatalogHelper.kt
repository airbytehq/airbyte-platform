/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import com.cronutils.model.Cron
import com.cronutils.model.CronType.QUARTZ
import com.cronutils.model.definition.CronDefinition
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.parser.CronParser
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.helpers.CatalogConfigDiffHelper
import io.airbyte.publicApi.server.generated.models.AirbyteApiConnectionSchedule
import io.airbyte.publicApi.server.generated.models.ConfiguredStreamMapper
import io.airbyte.publicApi.server.generated.models.ConnectionSyncModeEnum
import io.airbyte.publicApi.server.generated.models.ScheduleTypeEnum
import io.airbyte.publicApi.server.generated.models.SelectedFieldInfo
import io.airbyte.publicApi.server.generated.models.StreamConfiguration
import io.airbyte.publicApi.server.generated.models.StreamConfigurations
import io.airbyte.publicApi.server.generated.models.StreamMapperType
import io.airbyte.server.apis.publicapi.mappers.ConnectionReadMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.validation.Valid
import java.io.IOException

private val log = KotlinLogging.logger {}

/**
 * Does everything necessary to both build and validate the AirbyteCatalog.
 */
object AirbyteCatalogHelper {
  private val cronDefinition: CronDefinition = CronDefinitionBuilder.instanceDefinitionFor(QUARTZ)
  private val parser: CronParser = CronParser(cronDefinition)
  private const val MAX_LENGTH_OF_CRON = 7

  /**
   * Check whether stream configurations exist.
   *
   * @param streamConfigurations StreamConfigurations from connection create/update request
   * @return true if they exist, false if they don't
   */
  fun hasStreamConfigurations(streamConfigurations: StreamConfigurations?): Boolean = !streamConfigurations?.streams.isNullOrEmpty()

  /**
   * Just set a config to be full refresh overwrite.
   *
   * @param config config to be set
   */
  fun updateConfigDefaultFullRefreshOverwrite(config: AirbyteStreamConfiguration?): AirbyteStreamConfiguration {
    val updatedStreamConfiguration = AirbyteStreamConfiguration()
    config?.let {
      updatedStreamConfiguration.aliasName = it.aliasName
      updatedStreamConfiguration.cursorField = it.cursorField
      updatedStreamConfiguration.fieldSelectionEnabled = it.fieldSelectionEnabled
      updatedStreamConfiguration.selected = it.selected
      updatedStreamConfiguration.includeFiles = it.includeFiles
      updatedStreamConfiguration.selectedFields = it.selectedFields
      updatedStreamConfiguration.suggested = it.suggested
    }
    updatedStreamConfiguration.destinationSyncMode = DestinationSyncMode.OVERWRITE
    updatedStreamConfiguration.syncMode = SyncMode.FULL_REFRESH
    return updatedStreamConfiguration
  }

  /**
   * Given an airbyte catalog, set all streams to be full refresh overwrite.
   *
   * @param airbyteCatalog The catalog to be modified
   */
  fun updateAllStreamsFullRefreshOverwrite(airbyteCatalog: AirbyteCatalog): AirbyteCatalog {
    val updatedAirbyteCatalog = AirbyteCatalog()
    updatedAirbyteCatalog.streams =
      airbyteCatalog.streams
        .map {
          AirbyteStreamAndConfiguration().apply {
            config = updateConfigDefaultFullRefreshOverwrite(it.config)
            stream = it.stream
          }
        }.toList()

    return updatedAirbyteCatalog
  }

  /**
   * Validate field selection for a single stream.
   *
   * @param streamConfiguration The configuration input of a specific stream provided by the caller.
   * @param sourceStream The immutable schema defined by the source
   */
  @VisibleForTesting
  fun validateFieldSelection(
    streamConfiguration: StreamConfiguration,
    sourceStream: AirbyteStream,
  ) {
    if (streamConfiguration.selectedFields == null) {
      log.debug { "Selected fields not provided. Bypass validation." }
      return
    }

    val allTopLevelStreamFields = CatalogConfigDiffHelper.getStreamTopLevelFields(sourceStream.jsonSchema).toSet()
    if (streamConfiguration.selectedFields!!.isEmpty()) {
      // User puts an empty list of selected fields to sync, which is a bad request.
      throw BadRequestProblem(
        ProblemMessageData().message(
          "No fields selected for stream ${sourceStream.name}. The list of valid field names includes: $allTopLevelStreamFields.",
        ),
      )
    }
    // Validate input selected fields.
    val allSelectedFields =
      streamConfiguration.selectedFields!!.map {
        if (it.fieldPath.isNullOrEmpty()) {
          throw BadRequestProblem(
            ProblemMessageData().message(
              "Selected field path cannot be empty for stream:  ${sourceStream.name}.",
            ),
          )
        }
        if (it.fieldPath!!.size > 1) {
          // We do not support nested field selection. Only top level properties can be selected.
          throw BadRequestProblem(
            ProblemMessageData().message(
              "Nested field selection not supported for stream ${sourceStream.name}.",
            ),
          )
        }
        it.fieldPath!!.first()
      }
    // 1. Avoid duplicate fields selection.
    val allSelectedFieldsSet = allSelectedFields.toSet()
    if (allSelectedFields.size != allSelectedFieldsSet.size) {
      throw BadRequestProblem(
        ProblemMessageData().message(
          "Duplicate fields selected in configuration for stream: ${sourceStream.name}.",
        ),
      )
    }
    // 2. Avoid non-existing fields selection.
    require(allSelectedFields.all { it in allTopLevelStreamFields }) {
      throw BadRequestProblem(
        ProblemMessageData().message(
          "Invalid fields selected for stream ${sourceStream.name}. The list of valid field names includes: $allTopLevelStreamFields.",
        ),
      )
    }

    val requiredPrimaryKeySyncModes =
      listOf(
        ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY,
        ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE_DEDUPED,
        ConnectionSyncModeEnum.INCREMENTAL_UPDATE,
        ConnectionSyncModeEnum.FULL_REFRESH_UPDATE,
      )
    // 3. Selected fields must contain primary key(s) in dedup and update modes.
    if (streamConfiguration.syncMode != null && requiredPrimaryKeySyncModes.contains(streamConfiguration.syncMode)) {
      val primaryKeys = selectPrimaryKey(sourceStream, streamConfiguration)
      val primaryKeyFields = primaryKeys?.mapNotNull { it.firstOrNull() } ?: emptyList()
      require(primaryKeyFields.all { it in allSelectedFieldsSet }) {
        throw BadRequestProblem(
          ProblemMessageData().message(
            "Primary key fields are not selected properly for stream: ${sourceStream.name}. " +
              "Please include primary key(s) in the configuration for this stream.",
          ),
        )
      }
    }

    // 4. Selected fields must contain the cursor field in incremental modes.
    val incrementalSyncModes: Set<ConnectionSyncModeEnum> =
      setOf(
        ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY,
        ConnectionSyncModeEnum.INCREMENTAL_APPEND,
        ConnectionSyncModeEnum.INCREMENTAL_UPDATE,
        ConnectionSyncModeEnum.INCREMENTAL_SOFT_DELETE,
      )
    if (streamConfiguration.syncMode in incrementalSyncModes) {
      val cursorField = selectCursorField(sourceStream, streamConfiguration)
      if (!cursorField.isNullOrEmpty() && !allSelectedFieldsSet.contains(cursorField.first())) {
        // first element is the top level field, and it has to be present in selected fields
        throw BadRequestProblem(
          ProblemMessageData().message(
            "Cursor field is not selected properly for stream: ${sourceStream.name}. " +
              "Please include the cursor field in selected fields for this stream.",
          ),
        )
      }
    }
  }

  /**
   * Validate cron configuration for a given connectionSchedule.
   *
   * @param connectionSchedule the schedule to validate
   * @return boolean, but mostly so we can callWithTracker.
   */
  fun validateCronConfiguration(connectionSchedule: @Valid AirbyteApiConnectionSchedule): Boolean {
    if (connectionSchedule.scheduleType === ScheduleTypeEnum.CRON) {
      if (connectionSchedule.cronExpression == null) {
        throw BadRequestProblem(
          ProblemMessageData().message("Missing cron expression in the schedule."),
        )
      }
      val cronExpression = normalizeCronExpression(connectionSchedule)?.cronExpression
      try {
        val cron: Cron = parser.parse(cronExpression)
        cron.validate()
        val cronStrings: List<String> = cron.asString().split(" ")
        // Ensure first value is not `*`, could be seconds or minutes value
        Integer.valueOf(cronStrings[0])
        if (cronStrings.size == MAX_LENGTH_OF_CRON) {
          // Ensure minutes value is not `*`
          Integer.valueOf(cronStrings[1])
        }
      } catch (e: NumberFormatException) {
        log.debug { "Invalid cron expression: $cronExpression" }
        log.debug { "NumberFormatException: $e" }
        throw BadRequestProblem(
          ProblemMessageData().message(
            "The cron expression ${connectionSchedule.cronExpression}" +
              " is not valid or is less than the one hour minimum. The seconds and minutes values cannot be `*`.",
          ),
        )
      } catch (e: IllegalArgumentException) {
        log.debug { "Invalid cron expression: $cronExpression" }
        log.debug { "IllegalArgumentException: $e" }
        throw BadRequestProblem(
          ProblemMessageData().message(
            "The cron expression ${connectionSchedule.cronExpression} is not valid. Error: ${e.message}" +
              ". Please check the cron expression format at https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html",
          ),
        )
      }
    }
    return true
    // validate that the cron expression is not more often than every hour due to product specs
    // check that the first seconds and hour values are not *
  }

  fun normalizeCronExpression(connectionSchedule: AirbyteApiConnectionSchedule?): AirbyteApiConnectionSchedule? =
    connectionSchedule?.let { schedule ->
      schedule.cronExpression?.let { cronExpression ->
        if (cronExpression.endsWith("UTC")) {
          AirbyteApiConnectionSchedule(
            scheduleType = connectionSchedule.scheduleType,
            cronExpression = connectionSchedule.cronExpression?.replace("UTC", "")?.trim(),
          )
        } else {
          connectionSchedule
        }
      }
    }

  /**
   * Convert proto/object from public_api model to airbyte_api model.
   * */
  private fun selectedFieldInfoConverter(publicApiSelectedFieldInfo: SelectedFieldInfo): io.airbyte.api.model.generated.SelectedFieldInfo =
    io.airbyte.api.model.generated.SelectedFieldInfo().apply {
      fieldPath = publicApiSelectedFieldInfo.fieldPath
    }

  private fun mapperTypeConverter(publicApiMapperType: StreamMapperType): io.airbyte.api.model.generated.StreamMapperType =
    io.airbyte.api.model.generated.StreamMapperType
      .fromValue(publicApiMapperType.toString())

  private fun configuredMapperConverter(publicApiMappers: ConfiguredStreamMapper): io.airbyte.api.model.generated.ConfiguredStreamMapper =
    io.airbyte.api.model.generated.ConfiguredStreamMapper().apply {
      id = publicApiMappers.id
      type = mapperTypeConverter(publicApiMappers.type)
      mapperConfiguration = publicApiMappers.mapperConfiguration
    }

  private fun selectCursorField(
    airbyteStream: AirbyteStream,
    streamConfiguration: StreamConfiguration,
  ): List<String>? =
    if (airbyteStream.sourceDefinedCursor != null && airbyteStream.sourceDefinedCursor!!) {
      airbyteStream.defaultCursorField
    } else if (streamConfiguration.cursorField != null && streamConfiguration.cursorField!!.isNotEmpty()) {
      streamConfiguration.cursorField
    } else {
      airbyteStream.defaultCursorField
    }

  private fun selectPrimaryKey(
    airbyteStream: AirbyteStream,
    streamConfiguration: StreamConfiguration,
  ): List<List<String>>? =
    (airbyteStream.sourceDefinedPrimaryKey ?: emptyList()).ifEmpty {
      streamConfiguration.primaryKey
    }

  /**
   * Validates a stream's configurations and sets those configurations in the
   * `AirbyteStreamConfiguration` object. Logic comes from
   * https://docs.airbyte.com/understanding-airbyte/airbyte-protocol/#configuredairbytestream.
   *
   * @param streamConfiguration The configuration input of a specific stream provided by the caller.
   * @param validDestinationSyncModes All the valid destination sync modes for a destination
   * @param airbyteStream The immutable schema defined by the source
   * @return True if no exceptions. Needed so it can be used inside TrackingHelper.callWithTracker
   */
  fun validateStreamConfig(
    streamConfiguration: StreamConfiguration,
    validDestinationSyncModes: List<DestinationSyncMode?>,
    airbyteStream: AirbyteStream,
  ): Boolean {
    // 1. Validate field selection
    validateFieldSelection(streamConfiguration, airbyteStream)

    // 2. validate that sync and destination modes are valid
    // Not passing in a sync mode gets mapped to FULL_REFRESH_OVERWRITE
    if (streamConfiguration.syncMode == null || streamConfiguration.syncMode == ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE) {
      return true
    }
    val validCombinedSyncModes: Set<ConnectionSyncModeEnum> = validCombinedSyncModes(airbyteStream.supportedSyncModes, validDestinationSyncModes)
    if (!validCombinedSyncModes.contains(streamConfiguration.syncMode)) {
      throw BadRequestProblem(
        ProblemMessageData().message(
          "Cannot set sync mode to ${streamConfiguration.syncMode} for stream ${streamConfiguration.name}. " +
            "Valid sync modes are: $validCombinedSyncModes",
        ),
      )
    }

    when (streamConfiguration.syncMode) {
      ConnectionSyncModeEnum.INCREMENTAL_APPEND -> {
        validateCursorField(streamConfiguration.cursorField, airbyteStream)
      }

      ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY -> {
        validateCursorField(streamConfiguration.cursorField, airbyteStream)
        validatePrimaryKey(streamConfiguration.primaryKey, airbyteStream)
      }

      ConnectionSyncModeEnum.FULL_REFRESH_UPDATE, ConnectionSyncModeEnum.INCREMENTAL_UPDATE -> {
        validatePrimaryKey(streamConfiguration.primaryKey, airbyteStream)
      }

      else -> {}
    }
    return true
  }

  private fun validateCursorField(
    cursorField: List<String>?,
    airbyteStream: AirbyteStream,
  ) {
    if (airbyteStream.sourceDefinedCursor != null && airbyteStream.sourceDefinedCursor!!) {
      if (!cursorField.isNullOrEmpty()) {
        // if cursor given is not empty and is NOT the same as the default, throw error
        if (java.util.Set.copyOf(cursorField) != java.util.Set.copyOf(airbyteStream.defaultCursorField)) {
          throw BadRequestProblem(
            ProblemMessageData().message(
              "Cursor Field " + cursorField + " is already defined by source for stream: " + airbyteStream.name +
                ". Do not include a cursor field configuration for this stream.",
            ),
          )
        }
      }
    } else {
      if (!cursorField.isNullOrEmpty()) {
        // validate cursor field
        val validCursorFields: List<List<String>> = getStreamFields(airbyteStream.jsonSchema ?: Jsons.emptyObject())
        if (!validCursorFields.contains(cursorField)) {
          throw BadRequestProblem(
            ProblemMessageData().message(
              "Invalid cursor field for stream: ${airbyteStream.name}. The list of valid cursor fields include: $validCursorFields.",
            ),
          )
        }
      } else {
        // no default or given cursor field
        if (airbyteStream.defaultCursorField == null || airbyteStream.defaultCursorField!!.isEmpty()) {
          throw BadRequestProblem(
            ProblemMessageData().message(
              "No default cursor field for stream: ${airbyteStream.name}. Please include a cursor field configuration for this stream.",
            ),
          )
        }
      }
    }
  }

  private fun validatePrimaryKey(
    primaryKey: List<List<String>>?,
    airbyteStream: AirbyteStream,
  ) {
    // Validate that if a source defined primary key exists, that's the one we use.
    // Currently, UI only supports this and there's likely assumptions baked into the platform that mean this needs to be true.
    val sourceDefinedPrimaryKeyExists = !airbyteStream.sourceDefinedPrimaryKey.isNullOrEmpty()
    val configuredPrimaryKeyExists = !primaryKey.isNullOrEmpty()

    if (sourceDefinedPrimaryKeyExists && configuredPrimaryKeyExists) {
      if (airbyteStream.sourceDefinedPrimaryKey != primaryKey) {
        throw BadRequestProblem(
          ProblemMessageData().message(
            "Primary key for stream: ${airbyteStream.name} is already pre-defined. " +
              "Please remove the primaryKey or provide the value as ${airbyteStream.sourceDefinedPrimaryKey}.",
          ),
        )
      }
    }

    // Ensure that we've passed at least some kind of primary key
    val noPrimaryKey = !configuredPrimaryKeyExists && !sourceDefinedPrimaryKeyExists
    if (noPrimaryKey) {
      throw BadRequestProblem(
        ProblemMessageData().message(
          "No default primary key for stream: ${airbyteStream.name}. Please include a primary key configuration for this stream.",
        ),
      )
    }

    // Validate the actual key passed in
    val validPrimaryKey: List<List<String>> = getStreamFields(airbyteStream.jsonSchema!!)

    primaryKey?.let { primaryKey ->
      for (singlePrimaryKey in primaryKey) {
        if (!validPrimaryKey.contains(singlePrimaryKey)) { // todo double check if the .contains() for list of strings works as intended
          throw BadRequestProblem(
            ProblemMessageData().message(
              "Invalid primary key for stream: ${airbyteStream.name}. The list of valid primary keys fields: $validPrimaryKey.",
            ),
          )
        }
        if (singlePrimaryKey.distinct() != singlePrimaryKey) {
          throw BadRequestProblem(
            ProblemMessageData().message(
              "Duplicate primary key detected for stream: ${airbyteStream.name}, " +
                "please don't provide the same column more than once. Key: $primaryKey",
            ),
          )
        }
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
    val validCombinedSyncModes: MutableSet<ConnectionSyncModeEnum> = HashSet()
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
  private fun getStreamFields(connectorSchema: JsonNode): List<List<String>> {
    val yamlMapper = ObjectMapper(YAMLFactory())
    val streamFields: MutableList<List<String>> = ArrayList()
    val spec: JsonNode =
      try {
        yamlMapper.readTree(connectorSchema.traverse())
      } catch (e: IOException) {
        log.error(e) { "Error getting stream fields from schema" }
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
            val streamFieldPath: MutableList<String> = ArrayList(listOf(propertyName))
            streamFieldPath.addAll(entry)
            streamFields.add(streamFieldPath)
          }
        }
      }
    }
    return streamFields.toList()
  }

  /**
   * Converts an StreamConfiguration from a user to an object ready to pass into a configured catalog.
   *
   * @param sourceStreamAndConfiguration source schema stream and configuration
   * @param streamConfiguration The user's passed-in stream config
   */
  private fun toAirbyteStreamConfiguration(
    sourceStreamAndConfiguration: AirbyteStreamAndConfiguration,
    streamConfiguration: StreamConfiguration,
  ): AirbyteStreamConfiguration {
    val sourceSchemaStreamConfiguration = sourceStreamAndConfiguration.config
    val sourceSchemaStream = sourceStreamAndConfiguration.stream
    return AirbyteStreamConfiguration().apply {
      // Set stream config as selected
      selected = true
      aliasName = sourceSchemaStreamConfiguration.aliasName
      fieldSelectionEnabled = sourceSchemaStreamConfiguration.fieldSelectionEnabled
      selectedFields = sourceSchemaStreamConfiguration.selectedFields

      includeFiles = sourceSchemaStreamConfiguration.includeFiles
      if (streamConfiguration.includeFiles != null) {
        includeFiles = streamConfiguration.includeFiles
      }

      if (streamConfiguration.selectedFields != null) {
        // Override and update
        fieldSelectionEnabled = true
        selectedFields = streamConfiguration.selectedFields?.map { selectedFieldInfoConverter(it) }
      }
      suggested = sourceSchemaStreamConfiguration.suggested

      // Set namespace from StreamConfiguration if provided, otherwise keep existing value
      namespace = sourceSchemaStreamConfiguration.namespace
      if (streamConfiguration.namespace != null) {
        namespace = streamConfiguration.namespace
      }

      destinationObjectName = streamConfiguration.destinationObjectName ?: sourceSchemaStreamConfiguration.destinationObjectName

      mappers = sourceSchemaStreamConfiguration.mappers
      if (streamConfiguration.mappers != null) {
        mappers = streamConfiguration.mappers!!.map { configuredMapperConverter(it) }
      }

      if (streamConfiguration.syncMode == null) {
        syncMode = SyncMode.FULL_REFRESH
        destinationSyncMode = DestinationSyncMode.OVERWRITE
        cursorField = sourceSchemaStreamConfiguration.cursorField
        primaryKey = sourceSchemaStreamConfiguration.primaryKey
      } else {
        when (streamConfiguration.syncMode) {
          ConnectionSyncModeEnum.FULL_REFRESH_APPEND -> {
            syncMode = SyncMode.FULL_REFRESH
            destinationSyncMode = DestinationSyncMode.APPEND
            cursorField = sourceSchemaStreamConfiguration.cursorField
            primaryKey = sourceSchemaStreamConfiguration.primaryKey
          }

          ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE_DEDUPED -> {
            syncMode = SyncMode.FULL_REFRESH
            destinationSyncMode = DestinationSyncMode.OVERWRITE_DEDUP
            cursorField = sourceSchemaStreamConfiguration.cursorField
            primaryKey = selectPrimaryKey(sourceSchemaStream, streamConfiguration)
          }

          ConnectionSyncModeEnum.INCREMENTAL_APPEND -> {
            syncMode(SyncMode.INCREMENTAL)
            destinationSyncMode(DestinationSyncMode.APPEND)
            cursorField(selectCursorField(sourceSchemaStream, streamConfiguration))
            primaryKey(selectPrimaryKey(sourceSchemaStream, streamConfiguration))
          }

          ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY -> {
            syncMode = SyncMode.INCREMENTAL
            destinationSyncMode = DestinationSyncMode.APPEND_DEDUP
            cursorField = selectCursorField(sourceSchemaStream, streamConfiguration)
            primaryKey = selectPrimaryKey(sourceSchemaStream, streamConfiguration)
          }

          ConnectionSyncModeEnum.INCREMENTAL_UPDATE -> {
            syncMode = SyncMode.INCREMENTAL
            destinationSyncMode = DestinationSyncMode.UPDATE
            cursorField = selectCursorField(sourceSchemaStream, streamConfiguration)
            primaryKey = selectPrimaryKey(sourceSchemaStream, streamConfiguration)
          }

          ConnectionSyncModeEnum.FULL_REFRESH_UPDATE -> {
            syncMode = SyncMode.FULL_REFRESH
            destinationSyncMode = DestinationSyncMode.UPDATE
            cursorField = sourceSchemaStreamConfiguration.cursorField
            primaryKey = selectPrimaryKey(sourceSchemaStream, streamConfiguration)
          }

          ConnectionSyncModeEnum.INCREMENTAL_SOFT_DELETE -> {
            syncMode = SyncMode.INCREMENTAL
            destinationSyncMode = DestinationSyncMode.SOFT_DELETE
            cursorField = selectCursorField(sourceSchemaStream, streamConfiguration)
            primaryKey = selectPrimaryKey(sourceSchemaStream, streamConfiguration)
          }

          ConnectionSyncModeEnum.FULL_REFRESH_SOFT_DELETE -> {
            syncMode = SyncMode.FULL_REFRESH
            destinationSyncMode = DestinationSyncMode.SOFT_DELETE
            cursorField = sourceSchemaStreamConfiguration.cursorField
            primaryKey = selectPrimaryKey(sourceSchemaStream, streamConfiguration)
          }

          else -> {
            syncMode = SyncMode.FULL_REFRESH
            destinationSyncMode = DestinationSyncMode.OVERWRITE
            cursorField = sourceSchemaStreamConfiguration.cursorField
            primaryKey = sourceSchemaStreamConfiguration.primaryKey
          }
        }
      }
    }
  }

  /**
   * @param referenceCatalog the catalog we got from discoverSourceSchema
   * @param streamConfigurations the stream configurations passed in by the user
   */
  fun getValidConfiguredStreams(
    referenceCatalog: AirbyteCatalog,
    streamConfigurations: StreamConfigurations,
    validDestinationSyncModes: List<DestinationSyncMode>,
  ): List<AirbyteStreamAndConfiguration> {
    val alreadyConfiguredStreams = mutableSetOf<Pair<String, String?>>()

    return streamConfigurations.streams.orEmpty().map {
      val referenceStreamAndConfig = findAndValidateCatalogStreamAndConfiguration(referenceCatalog, it.name, it.namespace)
      validateStreamConfig(it, validDestinationSyncModes, referenceStreamAndConfig.stream)
      // For each of our configurations, attempt to map it to a valid configured stream that we can pass to the internal handlers.
      val mappedConfig = toAirbyteStreamConfiguration(referenceStreamAndConfig, it)
      val configuredStreamAndConfig =
        AirbyteStreamAndConfiguration().apply {
          this.stream = referenceStreamAndConfig.stream
          this.config = mappedConfig
        }
      if (!alreadyConfiguredStreams.add(Pair(it.name, it.namespace))) {
        throw BadRequestProblem(
          ProblemMessageData()
            .message("Duplicate stream found in configuration for stream name [${it.name}] in namespace [${it.namespace}]."),
        )
      }
      return@map configuredStreamAndConfig
    }
  }

  /**
   * Find the reference catalog stream that matches the incoming stream/namespace.
   * Validation now happens here - if we hit an error condition, we'll throw a problem.
   */
  private fun findAndValidateCatalogStreamAndConfiguration(
    referenceCatalog: AirbyteCatalog,
    streamName: String,
    streamNamespace: String?,
  ): AirbyteStreamAndConfiguration {
    // First try to find the stream by name
    val streamNameMatches = referenceCatalog.streams.filter { it.stream.name == streamName }
    // If we have just one match, return it
    if (streamNameMatches.size == 1) {
      return streamNameMatches.first()
    }

    // If we have multiple streams,
    // check if we have any matches with stream name && namespace
    // If we have one, return it
    // If we have multiple, throw a BadRequestProblem
    // If we have none, throw a BadRequestProblem
    val streamAndConfigs = streamNameMatches.filter { it.stream.namespace == streamNamespace }
    if (streamAndConfigs.size == 1) {
      return streamAndConfigs.first()
    } else if (streamAndConfigs.size > 1) {
      throw BadRequestProblem(
        ProblemMessageData()
          .message(
            "Multiple streams found with name [$streamName] and namespace [$streamNamespace]. Please call the https://reference.airbyte.com/reference/getstreamproperties endpoint to see the valid stream name and namespace combinations.",
          ),
      )
    } else {
      // If we have no matches with stream name && namespace, throw a BadRequestProblem
      throw BadRequestProblem(
        ProblemMessageData()
          .message(
            "No streams found with name [$streamName] and namespace [$streamNamespace]. Please call the https://reference.airbyte.com/reference/getstreamproperties endpoint to see the valid stream name and namespace combinations.",
          ),
      )
    }
  }
}
