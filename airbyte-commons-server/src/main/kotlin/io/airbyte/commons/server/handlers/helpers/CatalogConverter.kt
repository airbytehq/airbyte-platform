/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.ConfiguredStreamMapper
import io.airbyte.api.server.generated.models.DestinationSyncMode
import io.airbyte.api.server.generated.models.SelectedFieldInfo
import io.airbyte.api.server.generated.models.StreamDescriptor
import io.airbyte.api.server.generated.models.SyncMode
import io.airbyte.commons.converters.ApiConverters.Companion.toApi
import io.airbyte.commons.converters.toApi
import io.airbyte.commons.converters.toInternal
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.text.Names
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.FieldSelectionData
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.helpers.ProtocolConverters.Companion.toInternal
import io.airbyte.config.mapper.configs.HashingMapperConfig
import io.airbyte.mappers.helpers.createHashingMapper
import io.airbyte.mappers.helpers.getHashedFieldName
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.Nullable
import jakarta.inject.Singleton
import jakarta.validation.Valid
import java.util.Optional
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

/**
 * Convert classes between io.airbyte.protocol.models and the API models.
 */
@Singleton
class CatalogConverter(
  private val fieldGenerator: FieldGenerator,
  mappersList: List<Mapper<out MapperConfig>>,
) {
  private val mappers: Map<String, Mapper<out MapperConfig>> = mappersList.associateBy { o -> o.name }

  private fun toApi(stream: AirbyteStream): io.airbyte.api.server.generated.models.AirbyteStream =
    io.airbyte.api.server.generated.models.AirbyteStream(
      name = stream.name,
      jsonSchema = stream.jsonSchema,
      supportedSyncModes = stream.supportedSyncModes.convertTo<SyncMode>(),
      sourceDefinedCursor = if (stream.sourceDefinedCursor != null) stream.sourceDefinedCursor else false,
      defaultCursorField = stream.defaultCursorField,
      sourceDefinedPrimaryKey = stream.sourceDefinedPrimaryKey,
      namespace = stream.namespace,
      isFileBased = stream.isFileBased,
      isResumable = stream.isResumable,
    )

  /**
   * Convert an internal catalog and field selection mask to an api catalog model.
   *
   * @param catalog internal catalog
   * @param fieldSelectionData field selection mask
   * @return api catalog model
   */
  fun toApi(
    catalog: ConfiguredAirbyteCatalog,
    fieldSelectionData: FieldSelectionData?,
  ): AirbyteCatalog {
    val streams =
      catalog.streams
        .stream()
        .map { configuredStream: ConfiguredAirbyteStream ->
          val streamDescriptor =
            StreamDescriptor(name = configuredStream.stream.name, namespace = configuredStream.stream.namespace)
          val fieldSelectionEnabled = getStreamHasFieldSelectionEnabled(fieldSelectionData, streamDescriptor)
          val selectedFields =
            if (fieldSelectionEnabled == true) {
              // TODO(mfsiega-airbyte): support nested fields here.
              configuredStream.stream
                .jsonSchema
                .findValue("properties")
                .fieldNames()
                .asSequence()
                .map { fieldName: String -> SelectedFieldInfo(fieldPath = listOf(fieldName)) }
                .toList()
            } else {
              listOf()
            }
          val configuration =
            AirbyteStreamConfiguration(
              syncMode = configuredStream.syncMode.convertTo<SyncMode>(),
              cursorField = configuredStream.cursorField,
              destinationSyncMode = configuredStream.destinationSyncMode.convertTo<DestinationSyncMode>(),
              primaryKey = configuredStream.primaryKey,
              aliasName = Names.toAlphanumericAndUnderscore(configuredStream.stream.name),
              selected = true,
              suggested = false,
              includeFiles = configuredStream.includeFiles,
              destinationObjectName = configuredStream.destinationObjectName,
              fieldSelectionEnabled = fieldSelectionEnabled,
              selectedFields = selectedFields,
              hashedFields =
                configuredStream.mappers
                  .stream()
                  .filter { mapper: MapperConfig -> MapperOperationName.HASHING == mapper.name() }
                  .map { configuredHashingMapper: MapperConfig -> this.toApiFieldInfo(configuredHashingMapper) }
                  .collect(Collectors.toList<@Valid SelectedFieldInfo?>()),
              mappers =
                configuredStream.mappers
                  .stream()
                  .map<ConfiguredStreamMapper> { obj -> obj.toApi() }
                  .collect(Collectors.toList<@Valid ConfiguredStreamMapper?>()),
              generationId = configuredStream.generationId,
              minimumGenerationId = configuredStream.minimumGenerationId,
              syncId = configuredStream.syncId,
            )
          AirbyteStreamAndConfiguration(stream = configuredStream.stream.toApi(), config = configuration)
        }.collect(Collectors.toList())
    return AirbyteCatalog(streams = streams)
  }

  /**
   * Convert an internal model version of the catalog into an api model of the catalog.
   *
   * @param catalog internal catalog model
   * @param sourceVersion actor definition version for the source in use
   * @return api catalog model
   */
  fun toApi(
    catalog: io.airbyte.protocol.models.v0.AirbyteCatalog,
    @Nullable sourceVersion: ActorDefinitionVersion?,
  ): AirbyteCatalog {
    val suggestedStreams: MutableList<String> = ArrayList()
    val suggestingStreams: Boolean

    // There are occasions in tests where we have not seeded the sourceVersion fully. This is to
    // prevent those tests from failing
    if (sourceVersion != null) {
      suggestingStreams = sourceVersion.suggestedStreams != null
      if (suggestingStreams) {
        suggestedStreams.addAll(sourceVersion.suggestedStreams.streams)
      }
    } else {
      suggestingStreams = false
    }

    return AirbyteCatalog(
      streams =
        catalog.streams
          .stream()
          .map { stream: AirbyteStream -> this.toApi(stream) }
          .map { s: io.airbyte.api.server.generated.models.AirbyteStream ->
            AirbyteStreamAndConfiguration(
              stream = s,
              config = generateDefaultConfiguration(s, suggestingStreams, suggestedStreams, catalog.streams.stream().count()),
            )
          }.collect(Collectors.toList<@Valid AirbyteStreamAndConfiguration?>()),
    )
  }

  private fun toConfiguredProtocol(
    stream: io.airbyte.api.server.generated.models.AirbyteStream,
    config: AirbyteStreamConfiguration,
  ): AirbyteStream {
    if (config.fieldSelectionEnabled == true) {
      // Validate the selected field paths.
      val configSelectedFields =
        config.selectedFields ?: throw JsonValidationException("Requested field selection but no selected fields provided")
      val properties = stream.jsonSchema!!.findValue("properties")
      if (properties == null || !properties.isObject) {
        throw JsonValidationException("Requested field selection but no properties node found")
      }
      for (selectedFieldInfo in configSelectedFields) {
        val fieldPath = selectedFieldInfo.fieldPath
        if (fieldPath.isNullOrEmpty()) {
          throw JsonValidationException("Selected field path cannot be empty")
        }
        if (fieldPath.size > 1) {
          // TODO(mfsiega-airbyte): support nested fields.
          throw UnsupportedOperationException("Nested field selection not supported")
        }
      }
      // Only include the selected fields.
      // NOTE: we verified above that each selected field has at least one element in the field path.
      val selectedFieldNames =
        configSelectedFields
          .map { field: SelectedFieldInfo -> field.fieldPath!![0] }
          .toSet()
      // TODO(mfsiega-airbyte): we only check the top level of the cursor/primary key fields because we
      // don't support filtering nested fields yet.
      if (config.syncMode == SyncMode.INCREMENTAL &&
        // INCREMENTAL sync mode, AND
        !config.cursorField.isNullOrEmpty() &&
        // There is a cursor configured, AND
        !selectedFieldNames.contains(config.cursorField!![0])
      ) { // The cursor isn't in the selected fields.
        throw JsonValidationException("Cursor field cannot be de-selected in INCREMENTAL syncs")
      }
      if (config.destinationSyncMode == DestinationSyncMode.APPEND_DEDUP ||
        config.destinationSyncMode == DestinationSyncMode.OVERWRITE_DEDUP
      ) {
        for (primaryKeyComponent in config.primaryKey.orEmpty()) {
          if (!selectedFieldNames.contains(primaryKeyComponent[0])) {
            throw JsonValidationException("Primary key field cannot be de-selected in DEDUP mode")
          }
        }
      }
      for (selectedFieldName in selectedFieldNames) {
        if (!properties.has(selectedFieldName)) {
          log.info { "Requested selected field $selectedFieldName not found in JSON schema" }
        }
      }
      (properties as ObjectNode).retain(selectedFieldNames)
    }
    return AirbyteStream()
      .withName(stream.name)
      .withJsonSchema(stream.jsonSchema)
      .withSupportedSyncModes(
        stream.supportedSyncModes.orEmpty().convertTo<io.airbyte.protocol.models.v0.SyncMode>(),
      ).withSourceDefinedCursor(stream.sourceDefinedCursor)
      .withDefaultCursorField(stream.defaultCursorField)
      .withIsFileBased(stream.isFileBased)
      .withSourceDefinedPrimaryKey(Optional.ofNullable(stream.sourceDefinedPrimaryKey).orElse(emptyList()))
      .withNamespace(stream.namespace)
      .withIsResumable(stream.isResumable)
  }

  private fun toConfiguredHashingMappers(
    @Nullable hashedFields: List<SelectedFieldInfo>?,
  ): List<MapperConfig> {
    if (hashedFields == null) {
      return emptyList()
    }
    return hashedFields.map<SelectedFieldInfo, HashingMapperConfig> { f: SelectedFieldInfo -> createHashingMapper(f.fieldPath!!.first()) }
  }

  fun toConfiguredMappers(
    @Nullable mapperConfigs: List<ConfiguredStreamMapper>?,
  ): List<MapperConfig> {
    if (mapperConfigs == null) {
      return emptyList()
    }

    return mapperConfigs
      .stream()
      .map { obj -> obj.toInternal() }
      .collect(Collectors.toList())
  }

  private fun toApiFieldInfo(configuredHashingMapper: MapperConfig): SelectedFieldInfo {
    require(MapperOperationName.HASHING == configuredHashingMapper.name()) { "Expected hashing mapper" }
    return SelectedFieldInfo(
      fieldPath = listOf(getHashedFieldName(configuredHashingMapper as HashingMapperConfig)),
    )
  }

  /**
   * Converts the API catalog model into a protocol catalog. Note: only streams marked as selected
   * will be returned. This is included in this converter as the API model always carries all the
   * streams it has access to and then marks the ones that should not be used as not selected, while
   * the protocol version just uses the presence of the streams as evidence that it should be
   * included.
   *
   * @param catalog api catalog
   * @return protocol catalog
   */
  fun toConfiguredInternal(catalog: AirbyteCatalog): ConfiguredAirbyteCatalog {
    val errors: MutableList<JsonValidationException> = ArrayList()
    val streams =
      catalog.streams
        .stream()
        .filter { s: AirbyteStreamAndConfiguration -> s.config!!.selected == true }
        .map<ConfiguredAirbyteStream?> { s: AirbyteStreamAndConfiguration ->
          val config = s.config!!
          try {
            val convertedStream: io.airbyte.config.AirbyteStream = toConfiguredProtocol(s.stream!!, config).toInternal()
            val builder =
              ConfiguredAirbyteStream
                .Builder()
                .stream(convertedStream)
                .syncMode(
                  config.syncMode.convertTo<io.airbyte.config.SyncMode>(),
                ).destinationSyncMode(
                  config.destinationSyncMode.convertTo<io.airbyte.config.DestinationSyncMode>(),
                ).cursorField(config.cursorField)
                .primaryKey(Optional.ofNullable(config.primaryKey).orElse(emptyList()))
                .destinationObjectName(config.destinationObjectName)
                .fields(fieldGenerator.getFieldsFromSchema(convertedStream.jsonSchema))

            if (!config.mappers.isNullOrEmpty()) {
              builder
                .mappers(toConfiguredMappers(config.mappers))
            } else {
              // TODO(pedro): `hashedFields` support should be removed once the UI is updated to use `mappers`.
              builder
                .mappers(toConfiguredHashingMappers(config.hashedFields))
            }

            config.includeFiles?.let { builder.includeFiles(it) }

            return@map builder.build()
          } catch (e: JsonValidationException) {
            log.error(e) { "Error parsing catalog: $e" }
            errors.add(e)
            return@map null
          }
        }.collect(Collectors.toList<ConfiguredAirbyteStream?>())
    if (!errors.isEmpty()) {
      throw errors[0]
    }
    return ConfiguredAirbyteCatalog()
      .withStreams(streams)
  }

  /**
   * Set the default sync modes for an un-configured stream based on the stream properties.
   *
   *
   * The logic is: - source-defined cursor and source-defined primary key -> INCREMENTAL, APPEND-DEDUP
   * - source-defined cursor only or nothing defined by the source -> FULL REFRESH, OVERWRITE -
   * source-defined cursor and full refresh not available as a sync method -> INCREMENTAL, APPEND
   *
   * @param streamToConfigure the stream for which we're picking a sync mode
   * @param config the config to copy with the chosen sync modes
   */
  fun withDefaultSyncModesForNewStream(
    streamToConfigure: io.airbyte.api.server.generated.models.AirbyteStream,
    config: AirbyteStreamConfiguration,
  ): AirbyteStreamConfiguration {
    val hasSourceDefinedCursor = streamToConfigure.sourceDefinedCursor == true
    val hasSourceDefinedPrimaryKey = !streamToConfigure.sourceDefinedPrimaryKey.isNullOrEmpty()
    val supportsFullRefresh = streamToConfigure.supportedSyncModes.orEmpty().contains(SyncMode.FULL_REFRESH)
    return if (hasSourceDefinedCursor && hasSourceDefinedPrimaryKey) { // Source-defined cursor and primary key
      config.copy(
        syncMode = SyncMode.INCREMENTAL,
        destinationSyncMode = DestinationSyncMode.APPEND_DEDUP,
        primaryKey = streamToConfigure.sourceDefinedPrimaryKey,
      )
    } else if (hasSourceDefinedCursor && supportsFullRefresh) { // Source-defined cursor but no primary key.
      // NOTE: we prefer Full Refresh | Overwrite to avoid the risk of an Incremental | Append sync
      // blowing up their destination.
      config.copy(syncMode = SyncMode.FULL_REFRESH, destinationSyncMode = DestinationSyncMode.OVERWRITE)
    } else if (hasSourceDefinedCursor) { // Source-defined cursor but no primary key *and* no full-refresh supported.
      // If *only* incremental is supported, we go with it.
      config.copy(syncMode = SyncMode.INCREMENTAL, destinationSyncMode = DestinationSyncMode.APPEND)
    } else { // No source-defined cursor at all.
      config.copy(syncMode = SyncMode.FULL_REFRESH, destinationSyncMode = DestinationSyncMode.OVERWRITE)
    }
  }

  private fun generateDefaultConfiguration(
    stream: io.airbyte.api.server.generated.models.AirbyteStream,
    suggestingStreams: Boolean,
    suggestedStreams: List<String>,
    totalStreams: Long,
  ): AirbyteStreamConfiguration {
    val onlyOneStream = totalStreams == 1L
    val isSelected = onlyOneStream || (suggestingStreams && suggestedStreams.contains(stream.name))

    // The sync modes are placeholders until withDefaultSyncModesForNewStream picks them.
    // In the case where this connection hasn't yet been configured, the suggested streams are also
    // (pre)-selected
    val result =
      AirbyteStreamConfiguration(
        aliasName = Names.toAlphanumericAndUnderscore(stream.name),
        cursorField = stream.defaultCursorField,
        syncMode = SyncMode.FULL_REFRESH,
        destinationSyncMode = DestinationSyncMode.APPEND,
        primaryKey = stream.sourceDefinedPrimaryKey,
        suggested = isSelected,
        selected = isSelected,
      )

    return withDefaultSyncModesForNewStream(stream, result)
  }

  private fun getStreamHasFieldSelectionEnabled(
    fieldSelectionData: FieldSelectionData?,
    streamDescriptor: StreamDescriptor,
  ): Boolean? {
    if (fieldSelectionData == null ||
      fieldSelectionData.additionalProperties[streamDescriptorToStringForFieldSelection(streamDescriptor)] == null
    ) {
      return false
    }

    return fieldSelectionData.additionalProperties[streamDescriptorToStringForFieldSelection(streamDescriptor)]
  }

  /**
   * Converts the API catalog model into a protocol catalog. Note: returns all streams, regardless of
   * selected status. See
   * [CatalogConverter.toConfiguredProtocol] for
   * context.
   *
   * @param catalog api catalog
   * @return protocol catalog
   */
  fun toProtocolKeepAllStreams(catalog: AirbyteCatalog): ConfiguredAirbyteCatalog {
    val allSelected = catalog.copy(streams = catalog.streams.map { it.copy(config = it.config!!.copy(selected = true)) })
    return toConfiguredInternal(allSelected)
  }

  /**
   * To convert AirbyteCatalog from APIs to model. This is to differentiate between
   * toConfiguredProtocol as the other one converts to ConfiguredAirbyteCatalog object instead.
   */
  fun toProtocol(catalog: AirbyteCatalog): io.airbyte.protocol.models.v0.AirbyteCatalog {
    val errors: MutableList<JsonValidationException> = ArrayList()

    val protoCatalog =
      io.airbyte.protocol.models.v0
        .AirbyteCatalog()
    val airbyteStream =
      catalog.streams
        .stream()
        .map<AirbyteStream?> { stream: AirbyteStreamAndConfiguration ->
          try {
            return@map toConfiguredProtocol(stream.stream!!, stream.config!!)
          } catch (e: JsonValidationException) {
            log.error(e) { "Error parsing catalog: $e" }
            errors.add(e)
            return@map null
          }
        }.collect(Collectors.toList<AirbyteStream?>())

    if (!errors.isEmpty()) {
      throw errors[0]
    }
    protoCatalog.withStreams(airbyteStream)
    return protoCatalog
  }

  /**
   * Generate the map from StreamDescriptor to indicator of whether field selection is enabled for
   * that stream.
   *
   * @param syncCatalog the catalog
   * @return the map as a FieldSelectionData object
   */
  fun getFieldSelectionData(syncCatalog: AirbyteCatalog?): FieldSelectionData? {
    if (syncCatalog == null) {
      return null
    }
    val fieldSelectionData = FieldSelectionData()
    for (streamAndConfig in syncCatalog.streams) {
      val streamDescriptor =
        StreamDescriptor(name = streamAndConfig.stream!!.name, namespace = streamAndConfig.stream!!.namespace)
      val fieldSelectionEnabled = streamAndConfig.config!!.fieldSelectionEnabled ?: false
      fieldSelectionData.setAdditionalProperty(streamDescriptorToStringForFieldSelection(streamDescriptor), fieldSelectionEnabled)
    }
    return fieldSelectionData
  }

  // Return a string representation of a stream descriptor that's convenient to use as a key for the
  // field selection data.
  private fun streamDescriptorToStringForFieldSelection(streamDescriptor: StreamDescriptor): String =
    String.format("%s/%s", streamDescriptor.namespace, streamDescriptor.name)

  /**
   * Ensure that the configured sync modes are compatible with the source and the destination.
   *
   *
   * When we discover a new stream -- either during manual or auto schema refresh -- we want to pick
   * some default sync modes. This depends both on the source-supported sync modes -- represented in
   * the discovered catalog -- and the destination-supported sync modes. The latter is tricky because
   * the place where we're generating the default configuration isn't associated with a particular
   * destination.
   *
   *
   * A longer-term fix would be to restructure how we generate this default config, but for now we use
   * this to ensure that we've chosen defaults that work for the relevant sync.
   *
   * @param streamAndConfiguration the stream and configuration to check
   * @param supportedDestinationSyncModes the sync modes supported by the destination
   * @return the stream with compatible sync modes
   */
  fun withCompatibleDestinationSyncMode(
    streamAndConfiguration: AirbyteStreamAndConfiguration,
    supportedDestinationSyncModes: List<DestinationSyncMode>,
  ): AirbyteStreamAndConfiguration {
    val config = streamAndConfiguration.config!!
    if (supportedDestinationSyncModes.contains(config.destinationSyncMode)) {
      return streamAndConfiguration
    }
    val sourceSyncModes = streamAndConfiguration.stream!!.supportedSyncModes.orEmpty()
    val sourceSupportsFullRefresh = sourceSyncModes.contains(SyncMode.FULL_REFRESH)
    val destinationSupportsOverwrite = supportedDestinationSyncModes.contains(DestinationSyncMode.OVERWRITE)
    if (sourceSupportsFullRefresh && destinationSupportsOverwrite) {
      // We prefer to fall back to Full Refresh | Overwrite if possible.
      return streamAndConfiguration.copy(
        config = config.copy(syncMode = SyncMode.FULL_REFRESH, destinationSyncMode = DestinationSyncMode.OVERWRITE),
      )
    }
    // If *that* isn't possible, we pick something that *is* supported. This isn't ideal, but we don't
    // have a clean way
    // to fail in this case today.
    val supportedSyncMode = sourceSyncModes[0]
    val supportedDestinationSyncMode = supportedDestinationSyncModes[0]
    log.warn { "Default sync modes are incompatible, so falling back to $supportedSyncMode | $supportedDestinationSyncMode" }
    return streamAndConfiguration.copy(
      config = config.copy(syncMode = supportedSyncMode, destinationSyncMode = supportedDestinationSyncMode),
    )
  }
}
