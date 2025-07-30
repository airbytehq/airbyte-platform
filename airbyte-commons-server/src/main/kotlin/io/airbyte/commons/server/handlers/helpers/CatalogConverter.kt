/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.api.client.util.Preconditions
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.ConfiguredStreamMapper
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.api.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.commons.converters.ApiConverters.Companion.toApi
import io.airbyte.commons.converters.toApi
import io.airbyte.commons.converters.toInternal
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
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
import java.util.function.Consumer
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

/**
 * Convert classes between io.airbyte.protocol.models and io.airbyte.api.model.generated.
 */
@Singleton
class CatalogConverter(
  private val fieldGenerator: FieldGenerator,
  mappersList: List<Mapper<out MapperConfig>>,
) {
  private val mappers: Map<String, Mapper<out MapperConfig>> = mappersList.associateBy { o -> o.name }

  private fun toApi(stream: AirbyteStream): io.airbyte.api.model.generated.AirbyteStream =
    io.airbyte.api.model.generated
      .AirbyteStream()
      .name(stream.name)
      .jsonSchema(stream.jsonSchema)
      .supportedSyncModes(
        stream.supportedSyncModes.convertTo<SyncMode>(),
      ).sourceDefinedCursor(if (stream.sourceDefinedCursor != null) stream.sourceDefinedCursor else false)
      .defaultCursorField(stream.defaultCursorField)
      .sourceDefinedPrimaryKey(stream.sourceDefinedPrimaryKey)
      .namespace(stream.namespace)
      .isFileBased(stream.isFileBased)
      .isResumable(stream.isResumable)

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
            StreamDescriptor()
              .name(configuredStream.stream.name)
              .namespace(configuredStream.stream.namespace)
          val configuration =
            AirbyteStreamConfiguration()
              .syncMode(
                configuredStream.syncMode.convertTo<SyncMode>(),
              ).cursorField(configuredStream.cursorField)
              .destinationSyncMode(
                configuredStream.destinationSyncMode.convertTo<DestinationSyncMode>(),
              ).primaryKey(configuredStream.primaryKey)
              .aliasName(Names.toAlphanumericAndUnderscore(configuredStream.stream.name))
              .selected(true)
              .suggested(false)
              .includeFiles(configuredStream.includeFiles)
              .destinationObjectName(configuredStream.destinationObjectName)
              .fieldSelectionEnabled(getStreamHasFieldSelectionEnabled(fieldSelectionData, streamDescriptor))
              // TODO(pedro): `hashedFields` should be removed once the UI is updated to use `mappers`.
              .selectedFields(listOf<@Valid SelectedFieldInfo?>())
              .hashedFields(
                configuredStream.mappers
                  .stream()
                  .filter { mapper: MapperConfig -> MapperOperationName.HASHING == mapper.name() }
                  .map { configuredHashingMapper: MapperConfig -> this.toApiFieldInfo(configuredHashingMapper) }
                  .collect(Collectors.toList<@Valid SelectedFieldInfo?>()),
              ).mappers(
                configuredStream.mappers
                  .stream()
                  .map<ConfiguredStreamMapper> { obj -> obj.toApi() }
                  .collect(Collectors.toList<@Valid ConfiguredStreamMapper?>()),
              ).generationId(configuredStream.generationId)
              .minimumGenerationId(configuredStream.minimumGenerationId)
              .syncId(configuredStream.syncId)
          if (configuration.fieldSelectionEnabled) {
            val selectedColumns: MutableList<String> = ArrayList()
            // TODO(mfsiega-airbyte): support nested fields here.
            configuredStream.stream
              .jsonSchema
              .findValue("properties")
              .fieldNames()
              .forEachRemaining { e: String -> selectedColumns.add(e) }
            configuration.selectedFields =
              selectedColumns.stream().map { fieldName: String? -> SelectedFieldInfo().addFieldPathItem(fieldName) }.collect(
                Collectors.toList<@Valid SelectedFieldInfo?>(),
              )
          }
          AirbyteStreamAndConfiguration()
            .stream(configuredStream.stream.toApi())
            .config(configuration)
        }.collect(Collectors.toList())
    return AirbyteCatalog().streams(streams)
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

    return AirbyteCatalog()
      .streams(
        catalog.streams
          .stream()
          .map { stream: AirbyteStream -> this.toApi(stream) }
          .map { s: io.airbyte.api.model.generated.AirbyteStream ->
            AirbyteStreamAndConfiguration()
              .stream(s)
              .config(generateDefaultConfiguration(s, suggestingStreams, suggestedStreams, catalog.streams.stream().count()))
          }.collect(Collectors.toList<@Valid AirbyteStreamAndConfiguration?>()),
      )
  }

  @Throws(JsonValidationException::class)
  private fun toConfiguredProtocol(
    stream: io.airbyte.api.model.generated.AirbyteStream,
    config: AirbyteStreamConfiguration,
  ): AirbyteStream {
    if (config.fieldSelectionEnabled != null && config.fieldSelectionEnabled) {
      // Validate the selected field paths.
      if (config.selectedFields == null) {
        throw JsonValidationException("Requested field selection but no selected fields provided")
      }
      val properties = stream.jsonSchema.findValue("properties")
      if (properties == null || !properties.isObject) {
        throw JsonValidationException("Requested field selection but no properties node found")
      }
      for (selectedFieldInfo in config.selectedFields) {
        if (selectedFieldInfo.fieldPath == null || selectedFieldInfo.fieldPath.isEmpty()) {
          throw JsonValidationException("Selected field path cannot be empty")
        }
        if (selectedFieldInfo.fieldPath.size > 1) {
          // TODO(mfsiega-airbyte): support nested fields.
          throw UnsupportedOperationException("Nested field selection not supported")
        }
      }
      // Only include the selected fields.
      // NOTE: we verified above that each selected field has at least one element in the field path.
      val selectedFieldNames =
        config.selectedFields
          .stream()
          .map { field: SelectedFieldInfo -> field.fieldPath[0] }
          .collect(Collectors.toSet())
      // TODO(mfsiega-airbyte): we only check the top level of the cursor/primary key fields because we
      // don't support filtering nested fields yet.
      if (config.syncMode == SyncMode.INCREMENTAL &&
        // INCREMENTAL sync mode, AND
        !config.cursorField.isEmpty() &&
        // There is a cursor configured, AND
        !selectedFieldNames.contains(config.cursorField[0])
      ) { // The cursor isn't in the selected fields.
        throw JsonValidationException("Cursor field cannot be de-selected in INCREMENTAL syncs")
      }
      if (config.destinationSyncMode == DestinationSyncMode.APPEND_DEDUP ||
        config.destinationSyncMode == DestinationSyncMode.OVERWRITE_DEDUP
      ) {
        for (primaryKeyComponent in config.primaryKey) {
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
        stream.supportedSyncModes.convertTo<io.airbyte.protocol.models.v0.SyncMode>(),
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
    return hashedFields.stream().map<HashingMapperConfig> { f: SelectedFieldInfo -> createHashingMapper(f.fieldPath.first()) }.toList()
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
    Preconditions.checkArgument(MapperOperationName.HASHING == configuredHashingMapper.name(), "Expected hashing mapper")
    return SelectedFieldInfo()
      .fieldPath(java.util.List.of(getHashedFieldName(configuredHashingMapper as HashingMapperConfig)))
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
  @Throws(JsonValidationException::class)
  fun toConfiguredInternal(catalog: AirbyteCatalog): ConfiguredAirbyteCatalog {
    val errors: MutableList<JsonValidationException> = ArrayList()
    val streams =
      catalog.streams
        .stream()
        .filter { s: AirbyteStreamAndConfiguration -> s.config.selected }
        .map<ConfiguredAirbyteStream?> { s: AirbyteStreamAndConfiguration ->
          try {
            val convertedStream: io.airbyte.config.AirbyteStream = toConfiguredProtocol(s.getStream(), s.getConfig()).toInternal()
            val builder =
              ConfiguredAirbyteStream
                .Builder()
                .stream(convertedStream)
                .syncMode(
                  s.config.syncMode.convertTo<io.airbyte.config.SyncMode>(),
                ).destinationSyncMode(
                  s.config.destinationSyncMode.convertTo<io.airbyte.config.DestinationSyncMode>(),
                ).cursorField(s.config.cursorField)
                .primaryKey(Optional.ofNullable(s.config.primaryKey).orElse(emptyList()))
                .destinationObjectName(s.config.destinationObjectName)
                .fields(fieldGenerator.getFieldsFromSchema(convertedStream.jsonSchema))

            if (s.config.mappers != null && !s.config.mappers.isEmpty()) {
              builder
                .mappers(toConfiguredMappers(s.config.mappers))
            } else {
              // TODO(pedro): `hashedFields` support should be removed once the UI is updated to use `mappers`.
              builder
                .mappers(toConfiguredHashingMappers(s.config.hashedFields))
            }

            if (s.config.includeFiles != null) {
              builder.includeFiles(s.config.includeFiles)
            }

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
   * @param config the config to which we'll write the sync mode
   */
  fun configureDefaultSyncModesForNewStream(
    streamToConfigure: io.airbyte.api.model.generated.AirbyteStream,
    config: AirbyteStreamConfiguration,
  ) {
    val hasSourceDefinedCursor = streamToConfigure.sourceDefinedCursor != null && streamToConfigure.sourceDefinedCursor
    val hasSourceDefinedPrimaryKey =
      streamToConfigure.sourceDefinedPrimaryKey != null && !streamToConfigure.sourceDefinedPrimaryKey.isEmpty()
    val supportsFullRefresh = streamToConfigure.supportedSyncModes.contains(SyncMode.FULL_REFRESH)
    if (hasSourceDefinedCursor && hasSourceDefinedPrimaryKey) { // Source-defined cursor and primary key
      config
        .syncMode(SyncMode.INCREMENTAL)
        .destinationSyncMode(DestinationSyncMode.APPEND_DEDUP)
        .primaryKey(streamToConfigure.sourceDefinedPrimaryKey)
    } else if (hasSourceDefinedCursor && supportsFullRefresh) { // Source-defined cursor but no primary key.
      // NOTE: we prefer Full Refresh | Overwrite to avoid the risk of an Incremental | Append sync
      // blowing up their destination.
      config
        .syncMode(SyncMode.FULL_REFRESH)
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
    } else if (hasSourceDefinedCursor) { // Source-defined cursor but no primary key *and* no full-refresh supported.
      // If *only* incremental is supported, we go with it.
      config
        .syncMode(SyncMode.INCREMENTAL)
        .destinationSyncMode(DestinationSyncMode.APPEND)
    } else { // No source-defined cursor at all.
      config
        .syncMode(SyncMode.FULL_REFRESH)
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
    }
  }

  private fun generateDefaultConfiguration(
    stream: io.airbyte.api.model.generated.AirbyteStream,
    suggestingStreams: Boolean,
    suggestedStreams: List<String>,
    totalStreams: Long,
  ): AirbyteStreamConfiguration {
    val result =
      AirbyteStreamConfiguration()
        .aliasName(Names.toAlphanumericAndUnderscore(stream.name))
        .cursorField(stream.defaultCursorField)
        .destinationSyncMode(DestinationSyncMode.APPEND)
        .primaryKey(stream.sourceDefinedPrimaryKey)

    val onlyOneStream = totalStreams == 1L
    val isSelected = onlyOneStream || (suggestingStreams && suggestedStreams.contains(stream.name))

    // In the case where this connection hasn't yet been configured, the suggested streams are also
    // (pre)-selected
    result.suggested = isSelected
    result.selected = isSelected

    configureDefaultSyncModesForNewStream(stream, result)

    return result
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
  @Throws(JsonValidationException::class)
  fun toProtocolKeepAllStreams(catalog: AirbyteCatalog): ConfiguredAirbyteCatalog {
    val clone = Jsons.clone(catalog)
    clone.streams.forEach(
      Consumer { stream: AirbyteStreamAndConfiguration ->
        stream.config.selected =
          true
      },
    )
    return toConfiguredInternal(clone)
  }

  /**
   * To convert AirbyteCatalog from APIs to model. This is to differentiate between
   * toConfiguredProtocol as the other one converts to ConfiguredAirbyteCatalog object instead.
   */
  @Throws(JsonValidationException::class)
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
            return@map toConfiguredProtocol(stream.stream, stream.config)
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
        StreamDescriptor()
          .name(streamAndConfig.stream.name)
          .namespace(streamAndConfig.stream.namespace)
      val fieldSelectionEnabled =
        if (streamAndConfig.config.fieldSelectionEnabled == null) false else streamAndConfig.config.fieldSelectionEnabled
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
   */
  fun ensureCompatibleDestinationSyncMode(
    streamAndConfiguration: AirbyteStreamAndConfiguration,
    supportedDestinationSyncModes: List<DestinationSyncMode?>,
  ) {
    if (supportedDestinationSyncModes.contains(streamAndConfiguration.config.destinationSyncMode)) {
      return
    }
    val sourceSupportsFullRefresh = streamAndConfiguration.stream.supportedSyncModes.contains(SyncMode.FULL_REFRESH)
    val destinationSupportsOverwrite = supportedDestinationSyncModes.contains(DestinationSyncMode.OVERWRITE)
    if (sourceSupportsFullRefresh && destinationSupportsOverwrite) {
      // We prefer to fall back to Full Refresh | Overwrite if possible.
      streamAndConfiguration.config.syncMode(SyncMode.FULL_REFRESH).destinationSyncMode(DestinationSyncMode.OVERWRITE)
    } else {
      // If *that* isn't possible, we pick something that *is* supported. This isn't ideal, but we don't
      // have a clean way
      // to fail in this case today.
      val supportedSyncMode = streamAndConfiguration.stream.supportedSyncModes[0]
      val supportedDestinationSyncMode = supportedDestinationSyncModes[0]
      log.warn { "Default sync modes are incompatible, so falling back to $supportedSyncMode | $supportedDestinationSyncMode" }
      streamAndConfiguration.config.syncMode(supportedSyncMode).destinationSyncMode(supportedDestinationSyncMode)
    }
  }
}
