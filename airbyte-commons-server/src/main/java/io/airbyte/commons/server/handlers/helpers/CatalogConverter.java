/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.util.Preconditions;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStream;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.SelectedFieldInfo;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.SyncMode;
import io.airbyte.commons.converters.ApiConverters;
import io.airbyte.commons.converters.MapperConvertersKt;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.text.Names;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.FieldSelectionData;
import io.airbyte.config.MapperConfig;
import io.airbyte.config.MapperOperationName;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.helpers.ProtocolConverters;
import io.airbyte.config.mapper.configs.HashingMapperConfig;
import io.airbyte.mappers.helpers.MapperHelperKt;
import io.airbyte.mappers.transformations.Mapper;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert classes between io.airbyte.protocol.models and io.airbyte.api.model.generated.
 */
@Singleton
public class CatalogConverter {

  private final FieldGenerator fieldGenerator;
  private final Map<String, Mapper<? extends MapperConfig>> mappers;
  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogConverter.class);

  public CatalogConverter(final FieldGenerator fieldGenerator, final List<Mapper<? extends MapperConfig>> mappers) {
    this.fieldGenerator = fieldGenerator;
    this.mappers = mappers.stream().collect(Collectors.toMap(Mapper::getName, c -> c));
  }

  private io.airbyte.api.model.generated.AirbyteStream toApi(final io.airbyte.protocol.models.v0.AirbyteStream stream) {
    return new io.airbyte.api.model.generated.AirbyteStream()
        .name(stream.getName())
        .jsonSchema(stream.getJsonSchema())
        .supportedSyncModes(Enums.convertListTo(stream.getSupportedSyncModes(), io.airbyte.api.model.generated.SyncMode.class))
        .sourceDefinedCursor(stream.getSourceDefinedCursor() != null ? stream.getSourceDefinedCursor() : false)
        .defaultCursorField(stream.getDefaultCursorField())
        .sourceDefinedPrimaryKey(stream.getSourceDefinedPrimaryKey())
        .namespace(stream.getNamespace())
        .isFileBased(stream.getIsFileBased())
        .isResumable(stream.getIsResumable());
  }

  /**
   * Convert an internal catalog and field selection mask to an api catalog model.
   *
   * @param catalog internal catalog
   * @param fieldSelectionData field selection mask
   * @return api catalog model
   */
  public io.airbyte.api.model.generated.AirbyteCatalog toApi(final ConfiguredAirbyteCatalog catalog,
                                                             final FieldSelectionData fieldSelectionData) {
    final List<io.airbyte.api.model.generated.AirbyteStreamAndConfiguration> streams = catalog.getStreams()
        .stream()
        .map(configuredStream -> {
          final var streamDescriptor = new StreamDescriptor()
              .name(configuredStream.getStream().getName())
              .namespace(configuredStream.getStream().getNamespace());
          final io.airbyte.api.model.generated.AirbyteStreamConfiguration configuration =
              new io.airbyte.api.model.generated.AirbyteStreamConfiguration()
                  .syncMode(Enums.convertTo(configuredStream.getSyncMode(), io.airbyte.api.model.generated.SyncMode.class))
                  .cursorField(configuredStream.getCursorField())
                  .destinationSyncMode(
                      Enums.convertTo(configuredStream.getDestinationSyncMode(), io.airbyte.api.model.generated.DestinationSyncMode.class))
                  .primaryKey(configuredStream.getPrimaryKey())
                  .aliasName(Names.toAlphanumericAndUnderscore(configuredStream.getStream().getName()))
                  .selected(true)
                  .suggested(false)
                  .includeFiles(configuredStream.getIncludeFiles())
                  .fieldSelectionEnabled(getStreamHasFieldSelectionEnabled(fieldSelectionData, streamDescriptor))
                  .selectedFields(List.of())
                  // TODO(pedro): `hashedFields` should be removed once the UI is updated to use `mappers`.
                  .hashedFields(configuredStream.getMappers().stream().filter(mapper -> MapperOperationName.HASHING.equals(mapper.name()))
                      .map(this::toApiFieldInfo)
                      .collect(Collectors.toList()))
                  .mappers(configuredStream.getMappers().stream().map(MapperConvertersKt::toApi).collect(Collectors.toList()))
                  .generationId(configuredStream.getGenerationId())
                  .minimumGenerationId(configuredStream.getMinimumGenerationId())
                  .syncId(configuredStream.getSyncId());
          if (configuration.getFieldSelectionEnabled()) {
            final List<String> selectedColumns = new ArrayList<>();
            // TODO(mfsiega-airbyte): support nested fields here.
            configuredStream.getStream()
                .getJsonSchema()
                .findValue("properties")
                .fieldNames().forEachRemaining(selectedColumns::add);
            configuration.setSelectedFields(
                selectedColumns.stream().map((fieldName) -> new SelectedFieldInfo().addFieldPathItem(fieldName)).collect(Collectors.toList()));
          }
          return new io.airbyte.api.model.generated.AirbyteStreamAndConfiguration()
              .stream(ApiConverters.toApi(configuredStream.getStream()))
              .config(configuration);
        })
        .collect(Collectors.toList());
    return new io.airbyte.api.model.generated.AirbyteCatalog().streams(streams);
  }

  /**
   * Convert an internal model version of the catalog into an api model of the catalog.
   *
   * @param catalog internal catalog model
   * @param sourceVersion actor definition version for the source in use
   * @return api catalog model
   */
  public io.airbyte.api.model.generated.AirbyteCatalog toApi(final io.airbyte.protocol.models.v0.AirbyteCatalog catalog,
                                                             @Nullable final ActorDefinitionVersion sourceVersion) {
    final List<String> suggestedStreams = new ArrayList<>();
    final Boolean suggestingStreams;

    // There are occasions in tests where we have not seeded the sourceVersion fully. This is to
    // prevent those tests from failing
    if (sourceVersion != null) {
      suggestingStreams = sourceVersion.getSuggestedStreams() != null;
      if (suggestingStreams) {
        suggestedStreams.addAll(sourceVersion.getSuggestedStreams().getStreams());
      }
    } else {
      suggestingStreams = false;
    }

    return new io.airbyte.api.model.generated.AirbyteCatalog()
        .streams(catalog.getStreams()
            .stream()
            .map(this::toApi)
            .map(s -> new io.airbyte.api.model.generated.AirbyteStreamAndConfiguration()
                .stream(s)
                .config(generateDefaultConfiguration(s, suggestingStreams, suggestedStreams, catalog.getStreams().stream().count())))
            .collect(Collectors.toList()));
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private io.airbyte.protocol.models.v0.AirbyteStream toConfiguredProtocol(final AirbyteStream stream, final AirbyteStreamConfiguration config)
      throws JsonValidationException {
    if (config.getFieldSelectionEnabled() != null && config.getFieldSelectionEnabled()) {
      // Validate the selected field paths.
      if (config.getSelectedFields() == null) {
        throw new JsonValidationException("Requested field selection but no selected fields provided");
      }
      final JsonNode properties = stream.getJsonSchema().findValue("properties");
      if (properties == null || !properties.isObject()) {
        throw new JsonValidationException("Requested field selection but no properties node found");
      }
      for (final var selectedFieldInfo : config.getSelectedFields()) {
        if (selectedFieldInfo.getFieldPath() == null || selectedFieldInfo.getFieldPath().isEmpty()) {
          throw new JsonValidationException("Selected field path cannot be empty");
        }
        if (selectedFieldInfo.getFieldPath().size() > 1) {
          // TODO(mfsiega-airbyte): support nested fields.
          throw new UnsupportedOperationException("Nested field selection not supported");
        }
      }
      // Only include the selected fields.
      // NOTE: we verified above that each selected field has at least one element in the field path.
      final Set<String> selectedFieldNames =
          config.getSelectedFields().stream().map((field) -> field.getFieldPath().get(0)).collect(Collectors.toSet());
      // TODO(mfsiega-airbyte): we only check the top level of the cursor/primary key fields because we
      // don't support filtering nested fields yet.
      if (config.getSyncMode().equals(SyncMode.INCREMENTAL) // INCREMENTAL sync mode, AND
          && !config.getCursorField().isEmpty() // There is a cursor configured, AND
          && !selectedFieldNames.contains(config.getCursorField().get(0))) { // The cursor isn't in the selected fields.
        throw new JsonValidationException("Cursor field cannot be de-selected in INCREMENTAL syncs");
      }
      if (config.getDestinationSyncMode().equals(DestinationSyncMode.APPEND_DEDUP)
          || config.getDestinationSyncMode().equals(DestinationSyncMode.OVERWRITE_DEDUP)) {
        for (final List<String> primaryKeyComponent : config.getPrimaryKey()) {
          if (!selectedFieldNames.contains(primaryKeyComponent.get(0))) {
            throw new JsonValidationException("Primary key field cannot be de-selected in DEDUP mode");
          }
        }
      }
      for (final String selectedFieldName : selectedFieldNames) {
        if (!properties.has(selectedFieldName)) {
          LOGGER.info("Requested selected field {} not found in JSON schema", selectedFieldName);
        }
      }
      ((ObjectNode) properties).retain(selectedFieldNames);
    }
    return new io.airbyte.protocol.models.v0.AirbyteStream()
        .withName(stream.getName())
        .withJsonSchema(stream.getJsonSchema())
        .withSupportedSyncModes(Enums.convertListTo(stream.getSupportedSyncModes(), io.airbyte.protocol.models.v0.SyncMode.class))
        .withSourceDefinedCursor(stream.getSourceDefinedCursor())
        .withDefaultCursorField(stream.getDefaultCursorField())
        .withIsFileBased(stream.getIsFileBased())
        .withSourceDefinedPrimaryKey(Optional.ofNullable(stream.getSourceDefinedPrimaryKey()).orElse(Collections.emptyList()))
        .withNamespace(stream.getNamespace())
        .withIsResumable(stream.getIsResumable());
  }

  private List<? extends MapperConfig> toConfiguredHashingMappers(@Nullable final List<SelectedFieldInfo> hashedFields) {
    if (hashedFields == null) {
      return Collections.emptyList();
    }
    return hashedFields.stream().map(
        f -> MapperHelperKt.createHashingMapper(f.getFieldPath().getFirst()) // We don't support nested fields for now.
    ).toList();
  }

  public List<MapperConfig> toConfiguredMappers(final @Nullable List<io.airbyte.api.model.generated.ConfiguredStreamMapper> mapperConfigs) {
    if (mapperConfigs == null) {
      return Collections.emptyList();
    }

    return mapperConfigs.stream()
        .map(MapperConvertersKt::toInternal)
        .collect(Collectors.toList());
  }

  private SelectedFieldInfo toApiFieldInfo(final MapperConfig configuredHashingMapper) {
    Preconditions.checkArgument(MapperOperationName.HASHING.equals(configuredHashingMapper.name()), "Expected hashing mapper");
    return new SelectedFieldInfo()
        .fieldPath(List.of(MapperHelperKt.getHashedFieldName((HashingMapperConfig) configuredHashingMapper)));
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
  public ConfiguredAirbyteCatalog toConfiguredInternal(final io.airbyte.api.model.generated.AirbyteCatalog catalog)
      throws JsonValidationException {
    final List<JsonValidationException> errors = new ArrayList<>();
    final List<ConfiguredAirbyteStream> streams = catalog.getStreams()
        .stream()
        .filter(s -> s.getConfig().getSelected())
        .map(s -> {
          try {
            final io.airbyte.config.AirbyteStream convertedStream = ProtocolConverters.toInternal(toConfiguredProtocol(s.getStream(), s.getConfig()));
            final ConfiguredAirbyteStream.Builder builder = new ConfiguredAirbyteStream.Builder()
                .stream(convertedStream)
                .syncMode(Enums.convertTo(s.getConfig().getSyncMode(), io.airbyte.config.SyncMode.class))
                .destinationSyncMode(Enums.convertTo(s.getConfig().getDestinationSyncMode(), io.airbyte.config.DestinationSyncMode.class))
                .cursorField(s.getConfig().getCursorField())
                .primaryKey(Optional.ofNullable(s.getConfig().getPrimaryKey()).orElse(Collections.emptyList()))
                .fields(fieldGenerator.getFieldsFromSchema(convertedStream.getJsonSchema()));

            if (s.getConfig().getMappers() != null && !s.getConfig().getMappers().isEmpty()) {
              builder
                  .mappers(toConfiguredMappers(s.getConfig().getMappers()));
            } else {
              // TODO(pedro): `hashedFields` support should be removed once the UI is updated to use `mappers`.
              builder
                  .mappers(toConfiguredHashingMappers(s.getConfig().getHashedFields()));
            }

            if (s.getConfig().getIncludeFiles() != null) {
              builder.includeFiles(s.getConfig().getIncludeFiles());
            }

            return builder.build();
          } catch (final JsonValidationException e) {
            LOGGER.error("Error parsing catalog: {}", e);
            errors.add(e);
            return null;
          }
        })
        .collect(Collectors.toList());
    if (!errors.isEmpty()) {
      throw errors.get(0);
    }
    return new ConfiguredAirbyteCatalog()
        .withStreams(streams);
  }

  /**
   * Set the default sync modes for an un-configured stream based on the stream properties.
   * <p>
   * The logic is: - source-defined cursor and source-defined primary key -> INCREMENTAL, APPEND-DEDUP
   * - source-defined cursor only or nothing defined by the source -> FULL REFRESH, OVERWRITE -
   * source-defined cursor and full refresh not available as a sync method -> INCREMENTAL, APPEND
   *
   * @param streamToConfigure the stream for which we're picking a sync mode
   * @param config the config to which we'll write the sync mode
   */
  public void configureDefaultSyncModesForNewStream(final AirbyteStream streamToConfigure, final AirbyteStreamConfiguration config) {
    final boolean hasSourceDefinedCursor = streamToConfigure.getSourceDefinedCursor() != null && streamToConfigure.getSourceDefinedCursor();
    final boolean hasSourceDefinedPrimaryKey =
        streamToConfigure.getSourceDefinedPrimaryKey() != null && !streamToConfigure.getSourceDefinedPrimaryKey().isEmpty();
    final boolean supportsFullRefresh = streamToConfigure.getSupportedSyncModes().contains(SyncMode.FULL_REFRESH);
    if (hasSourceDefinedCursor && hasSourceDefinedPrimaryKey) { // Source-defined cursor and primary key
      config
          .syncMode(SyncMode.INCREMENTAL)
          .destinationSyncMode(DestinationSyncMode.APPEND_DEDUP)
          .primaryKey(streamToConfigure.getSourceDefinedPrimaryKey());
    } else if (hasSourceDefinedCursor && supportsFullRefresh) { // Source-defined cursor but no primary key.
      // NOTE: we prefer Full Refresh | Overwrite to avoid the risk of an Incremental | Append sync
      // blowing up their destination.
      config
          .syncMode(SyncMode.FULL_REFRESH)
          .destinationSyncMode(DestinationSyncMode.OVERWRITE);
    } else if (hasSourceDefinedCursor) { // Source-defined cursor but no primary key *and* no full-refresh supported.
      // If *only* incremental is supported, we go with it.
      config
          .syncMode(SyncMode.INCREMENTAL)
          .destinationSyncMode(DestinationSyncMode.APPEND);
    } else { // No source-defined cursor at all.
      config
          .syncMode(SyncMode.FULL_REFRESH)
          .destinationSyncMode(DestinationSyncMode.OVERWRITE);
    }
  }

  @SuppressWarnings("LineLength")
  private io.airbyte.api.model.generated.AirbyteStreamConfiguration generateDefaultConfiguration(final io.airbyte.api.model.generated.AirbyteStream stream,
                                                                                                 final Boolean suggestingStreams,
                                                                                                 final List<String> suggestedStreams,
                                                                                                 final Long totalStreams) {
    final io.airbyte.api.model.generated.AirbyteStreamConfiguration result = new io.airbyte.api.model.generated.AirbyteStreamConfiguration()
        .aliasName(Names.toAlphanumericAndUnderscore(stream.getName()))
        .cursorField(stream.getDefaultCursorField())
        .destinationSyncMode(io.airbyte.api.model.generated.DestinationSyncMode.APPEND)
        .primaryKey(stream.getSourceDefinedPrimaryKey());

    final boolean onlyOneStream = totalStreams == 1L;
    final boolean isSelected = onlyOneStream || (suggestingStreams && suggestedStreams.contains(stream.getName()));

    // In the case where this connection hasn't yet been configured, the suggested streams are also
    // (pre)-selected
    result.setSuggested(isSelected);
    result.setSelected(isSelected);

    configureDefaultSyncModesForNewStream(stream, result);

    return result;
  }

  private Boolean getStreamHasFieldSelectionEnabled(final FieldSelectionData fieldSelectionData, final StreamDescriptor streamDescriptor) {
    if (fieldSelectionData == null
        || fieldSelectionData.getAdditionalProperties().get(streamDescriptorToStringForFieldSelection(streamDescriptor)) == null) {
      return false;
    }

    return fieldSelectionData.getAdditionalProperties().get(streamDescriptorToStringForFieldSelection(streamDescriptor));
  }

  /**
   * Converts the API catalog model into a protocol catalog. Note: returns all streams, regardless of
   * selected status. See
   * {@link CatalogConverter#toConfiguredProtocol(AirbyteStream, AirbyteStreamConfiguration)} for
   * context.
   *
   * @param catalog api catalog
   * @return protocol catalog
   */
  @SuppressWarnings("LineLength")
  public ConfiguredAirbyteCatalog toProtocolKeepAllStreams(final io.airbyte.api.model.generated.AirbyteCatalog catalog)
      throws JsonValidationException {
    final AirbyteCatalog clone = Jsons.clone(catalog);
    clone.getStreams().forEach(stream -> stream.getConfig().setSelected(true));
    return toConfiguredInternal(clone);
  }

  /**
   * To convert AirbyteCatalog from APIs to model. This is to differentiate between
   * toConfiguredProtocol as the other one converts to ConfiguredAirbyteCatalog object instead.
   */
  public io.airbyte.protocol.models.v0.AirbyteCatalog toProtocol(
                                                                 final io.airbyte.api.model.generated.AirbyteCatalog catalog)
      throws JsonValidationException {
    final List<JsonValidationException> errors = new ArrayList<>();

    final io.airbyte.protocol.models.v0.AirbyteCatalog protoCatalog =
        new io.airbyte.protocol.models.v0.AirbyteCatalog();
    final var airbyteStream = catalog.getStreams().stream().map(stream -> {
      try {
        return toConfiguredProtocol(stream.getStream(), stream.getConfig());
      } catch (final JsonValidationException e) {
        LOGGER.error("Error parsing catalog: {}", e);
        errors.add(e);
        return null;
      }
    }).collect(Collectors.toList());

    if (!errors.isEmpty()) {
      throw errors.get(0);
    }
    protoCatalog.withStreams(airbyteStream);
    return protoCatalog;
  }

  /**
   * Generate the map from StreamDescriptor to indicator of whether field selection is enabled for
   * that stream.
   *
   * @param syncCatalog the catalog
   * @return the map as a FieldSelectionData object
   */
  public FieldSelectionData getFieldSelectionData(final AirbyteCatalog syncCatalog) {
    if (syncCatalog == null) {
      return null;
    }
    final var fieldSelectionData = new FieldSelectionData();
    for (final AirbyteStreamAndConfiguration streamAndConfig : syncCatalog.getStreams()) {
      final var streamDescriptor = new StreamDescriptor()
          .name(streamAndConfig.getStream().getName())
          .namespace(streamAndConfig.getStream().getNamespace());
      final boolean fieldSelectionEnabled =
          streamAndConfig.getConfig().getFieldSelectionEnabled() == null ? false : streamAndConfig.getConfig().getFieldSelectionEnabled();
      fieldSelectionData.setAdditionalProperty(streamDescriptorToStringForFieldSelection(streamDescriptor), fieldSelectionEnabled);
    }
    return fieldSelectionData;
  }

  // Return a string representation of a stream descriptor that's convenient to use as a key for the
  // field selection data.
  private String streamDescriptorToStringForFieldSelection(final StreamDescriptor streamDescriptor) {
    return String.format("%s/%s", streamDescriptor.getNamespace(), streamDescriptor.getName());
  }

  /**
   * Ensure that the configured sync modes are compatible with the source and the destination.
   * <p>
   * When we discover a new stream -- either during manual or auto schema refresh -- we want to pick
   * some default sync modes. This depends both on the source-supported sync modes -- represented in
   * the discovered catalog -- and the destination-supported sync modes. The latter is tricky because
   * the place where we're generating the default configuration isn't associated with a particular
   * destination.
   * <p>
   * A longer-term fix would be to restructure how we generate this default config, but for now we use
   * this to ensure that we've chosen defaults that work for the relevant sync.
   *
   * @param streamAndConfiguration the stream and configuration to check
   * @param supportedDestinationSyncModes the sync modes supported by the destination
   */
  public void ensureCompatibleDestinationSyncMode(final AirbyteStreamAndConfiguration streamAndConfiguration,
                                                  final List<DestinationSyncMode> supportedDestinationSyncModes) {
    if (supportedDestinationSyncModes.contains(streamAndConfiguration.getConfig().getDestinationSyncMode())) {
      return;
    }
    final var sourceSupportsFullRefresh = streamAndConfiguration.getStream().getSupportedSyncModes().contains(SyncMode.FULL_REFRESH);
    final var destinationSupportsOverwrite = supportedDestinationSyncModes.contains(DestinationSyncMode.OVERWRITE);
    if (sourceSupportsFullRefresh && destinationSupportsOverwrite) {
      // We prefer to fall back to Full Refresh | Overwrite if possible.
      streamAndConfiguration.getConfig().syncMode(SyncMode.FULL_REFRESH).destinationSyncMode(DestinationSyncMode.OVERWRITE);
    } else {
      // If *that* isn't possible, we pick something that *is* supported. This isn't ideal, but we don't
      // have a clean way
      // to fail in this case today.
      final var supportedSyncMode = streamAndConfiguration.getStream().getSupportedSyncModes().get(0);
      final var supportedDestinationSyncMode = supportedDestinationSyncModes.get(0);
      LOGGER.warn("Default sync modes are incompatible, so falling back to {} | {}", supportedSyncMode, supportedDestinationSyncMode);
      streamAndConfiguration.getConfig().syncMode(supportedSyncMode).destinationSyncMode(supportedDestinationSyncMode);
    }
  }

}
