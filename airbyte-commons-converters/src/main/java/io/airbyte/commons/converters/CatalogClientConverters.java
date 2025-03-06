/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.client.model.generated.ConfiguredStreamMapper;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.text.Names;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.MapperConfig;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utilities to convert Catalog protocol to Catalog API client. This class is similar to the
 * existing logic in CatalogConverter.java; however, code can't be shared because the protocol model
 * is essentially converted to two different API models. Thus, if we need to change logic in either
 * place, we have to take care of the other one too.
 */
@Singleton
public class CatalogClientConverters {

  private final FieldGenerator fieldGenerator;

  public CatalogClientConverters(final FieldGenerator fieldGenerator) {
    this.fieldGenerator = fieldGenerator;
  }

  /**
   * Convert API model to Airbyte protocol model.
   *
   * @param catalog API model
   * @return Airbyte protocol model
   */
  public io.airbyte.protocol.models.AirbyteCatalog toAirbyteProtocol(
                                                                     final io.airbyte.api.client.model.generated.AirbyteCatalog catalog) {

    final io.airbyte.protocol.models.AirbyteCatalog protoCatalog =
        new io.airbyte.protocol.models.AirbyteCatalog();
    final var airbyteStreams = catalog.getStreams().stream().map(stream -> {
      try {
        return toStreamProtocol(stream.getStream(), stream.getConfig());
      } catch (final JsonValidationException e) {
        return null;
      }
    }).collect(Collectors.toList());

    protoCatalog.withStreams(airbyteStreams);
    return protoCatalog;
  }

  /**
   * Convert the API model to the Protocol model with configuration.
   *
   * @param catalog the API catalog
   * @return the protocol catalog
   */
  @SuppressWarnings("checkstyle:LineLength") // the auto-formatter produces a format that conflicts with checkstyle
  public io.airbyte.config.ConfiguredAirbyteCatalog toConfiguredAirbyteInternal(
                                                                                final io.airbyte.api.client.model.generated.AirbyteCatalog catalog) {
    final io.airbyte.config.ConfiguredAirbyteCatalog protoCatalog =
        new io.airbyte.config.ConfiguredAirbyteCatalog();
    final var airbyteStreams = catalog.getStreams().stream().map(stream -> {
      try {
        return toConfiguredStreamInternal(stream.getStream(), stream.getConfig());
      } catch (final JsonValidationException e) {
        return null;
      }
    }).collect(Collectors.toList());

    protoCatalog.withStreams(airbyteStreams);
    return protoCatalog;
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private io.airbyte.protocol.models.AirbyteStream toStreamProtocol(
                                                                    final io.airbyte.api.client.model.generated.AirbyteStream stream,
                                                                    final AirbyteStreamConfiguration config)
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
      if (config.getDestinationSyncMode().equals(DestinationSyncMode.APPEND_DEDUP)) {
        for (final List<String> primaryKeyComponent : config.getPrimaryKey()) {
          if (!selectedFieldNames.contains(primaryKeyComponent.get(0))) {
            throw new JsonValidationException("Primary key field cannot be de-selected in DEDUP mode");
          }
        }
      }
      for (final String selectedFieldName : selectedFieldNames) {
        if (!properties.has(selectedFieldName)) {
          throw new JsonValidationException(String.format("Requested selected field %s not found in JSON schema", selectedFieldName));
        }
      }
      ((ObjectNode) properties).retain(selectedFieldNames);
    }
    return new io.airbyte.protocol.models.AirbyteStream()
        .withName(stream.getName())
        .withJsonSchema(stream.getJsonSchema())
        .withSupportedSyncModes(Enums.convertListTo(stream.getSupportedSyncModes(), io.airbyte.protocol.models.SyncMode.class))
        .withSourceDefinedCursor(stream.getSourceDefinedCursor())
        .withDefaultCursorField(stream.getDefaultCursorField())
        .withSourceDefinedPrimaryKey(
            Optional.ofNullable(stream.getSourceDefinedPrimaryKey()).orElse(Collections.emptyList()))
        .withNamespace(stream.getNamespace())
        .withIsResumable(stream.isResumable());
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private io.airbyte.config.AirbyteStream toStreamInternal(
                                                           final io.airbyte.api.client.model.generated.AirbyteStream stream,
                                                           final AirbyteStreamConfiguration config)
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
      if (config.getDestinationSyncMode().equals(DestinationSyncMode.APPEND_DEDUP)) {
        for (final List<String> primaryKeyComponent : config.getPrimaryKey()) {
          if (!selectedFieldNames.contains(primaryKeyComponent.get(0))) {
            throw new JsonValidationException("Primary key field cannot be de-selected in DEDUP mode");
          }
        }
      }
      for (final String selectedFieldName : selectedFieldNames) {
        if (!properties.has(selectedFieldName)) {
          throw new JsonValidationException(String.format("Requested selected field %s not found in JSON schema", selectedFieldName));
        }
      }
      ((ObjectNode) properties).retain(selectedFieldNames);
    }
    return new io.airbyte.config.AirbyteStream(stream.getName(), stream.getJsonSchema(),
        Enums.convertListTo(stream.getSupportedSyncModes(), io.airbyte.config.SyncMode.class))
            .withSourceDefinedCursor(stream.getSourceDefinedCursor())
            .withDefaultCursorField(stream.getDefaultCursorField())
            .withSourceDefinedPrimaryKey(
                Optional.ofNullable(stream.getSourceDefinedPrimaryKey()).orElse(Collections.emptyList()))
            .withNamespace(stream.getNamespace())
            .withIsResumable(stream.isResumable());
  }

  private io.airbyte.api.model.generated.ConfiguredStreamMapper toModel(final io.airbyte.api.client.model.generated.ConfiguredStreamMapper mapper) {
    return new io.airbyte.api.model.generated.ConfiguredStreamMapper()
        .id(mapper.getId())
        .type(io.airbyte.api.model.generated.StreamMapperType.fromValue(mapper.getType().getValue()))
        .mapperConfiguration(mapper.getMapperConfiguration());

  }

  private List<MapperConfig> toConfiguredMappers(final @Nullable List<ConfiguredStreamMapper> mapperConfigs) {
    if (mapperConfigs == null) {
      return Collections.emptyList();
    }
    return mapperConfigs.stream()
        .map(mapperConfig -> MapperConvertersKt.toInternal(toModel(mapperConfig)))
        .toList();
  }

  private ConfiguredAirbyteStream toConfiguredStreamInternal(
                                                             final io.airbyte.api.client.model.generated.AirbyteStream stream,
                                                             final AirbyteStreamConfiguration config)
      throws JsonValidationException {
    final var convertedStream = toStreamInternal(stream, config);
    final ConfiguredAirbyteStream.Builder builder = new ConfiguredAirbyteStream.Builder()
        .stream(convertedStream)
        .syncMode(Enums.convertTo(config.getSyncMode(), io.airbyte.config.SyncMode.class))
        .destinationSyncMode(Enums.convertTo(config.getDestinationSyncMode(), io.airbyte.config.DestinationSyncMode.class))
        .primaryKey(config.getPrimaryKey())
        .cursorField(config.getCursorField())
        .generationId(config.getGenerationId())
        .minimumGenerationId(config.getMinimumGenerationId())
        .syncId(config.getSyncId())
        .fields(fieldGenerator.getFieldsFromSchema(convertedStream.getJsonSchema()))
        .mappers(toConfiguredMappers(config.getMappers()));

    return builder.build();
  }

  /**
   * Converts a protocol AirbyteCatalog to an OpenAPI client versioned AirbyteCatalog.
   */
  @SuppressWarnings("LineLength")
  public io.airbyte.api.client.model.generated.AirbyteCatalog toAirbyteCatalogClientApi(
                                                                                        final io.airbyte.protocol.models.AirbyteCatalog catalog) {
    return new io.airbyte.api.client.model.generated.AirbyteCatalog(catalog.getStreams()
        .stream()
        .map(this::toAirbyteStreamClientApi)
        .map(s -> new io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration(s, generateDefaultConfiguration(s)))
        .toList());
  }

  @SuppressWarnings("LineLength")
  private io.airbyte.api.client.model.generated.AirbyteStreamConfiguration generateDefaultConfiguration(
                                                                                                        final io.airbyte.api.client.model.generated.AirbyteStream stream) {
    return new io.airbyte.api.client.model.generated.AirbyteStreamConfiguration(
        !stream.getSupportedSyncModes().isEmpty() ? Enums.convertTo(stream.getSupportedSyncModes().get(0),
            io.airbyte.api.client.model.generated.SyncMode.class) : io.airbyte.api.client.model.generated.SyncMode.INCREMENTAL,
        io.airbyte.api.client.model.generated.DestinationSyncMode.APPEND,
        stream.getDefaultCursorField(),
        stream.getSourceDefinedPrimaryKey(),
        Names.toAlphanumericAndUnderscore(stream.getName()),
        true,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private io.airbyte.api.client.model.generated.AirbyteStream toAirbyteStreamClientApi(
                                                                                       final io.airbyte.protocol.models.AirbyteStream stream) {
    return new io.airbyte.api.client.model.generated.AirbyteStream(
        stream.getName(),
        stream.getJsonSchema(),
        Enums.convertListTo(stream.getSupportedSyncModes(),
            io.airbyte.api.client.model.generated.SyncMode.class),
        stream.getSourceDefinedCursor(),
        stream.getDefaultCursorField(),
        stream.getSourceDefinedPrimaryKey(),
        stream.getNamespace(),
        stream.getIsResumable());
  }

}
