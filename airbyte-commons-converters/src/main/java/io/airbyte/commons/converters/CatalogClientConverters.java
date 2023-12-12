/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.text.Names;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.validation.json.JsonValidationException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utilities to convert Catalog protocol to Catalog API client. This class was similar to existing
 * logic in CatalogConverter.java; But code can't be shared because the protocol model is
 * essentially converted to two different api models. Thus, if we need to change logic on either
 * place we have to take care of the other one too.
 */
public class CatalogClientConverters {

  /**
   * Convert to api model to airbyte protocol model.
   *
   * @param catalog api model
   * @return catalog protocol model
   */
  public static io.airbyte.protocol.models.AirbyteCatalog toAirbyteProtocol(final io.airbyte.api.client.model.generated.AirbyteCatalog catalog) {

    final io.airbyte.protocol.models.AirbyteCatalog protoCatalog =
        new io.airbyte.protocol.models.AirbyteCatalog();
    final var airbyteStream = catalog.getStreams().stream().map(stream -> {
      try {
        return toStreamProtocol(stream.getStream(), stream.getConfig());
      } catch (final JsonValidationException e) {
        return null;
      }
    }).collect(Collectors.toList());

    protoCatalog.withStreams(airbyteStream);
    return protoCatalog;
  }

  /**
   * Convert the API model to the Protocol model with configuration.
   *
   * @param catalog the API catalog
   * @return the protocol catalog
   */
  @SuppressWarnings("checkstyle:LineLength") // the auto-formatter produces a format that conflicts with checkstyle
  public static io.airbyte.protocol.models.ConfiguredAirbyteCatalog toConfiguredAirbyteProtocol(final io.airbyte.api.client.model.generated.AirbyteCatalog catalog) {
    final io.airbyte.protocol.models.ConfiguredAirbyteCatalog protoCatalog =
        new io.airbyte.protocol.models.ConfiguredAirbyteCatalog();
    final var airbyteStream = catalog.getStreams().stream().map(stream -> {
      try {
        return toConfiguredStreamProtocol(stream.getStream(), stream.getConfig());
      } catch (final JsonValidationException e) {
        return null;
      }
    }).collect(Collectors.toList());

    protoCatalog.withStreams(airbyteStream);
    return protoCatalog;
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private static io.airbyte.protocol.models.AirbyteStream toStreamProtocol(final io.airbyte.api.client.model.generated.AirbyteStream stream,
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
        .withNamespace(stream.getNamespace());
  }

  private static ConfiguredAirbyteStream toConfiguredStreamProtocol(final io.airbyte.api.client.model.generated.AirbyteStream stream,
                                                                    final AirbyteStreamConfiguration config)
      throws JsonValidationException {
    return new ConfiguredAirbyteStream()
        .withStream(toStreamProtocol(stream, config))
        .withSyncMode(Enums.convertTo(config.getSyncMode(), io.airbyte.protocol.models.SyncMode.class))
        .withDestinationSyncMode(Enums.convertTo(config.getDestinationSyncMode(), io.airbyte.protocol.models.DestinationSyncMode.class))
        .withPrimaryKey(config.getPrimaryKey())
        .withCursorField(config.getCursorField());
  }

  /**
   * Converts a protocol AirbyteCatalog to an OpenAPI client versioned AirbyteCatalog.
   */
  @SuppressWarnings("LineLength")
  public static io.airbyte.api.client.model.generated.AirbyteCatalog toAirbyteCatalogClientApi(
                                                                                               final io.airbyte.protocol.models.AirbyteCatalog catalog) {
    return new io.airbyte.api.client.model.generated.AirbyteCatalog()
        .streams(catalog.getStreams()
            .stream()
            .map(stream -> toAirbyteStreamClientApi(stream))
            .map(s -> new io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration()
                .stream(s)
                .config(generateDefaultConfiguration(s)))
            .collect(Collectors.toList()));
  }

  @SuppressWarnings("LineLength")
  private static io.airbyte.api.client.model.generated.AirbyteStreamConfiguration generateDefaultConfiguration(
                                                                                                               final io.airbyte.api.client.model.generated.AirbyteStream stream) {
    final io.airbyte.api.client.model.generated.AirbyteStreamConfiguration result =
        new io.airbyte.api.client.model.generated.AirbyteStreamConfiguration()
            .aliasName(Names.toAlphanumericAndUnderscore(stream.getName()))
            .cursorField(stream.getDefaultCursorField())
            .destinationSyncMode(io.airbyte.api.client.model.generated.DestinationSyncMode.APPEND)
            .primaryKey(stream.getSourceDefinedPrimaryKey())
            .selected(true);
    if (stream.getSupportedSyncModes().size() > 0) {
      result.setSyncMode(Enums.convertTo(stream.getSupportedSyncModes().get(0),
          io.airbyte.api.client.model.generated.SyncMode.class));
    } else {
      result.setSyncMode(io.airbyte.api.client.model.generated.SyncMode.INCREMENTAL);
    }
    return result;
  }

  private static io.airbyte.api.client.model.generated.AirbyteStream toAirbyteStreamClientApi(final AirbyteStream stream) {
    return new io.airbyte.api.client.model.generated.AirbyteStream()
        .name(stream.getName())
        .jsonSchema(stream.getJsonSchema())
        .supportedSyncModes(Enums.convertListTo(stream.getSupportedSyncModes(),
            io.airbyte.api.client.model.generated.SyncMode.class))
        .sourceDefinedCursor(stream.getSourceDefinedCursor())
        .defaultCursorField(stream.getDefaultCursorField())
        .sourceDefinedPrimaryKey(stream.getSourceDefinedPrimaryKey())
        .namespace(stream.getNamespace());
  }

}
