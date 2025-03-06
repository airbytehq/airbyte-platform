/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.SyncMode;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Helper class for Catalog and Stream related operations. Generally only used in tests.
 */
public class CatalogHelpers {

  private final FieldGenerator fieldGenerator;

  public CatalogHelpers(final FieldGenerator fieldGenerator) {
    this.fieldGenerator = fieldGenerator;
  }

  public static AirbyteStream createAirbyteStream(final String streamName, final Field... fields) {
    // Namespace is null since not all sources set it.
    return createAirbyteStream(streamName, null, Arrays.asList(fields));
  }

  public static AirbyteStream createAirbyteStream(final String streamName,
                                                  final String namespace,
                                                  final Field... fields) {
    return createAirbyteStream(streamName, namespace, Arrays.asList(fields));
  }

  public static AirbyteStream createAirbyteStream(final String streamName,
                                                  final String namespace,
                                                  final List<Field> fields) {
    return new AirbyteStream(streamName, fieldsToJsonSchema(fields), List.of(SyncMode.FULL_REFRESH)).withNamespace(namespace);
  }

  public ConfiguredAirbyteCatalog createConfiguredAirbyteCatalog(final String streamName,
                                                                 final String namespace,
                                                                 final Field... fields) {
    return new ConfiguredAirbyteCatalog().withStreams(
        List.of(createConfiguredAirbyteStream(streamName, namespace, fields)));
  }

  public ConfiguredAirbyteCatalog createConfiguredAirbyteCatalog(final String streamName,
                                                                 final String namespace,
                                                                 final List<Field> fields) {
    return new ConfiguredAirbyteCatalog().withStreams(
        List.of(createConfiguredAirbyteStream(streamName, namespace, fields)));
  }

  public ConfiguredAirbyteStream createConfiguredAirbyteStream(final String streamName,
                                                               final String namespace,
                                                               final Field... fields) {
    return createConfiguredAirbyteStream(streamName, namespace, Arrays.asList(fields));
  }

  public ConfiguredAirbyteStream createConfiguredAirbyteStream(final String streamName,
                                                               final String namespace,
                                                               final List<Field> fields) {
    final JsonNode jsonSchema = fieldsToJsonSchema(fields);
    return new ConfiguredAirbyteStream.Builder()
        .stream(new AirbyteStream(streamName, jsonSchema, List.of(SyncMode.FULL_REFRESH)).withNamespace(namespace))
        .syncMode(SyncMode.FULL_REFRESH)
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .fields(fieldGenerator.getFieldsFromSchema(jsonSchema))
        .build();
  }

  /**
   * Converts a {@link ConfiguredAirbyteCatalog} into an {@link AirbyteCatalog}. This is possible
   * because the latter is a subset of the former.
   *
   * @param configuredCatalog - catalog to convert
   * @return - airbyte catalog
   */
  public static AirbyteCatalog configuredCatalogToCatalog(final ConfiguredAirbyteCatalog configuredCatalog) {
    return new AirbyteCatalog().withStreams(
        configuredCatalog.getStreams()
            .stream()
            .map(ConfiguredAirbyteStream::getStream)
            .map(ProtocolConverters::toProtocol)
            .toList());
  }

  /**
   * Extracts {@link StreamDescriptor} for a given {@link AirbyteStream}.
   *
   * @param airbyteStream stream
   * @return stream descriptor
   */
  public static StreamDescriptor extractDescriptor(final ConfiguredAirbyteStream airbyteStream) {
    return extractDescriptor(airbyteStream.getStream());
  }

  /**
   * Extracts {@link StreamDescriptor} for a given {@link ConfiguredAirbyteStream}.
   *
   * @param airbyteStream stream
   * @return stream descriptor
   */
  public static StreamDescriptor extractDescriptor(final AirbyteStream airbyteStream) {
    return airbyteStream.getStreamDescriptor();
  }

  /**
   * Extracts {@link StreamDescriptor}s for each stream in a given {@link ConfiguredAirbyteCatalog}.
   *
   * @param configuredCatalog catalog
   * @return list of stream descriptors
   */
  public static List<StreamDescriptor> extractStreamDescriptors(final ConfiguredAirbyteCatalog configuredCatalog) {
    return configuredCatalog.getStreams().stream().map(CatalogHelpers::extractDescriptor).toList();
  }

  /**
   * Maps a list of fields into a JsonSchema object with names and types. This method will throw if it
   * receives multiple fields with the same name.
   *
   * @param fields fields to map to JsonSchema
   * @return JsonSchema representation of the fields.
   */
  public static JsonNode fieldsToJsonSchema(final List<Field> fields) {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("type", "object")
        .put("properties", fields
            .stream()
            .collect(Collectors.toMap(
                Field::getName,
                field -> {
                  if (isObjectWithSubFields(field)) {
                    return fieldsToJsonSchema(field.getSubFields());
                  } else {
                    return field.getType().getJsonSchemaTypeMap();
                  }
                })))
        .build());
  }

  public static JsonNode fieldsToJsonSchema(final Field... fields) {
    return fieldsToJsonSchema(Arrays.asList(fields));
  }

  private static boolean isObjectWithSubFields(final Field field) {
    return field.getType().equals(JsonSchemaType.OBJECT) && field.getSubFields() != null
        && !field.getSubFields().isEmpty();
  }

}
