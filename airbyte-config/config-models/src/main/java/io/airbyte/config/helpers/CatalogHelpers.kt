/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.helpers.ProtocolConverters.Companion.toProtocol
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.Field

/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

/**
 * Helper class for Catalog and Stream related operations. Generally only used in tests.
 */
class CatalogHelpers(
  private val fieldGenerator: FieldGenerator,
) {
  fun createConfiguredAirbyteCatalog(
    streamName: String,
    namespace: String?,
    vararg fields: Field,
  ): ConfiguredAirbyteCatalog =
    ConfiguredAirbyteCatalog().withStreams(
      listOf(createConfiguredAirbyteStream(streamName, namespace, *fields)),
    )

  fun createConfiguredAirbyteCatalog(
    streamName: String,
    namespace: String?,
    fields: List<Field>,
  ): ConfiguredAirbyteCatalog =
    ConfiguredAirbyteCatalog().withStreams(
      listOf(createConfiguredAirbyteStream(streamName, namespace, fields)),
    )

  fun createConfiguredAirbyteStream(
    streamName: String,
    namespace: String?,
    vararg fields: Field,
  ): ConfiguredAirbyteStream = createConfiguredAirbyteStream(streamName, namespace, listOf(*fields))

  fun createConfiguredAirbyteStream(
    streamName: String,
    namespace: String?,
    fields: List<Field>,
  ): ConfiguredAirbyteStream {
    val jsonSchema = fieldsToJsonSchema(fields)
    return ConfiguredAirbyteStream
      .Builder()
      .stream(AirbyteStream(streamName, jsonSchema, listOf(SyncMode.FULL_REFRESH)).withNamespace(namespace))
      .syncMode(SyncMode.FULL_REFRESH)
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .fields(fieldGenerator.getFieldsFromSchema(jsonSchema))
      .build()
  }

  companion object {
    @JvmStatic
    fun createAirbyteStream(
      streamName: String,
      vararg fields: Field,
    ): AirbyteStream {
      // Namespace is null since not all sources set it.
      return createAirbyteStream(streamName, null, listOf(*fields))
    }

    @JvmStatic
    fun createAirbyteStream(
      streamName: String,
      namespace: String?,
      vararg fields: Field,
    ): AirbyteStream = createAirbyteStream(streamName, namespace, listOf(*fields))

    fun createAirbyteStream(
      streamName: String,
      namespace: String?,
      fields: List<Field>,
    ): AirbyteStream = AirbyteStream(streamName, fieldsToJsonSchema(fields), listOf(SyncMode.FULL_REFRESH)).withNamespace(namespace)

    /**
     * Converts a [ConfiguredAirbyteCatalog] into an [AirbyteCatalog]. This is possible
     * because the latter is a subset of the former.
     *
     * @param configuredCatalog - catalog to convert
     * @return - airbyte catalog
     */
    @JvmStatic
    fun configuredCatalogToCatalog(configuredCatalog: ConfiguredAirbyteCatalog): AirbyteCatalog =
      AirbyteCatalog().withStreams(
        configuredCatalog.streams
          .stream()
          .map(ConfiguredAirbyteStream::stream)
          .map { obj: AirbyteStream -> obj.toProtocol() }
          .toList(),
      )

    /**
     * Extracts [StreamDescriptor] for a given [AirbyteStream].
     *
     * @param airbyteStream stream
     * @return stream descriptor
     */
    @JvmStatic
    fun extractDescriptor(airbyteStream: ConfiguredAirbyteStream): StreamDescriptor = extractDescriptor(airbyteStream.stream)

    /**
     * Extracts [StreamDescriptor] for a given [ConfiguredAirbyteStream].
     *
     * @param airbyteStream stream
     * @return stream descriptor
     */
    fun extractDescriptor(airbyteStream: AirbyteStream): StreamDescriptor = airbyteStream.streamDescriptor

    /**
     * Extracts [StreamDescriptor]s for each stream in a given [ConfiguredAirbyteCatalog].
     *
     * @param configuredCatalog catalog
     * @return list of stream descriptors
     */
    fun extractStreamDescriptors(configuredCatalog: ConfiguredAirbyteCatalog): List<StreamDescriptor> =
      configuredCatalog.streams
        .stream()
        .map { airbyteStream: ConfiguredAirbyteStream ->
          extractDescriptor(airbyteStream)
        }.toList()

    /**
     * Maps a list of fields into a JsonSchema object with names and types. This method will throw if it
     * receives multiple fields with the same name.
     *
     * @param fields fields to map to JsonSchema
     * @return JsonSchema representation of the fields.
     */
    fun fieldsToJsonSchema(fields: List<Field>): JsonNode {
      val properties =
        fields
          .asSequence()
          .associateBy({ it.name }) { field ->
            if (isObjectWithSubFields(field)) {
              fieldsToJsonSchema(field.subFields)
            } else {
              field.type.jsonSchemaTypeMap
            }
          }

      return Jsons.jsonNode(
        mapOf(
          "type" to "object",
          "properties" to properties,
        ),
      )
    }

    @JvmStatic
    fun fieldsToJsonSchema(vararg fields: Field): JsonNode = fieldsToJsonSchema(listOf(*fields))

    private fun isObjectWithSubFields(field: Field): Boolean =
      field.type == JsonSchemaType.OBJECT && field.subFields != null && !field.subFields.isEmpty()
  }
}
