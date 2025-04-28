/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.ConfiguredStreamMapper
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.text.Names
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.MapperConfig
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Singleton
import io.airbyte.api.client.model.generated.AirbyteCatalog as ClientAirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStream as ClientAirbyteStream
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration as ClientAirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration as ClientAirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.ConfiguredStreamMapper as ClientConfiguredStreamMapper
import io.airbyte.api.client.model.generated.SyncMode as ClientSyncMode
import io.airbyte.config.AirbyteStream as ConfigAirbyteStream
import io.airbyte.protocol.models.v0.AirbyteCatalog as ProtocolAirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream as ProtocolAirbyteStream

/**
 * Utilities to convert Catalog protocol to Catalog API client. This class is similar to the
 * existing logic in CatalogConverter.java; however, code can't be shared because the protocol model
 * is essentially converted to two different API models. Thus, if we need to change logic in either
 * place, we have to take care of the other one too.
 */
@Singleton
class CatalogClientConverters(
  private val fieldGenerator: FieldGenerator,
) {
  /**
   * Convert the API model to the Protocol model with configuration.
   *
   * @param catalog the API catalog
   * @return the protocol catalog
   */
  fun toConfiguredAirbyteInternal(catalog: ClientAirbyteCatalog): ConfiguredAirbyteCatalog {
    val airbyteStreams =
      catalog.streams
        .mapNotNull {
          runCatching { toConfiguredStreamInternal(it.stream!!, it.config!!) }
            .getOrNull()
        }

    return ConfiguredAirbyteCatalog().withStreams(airbyteStreams)
  }

  private fun toStreamInternal(
    stream: ClientAirbyteStream,
    config: ClientAirbyteStreamConfiguration,
  ): ConfigAirbyteStream {
    if (config.fieldSelectionEnabled == true) {
      val properties = stream.jsonSchema?.findValue("properties")
      if (properties == null || !properties.isObject) {
        throw JsonValidationException("Requested field selection but no properties node found")
      }

      val selectedFields = config.selectedFields ?: throw JsonValidationException("Requested field selection but no selected fields provided")
      selectedFields.forEach {
        val fieldPath = it.fieldPath ?: throw JsonValidationException("Selected field path cannot be empty")
        if (fieldPath.size != 1) {
          throw UnsupportedOperationException("Nested field selection not supported")
        }
      }

      val selectedFieldNames = selectedFields.mapNotNull { it.fieldPath?.firstOrNull() }.toSet()

      if (config.syncMode == SyncMode.INCREMENTAL &&
        config.cursorField?.isNotEmpty() == true &&
        config.cursorField!!.first() !in selectedFieldNames
      ) {
        throw JsonValidationException("Cursor field cannot be de-selected in INCREMENTAL syncs")
      }

      if (config.destinationSyncMode == DestinationSyncMode.APPEND_DEDUP) {
        config.primaryKey?.forEach {
          if (it.first() !in selectedFieldNames) {
            throw JsonValidationException("Primary key field cannot be de-selected in DEDUP mode")
          }
        }
      }

      selectedFieldNames.forEach {
        if (!properties.has(it)) {
          throw JsonValidationException("Requested selected field $it not found in JSON schema")
        }
      }

      (properties as ObjectNode).retain(selectedFieldNames)
    }

    return ConfigAirbyteStream(
      name = stream.name,
      jsonSchema = stream.jsonSchema!!,
      supportedSyncModes = Enums.convertListTo(stream.supportedSyncModes, io.airbyte.config.SyncMode::class.java),
      sourceDefinedCursor = stream.sourceDefinedCursor,
      defaultCursorField = stream.defaultCursorField,
      sourceDefinedPrimaryKey = stream.sourceDefinedPrimaryKey ?: emptyList(),
      namespace = stream.namespace,
      isResumable = stream.isResumable,
      isFileBased = stream.isFileBased,
    )
  }

  private fun toModel(mapper: ClientConfiguredStreamMapper): io.airbyte.api.model.generated.ConfiguredStreamMapper =
    io.airbyte.api.model.generated
      .ConfiguredStreamMapper()
      .id(mapper.id)
      .type(
        io.airbyte.api.model.generated.StreamMapperType
          .fromValue(mapper.type.value),
      ).mapperConfiguration(mapper.mapperConfiguration)

  private fun toConfiguredMappers(mapperConfigs: List<ConfiguredStreamMapper>?): List<MapperConfig> =
    mapperConfigs?.map { toModel(it).toInternal() } ?: emptyList()

  private fun toConfiguredStreamInternal(
    stream: ClientAirbyteStream,
    config: AirbyteStreamConfiguration,
  ): ConfiguredAirbyteStream {
    val convertedStream = toStreamInternal(stream, config)
    return ConfiguredAirbyteStream
      .Builder()
      .stream(convertedStream)
      .syncMode(Enums.convertTo(config.syncMode, io.airbyte.config.SyncMode::class.java))
      .destinationSyncMode(
        Enums.convertTo(
          config.destinationSyncMode,
          io.airbyte.config.DestinationSyncMode::class.java,
        ),
      ).primaryKey(config.primaryKey)
      .cursorField(config.cursorField)
      .generationId(config.generationId)
      .minimumGenerationId(config.minimumGenerationId)
      .syncId(config.syncId)
      .includeFiles(config.includeFiles ?: false)
      .fields(fieldGenerator.getFieldsFromSchema(convertedStream.jsonSchema))
      .mappers(toConfiguredMappers(config.mappers))
      .build()
  }

  /**
   * Converts a protocol AirbyteCatalog to an OpenAPI client versioned AirbyteCatalog.
   */
  fun toAirbyteCatalogClientApi(catalog: ProtocolAirbyteCatalog): ClientAirbyteCatalog =
    ClientAirbyteCatalog(
      catalog.streams.map { it.toAirbyteStreamClientApi() }.map { s ->
        ClientAirbyteStreamAndConfiguration(
          s,
          generateDefaultConfiguration(s),
        )
      },
    )

  private fun generateDefaultConfiguration(stream: ClientAirbyteStream): ClientAirbyteStreamConfiguration =
    ClientAirbyteStreamConfiguration(
      syncMode =
        if (stream.supportedSyncModes?.isNotEmpty() == true) {
          Enums.convertTo(
            stream.supportedSyncModes!!.first(),
            ClientSyncMode::class.java,
          )
        } else {
          io.airbyte.api.client.model.generated.SyncMode.INCREMENTAL
        },
      destinationSyncMode = io.airbyte.api.client.model.generated.DestinationSyncMode.APPEND,
      cursorField = stream.defaultCursorField,
      primaryKey = stream.sourceDefinedPrimaryKey,
      aliasName = Names.toAlphanumericAndUnderscore(stream.name),
      selected = true,
      includeFiles = false,
      suggested = null,
      fieldSelectionEnabled = null,
      selectedFields = null,
      hashedFields = null,
      mappers = null,
      minimumGenerationId = null,
      generationId = null,
      syncId = null,
    )
}

private fun ProtocolAirbyteStream.toAirbyteStreamClientApi(): ClientAirbyteStream =
  ClientAirbyteStream(
    name = name,
    jsonSchema = jsonSchema,
    supportedSyncModes =
      Enums.convertListTo(
        supportedSyncModes,
        io.airbyte.api.client.model.generated.SyncMode::class.java,
      ),
    sourceDefinedCursor = sourceDefinedCursor,
    defaultCursorField = defaultCursorField,
    sourceDefinedPrimaryKey = sourceDefinedPrimaryKey,
    namespace = namespace,
    isResumable = isResumable,
    isFileBased = isFileBased,
  )
