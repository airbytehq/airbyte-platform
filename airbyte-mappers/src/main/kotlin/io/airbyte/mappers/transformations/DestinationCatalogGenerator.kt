/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.JsonsSchemaConstants.PROPERTIES
import io.airbyte.config.JsonsSchemaConstants.TYPE
import io.airbyte.config.JsonsSchemaConstants.TYPE_OBJECT
import io.airbyte.config.MapperConfig
import io.airbyte.config.StreamDescriptor
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

val log = KotlinLogging.logger {}

@Singleton
class DestinationCatalogGenerator(
  val mappers: List<Mapper<out MapperConfig>>,
  private val objectMapper: ObjectMapper,
) {
  private val mappersByName = mappers.associateBy { it.name }

  enum class MapperErrorType {
    MISSING_MAPPER,
    INVALID_MAPPER_CONFIG,
    FIELD_NOT_FOUND,
    FIELD_ALREADY_EXISTS,
  }

  data class MapperError(
    val type: MapperErrorType,
    val message: String,
  )

  data class CatalogGenerationResult(
    val catalog: ConfiguredAirbyteCatalog,
    val errors: Map<StreamDescriptor, Map<MapperConfig, MapperError>>,
  )

  /**
   * Apply the mapper transformations to the catalog in order to generate the destination catalog.
   * It won't modify tbe input catalog, it creates a copy of the configure catalog, then mutate the copy and then returns it.
   */
  fun generateDestinationCatalog(inputCatalog: ConfiguredAirbyteCatalog): CatalogGenerationResult {
    val resultCatalog = objectMapper.readValue(objectMapper.writeValueAsString(inputCatalog), ConfiguredAirbyteCatalog::class.java)

    return resultCatalog.streams.fold(CatalogGenerationResult(resultCatalog, mapOf())) { acc, it ->
      val errors = applyCatalogMapperTransformations(it)
      CatalogGenerationResult(resultCatalog, acc.errors + Pair(it.stream.streamDescriptor, errors))
    }
  }

  private fun applyCatalogMapperTransformations(stream: ConfiguredAirbyteStream): Map<MapperConfig, MapperError> {
    val (updateFields, _, errors) = applyMapperToFields(stream)

    val jsonSchema =
      """ 
      { 
        "$TYPE": "$TYPE_OBJECT", 
        "${'$'}schema": "http://json-schema.org/schema#", 
        "$PROPERTIES":
          ${generateJsonSchemaFromFields(updateFields.fields, stream.stream.jsonSchema)},
        "additionalProperties": true
      } 
      """.trimIndent()
    stream.fields = updateFields.fields
    stream.stream.jsonSchema = objectMapper.readValue(jsonSchema, JsonNode::class.java)
    stream.cursorField = updateFields.cursor
    stream.primaryKey = updateFields.primaryKey
    stream.stream.sourceDefinedPrimaryKey = updateFields.sourceDefinedPrimaryKey
    stream.stream.defaultCursorField = updateFields.sourceDefaultCursor

    return errors
  }

  data class MapperToFieldAccumulator(
    val slimStream: SlimStream,
    val validConfig: List<MapperConfig>,
    val errors: Map<MapperConfig, MapperError>,
  )

  internal fun applyMapperToFields(stream: ConfiguredAirbyteStream): MapperToFieldAccumulator {
    val result =
      stream.mappers
        .map {
          Pair(
            mappersByName[it.name()],
            it,
          )
        }.fold(
          MapperToFieldAccumulator(
            SlimStream(
              fields = stream.fields ?: listOf(),
              cursor = stream.cursorField,
              primaryKey = stream.primaryKey,
              sourceDefinedPrimaryKey = stream.stream.sourceDefinedPrimaryKey,
              sourceDefaultCursor = stream.stream.defaultCursorField,
            ),
            listOf(),
            mapOf(),
          ),
        ) { mapperAcc, (mapperInstance, mapperConfig) ->
          if (mapperInstance == null) {
            log.warn { "Trying to use a mapper named ${mapperConfig.name()} which doesn't have a known implementation. The mapper won't be apply" }
            mapperAcc.copy(
              errors =
                mapperAcc.errors +
                  Pair(
                    mapperConfig,
                    MapperError(type = MapperErrorType.MISSING_MAPPER, message = "Cannot find mapper ${mapperConfig.name()}, ignoring."),
                  ),
            )
          } else {
            try {
              mapperAcc.copy(
                slimStream = (mapperInstance as Mapper<MapperConfig>).schema(mapperConfig, mapperAcc.slimStream),
                validConfig = mapperAcc.validConfig + mapperConfig,
              )
            } catch (e: MapperException) {
              log.error(e) {
                "Trying to use a mapper named ${mapperConfig.name()} which failed to resolve its schema for the config:" +
                  " ${mapperConfig.config()}. The mapper won't be applied"
              }
              mapperAcc.copy(errors = mapperAcc.errors + Pair(mapperConfig, MapperError(type = e.type, message = e.message ?: "Unexpected error")))
            } catch (e: Exception) {
              log.error(e) {
                "Trying to use a mapper named ${mapperConfig.name()} which failed to resolve its schema for the config:" +
                  " ${mapperConfig.config()}. The mapper won't be apply"
              }
              mapperAcc.copy(
                errors =
                  mapperAcc.errors +
                    Pair(
                      mapperConfig,
                      MapperError(type = MapperErrorType.INVALID_MAPPER_CONFIG, message = e.message ?: "Unexpected error"),
                    ),
              )
            }
          }
        }

    stream.mappers = result.validConfig

    return result
  }

  internal fun generateJsonSchemaFromFields(
    fields: List<Field>,
    jsonSchema: JsonNode,
  ): String =
    objectMapper.writeValueAsString(
      fields.associate {
        if (arrayOf(FieldType.OBJECT, FieldType.ARRAY, FieldType.MULTI, FieldType.UNKNOWN).contains(it.type)) {
          Pair(it.name, jsonSchema.get(PROPERTIES).get(it.name))
        } else {
          Pair(it.name, objectMapper.valueToTree(it.type.toMap()))
        }
      },
    )
}
