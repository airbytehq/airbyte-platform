package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.JsonsSchemaConstants.PROPERTIES
import io.airbyte.config.JsonsSchemaConstants.TYPE
import io.airbyte.config.JsonsSchemaConstants.TYPE_OBJECT
import io.airbyte.config.StreamDescriptor
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

val log = KotlinLogging.logger {}

@Singleton
class DestinationCatalogGenerator(
  val mappers: List<Mapper>,
) {
  private val mappersByName = mappers.associateBy { it.name }

  data class CatalogGenerationResult(
    val catalog: ConfiguredAirbyteCatalog,
    val errors: Map<StreamDescriptor, Map<ConfiguredMapper, MapperError>>,
  )

  /**
   * Apply the mapper transformations to the catalog in order to generate the destination catalog.
   * It won't modify tbe input catalog, it creates a copy of the configure catalog, then mutate the copy and then returns it.
   */
  fun generateDestinationCatalog(inputCatalog: ConfiguredAirbyteCatalog): CatalogGenerationResult {
    val resultCatalog = Jsons.clone(inputCatalog)

    return resultCatalog.streams.fold(CatalogGenerationResult(resultCatalog, mapOf())) { acc, it ->
      val errors = applyCatalogMapperTransformations(it)
      CatalogGenerationResult(resultCatalog, acc.errors + Pair(it.stream.streamDescriptor, errors))
    }
  }

  private fun applyCatalogMapperTransformations(stream: ConfiguredAirbyteStream): Map<ConfiguredMapper, MapperError> {
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
    stream.stream.jsonSchema = Jsons.deserialize(jsonSchema)
    stream.cursorField = updateFields.cursor
    stream.primaryKey = updateFields.primaryKey
    stream.stream.sourceDefinedPrimaryKey = updateFields.sourceDefinedPrimaryKey
    stream.stream.defaultCursorField = updateFields.sourceDefaultCursor

    return errors
  }

  data class MapperToFieldAccumulator(
    val slimStream: SlimStream,
    val validConfig: List<ConfiguredMapper>,
    val errors: Map<ConfiguredMapper, MapperError>,
  )

  enum class MapperError {
    MISSING_MAPPER,
    INVALID_MAPPER_CONFIG,
  }

  internal fun applyMapperToFields(stream: ConfiguredAirbyteStream): MapperToFieldAccumulator {
    val result =
      stream.mappers.map {
        Pair(
          mappersByName[it.name],
          it,
        )
      }
        .fold(
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
        ) {
            mapperAcc, (mapperInstance, configuredMapper) ->
          if (mapperInstance == null) {
            log.warn { "Trying to use a mapper named ${configuredMapper.name} which doesn't have a known implementation. The mapper won't be apply" }
            mapperAcc.copy(errors = mapperAcc.errors + Pair(configuredMapper, MapperError.MISSING_MAPPER))
          } else {
            try {
              mapperAcc.copy(
                slimStream = mapperInstance.schema(configuredMapper, mapperAcc.slimStream),
                validConfig = mapperAcc.validConfig + configuredMapper,
              )
            } catch (e: Exception) {
              log.warn {
                "Trying to use a mapper named ${configuredMapper.name} which failed to resolve its schema for the config:" +
                  " ${configuredMapper.config}. The mapper won't be apply"
              }
              mapperAcc.copy(errors = mapperAcc.errors + Pair(configuredMapper, MapperError.INVALID_MAPPER_CONFIG))
            }
          }
        }

    stream.mappers = result.validConfig

    return result
  }

  internal fun generateJsonSchemaFromFields(
    fields: List<Field>,
    jsonSchema: JsonNode,
  ): String {
    return Jsons.serialize(
      fields.associate {
        if (arrayOf(FieldType.OBJECT, FieldType.ARRAY, FieldType.MULTI, FieldType.UNKNOWN).contains(it.type)) {
          Pair(it.name, jsonSchema.get(PROPERTIES).get(it.name))
        } else {
          Pair(it.name, Jsons.jsonNode(it.type.toMap()))
        }
      },
    )
  }
}
