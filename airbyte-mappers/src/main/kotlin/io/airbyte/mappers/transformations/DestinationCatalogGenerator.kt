package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import jakarta.inject.Singleton

@Singleton
class DestinationCatalogGenerator(val mappers: List<Mapper>) {
  private val mappersByName = mappers.associateBy { it.name }

  /**
   * Apply the mapper transformations to the catalog in order to generate the destination catalog.
   * It won't modify tbe input catalog, it creates a copy of the configure catalog, then mutate the copy and then returns it.
   */
  fun generateDestinationCatalog(inputCatalog: ConfiguredAirbyteCatalog): ConfiguredAirbyteCatalog {
    val resultCatalog = Jsons.clone(inputCatalog)

    resultCatalog.streams.map {
      applyCatalogMapperTransformations(it)
    }

    return resultCatalog
  }

  internal fun applyCatalogMapperTransformations(stream: ConfiguredAirbyteStream) {
    val updateFields = applyMapperToFields(stream)

    val jsonSchema =
      """ 
      { 
        "type": "object", 
        "${'$'}schema": "http://json-schema.org/schema#", 
        "properties":
          ${generateJsonSchemaFromFields(updateFields, stream.stream.jsonSchema)}  
      } 
      """.trimIndent()
    stream.fields = updateFields
    stream.stream.jsonSchema = Jsons.deserialize(jsonSchema)
  }

  internal fun applyMapperToFields(stream: ConfiguredAirbyteStream): List<Field> {
    return stream.mappers.map {
      Pair(
        mappersByName[it.name] ?: throw IllegalStateException("Unexpected mapper operation: ${it.name}"),
        it,
      )
    }
      .fold(stream.fields ?: listOf()) { fields, (mapperInstance, configuredMapper) ->
        mapperInstance!!.schema(configuredMapper, fields)
      }
  }

  internal fun fieldSerialization(
    field: Field,
    jsonSchema: JsonNode,
  ): String {
    return if (arrayOf(FieldType.OBJECT, FieldType.ARRAY, FieldType.MULTI).contains(field.type)) {
      jsonSchema.get(field.name).toString()
    } else {
      Jsons.serialize(field.type.toMap())
    }
  }

  internal fun generateJsonSchemaFromFields(
    fields: List<Field>,
    jsonSchema: JsonNode,
  ): String {
    return Jsons.serialize(
      fields.associate {
        if (arrayOf(FieldType.OBJECT, FieldType.ARRAY, FieldType.MULTI).contains(it.type)) {
          Pair(it.name, jsonSchema.get(it.name))
        } else {
          Pair(it.name, Jsons.jsonNode(it.type.toMap()))
        }
      },
    )
  }
}
