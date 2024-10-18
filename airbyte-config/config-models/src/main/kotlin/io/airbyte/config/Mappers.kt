package io.airbyte.config

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.commons.json.Jsons

object MapperOperationName {
  const val HASHING = "hashing"
  const val FIELD_RENAMING = "field-renaming"
}

/**
 * Data model to describe the interfaces of mappers.
 *
 * Intent is to power the UI and eventually mapper validation
 */
typealias MapperSpecificationFieldName = String

@JsonDeserialize(using = MapperSpecificationFieldDeserializer::class)
interface MapperSpecificationField<out T : Any> {
  val title: String
  val description: String
  val type: String
  val examples: List<T>?
  val default: T?
}

@JsonDeserialize(`as` = MapperSpecificationFieldString::class)
data class MapperSpecificationFieldString(
  override val title: String,
  override val description: String,
  override val examples: List<String>? = null,
  override val default: String? = null,
) : MapperSpecificationField<String> {
  override val type = "string"
}

@JsonDeserialize(`as` = MapperSpecificationFieldInt::class)
data class MapperSpecificationFieldInt(
  override val title: String,
  override val description: String,
  override val examples: List<Int>?,
  override val default: Int?,
  val minimum: Int?,
  val maximum: Int?,
) : MapperSpecificationField<Int> {
  override val type = "integer"
}

@JsonDeserialize(`as` = MapperSpecificationFieldEnum::class)
data class MapperSpecificationFieldEnum(
  override val title: String,
  override val description: String,
  val enum: List<String>,
  override val default: String?,
  override val examples: List<String>?,
) : MapperSpecificationField<String> {
  override val type = "string"
}

class MapperSpecificationFieldDeserializer : JsonDeserializer<MapperSpecificationField<Any>>() {
  override fun deserialize(
    jsonParser: JsonParser,
    deserializationContext: DeserializationContext,
  ): MapperSpecificationField<Any> {
    val mapper = jsonParser.codec as ObjectMapper
    val root: ObjectNode = mapper.readTree(jsonParser)

    if (root.has("type")) {
      when (root.get("type").asText()) {
        "string" -> {
          return if (root.has("enum")) {
            Jsons.deserialize(root.toString(), MapperSpecificationFieldEnum::class.java)
          } else {
            Jsons.deserialize(root.toString(), MapperSpecificationFieldString::class.java)
          }
        }
        "integer" -> {
          return Jsons.deserialize(root.toString(), MapperSpecificationFieldInt::class.java)
        }
      }
    }

    throw IllegalStateException("Deserializing an unexpected object")
  }
}

data class MapperSpecification(
  val name: String,
  val documentationUrl: String?,
  val config: Map<MapperSpecificationFieldName, MapperSpecificationField<Any>>,
)

/**
 * Configured mapper we want to apply.
 */
data class ConfiguredMapper(
  val name: String,
  val config: Map<String, String>,
)
