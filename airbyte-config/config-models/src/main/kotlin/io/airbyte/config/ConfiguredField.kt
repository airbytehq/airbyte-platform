package io.airbyte.config

enum class FieldType(val type: String, val format: String? = null, val airbyteType: String? = null) {
  STRING("string"),
  BOOLEAN("boolean"),
  DATE("string", format = "date"),
  TIMESTAMP_WITHOUT_TIMEZONE("string", format = "date-time", airbyteType = "timestamp_without_timezone"),
  TIMESTAMP_WITH_TIMEZONE("string", format = "date-time", airbyteType = "timestamp_with_timezone"),
  TIME_WITHOUT_TIMEZONE("string", format = "time", airbyteType = "time_without_timezone"),
  TIME_WITH_TIMEZONE("string", format = "time", airbyteType = "time_with_timezone"),
  INTEGER("integer"),
  NUMBER("number"),
  ARRAY("array"),
  OBJECT("object"),
  MULTI("oneOf"),

  // This type is needed in order to support invalid configuration. The way the destination handles such schema is by storing the records related to
  // it as a blob. The platform should thus make sure that we don't remove those entries from the catalog.
  UNKNOWN("unknown"),
  ;

  fun toMap(): Map<String, String> {
    val result =
      mutableMapOf(
        "type" to type,
      )
    format?.let { result["format"] = it }
    airbyteType?.let { result["airbyte_type"] = it }

    return result
  }
}

/**
 * Field definition, intended to replace AirbyteStream.jsonSchema.
 */
data class Field(
  val name: String,
  val type: FieldType,
  // TODO in order to fully support nested configuration, we need to support Array, Object and Union.
  // This is required in order to be able to deprecate `ConfiguredAirbyteStream.stream.jsonSchema`
)
