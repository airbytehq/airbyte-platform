/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE_TIMESTAMP_WITHOUT_TIMEZONE
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE_TIMESTAMP_WITH_TIMEZONE
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE_TIME_WITHOUT_TIMEZONE
import io.airbyte.config.JsonsSchemaConstants.AIRBYTE_TYPE_TIME_WITH_TIMEZONE
import io.airbyte.config.JsonsSchemaConstants.FORMAT
import io.airbyte.config.JsonsSchemaConstants.FORMAT_DATE
import io.airbyte.config.JsonsSchemaConstants.FORMAT_DATE_TIME
import io.airbyte.config.JsonsSchemaConstants.FORMAT_TIME
import io.airbyte.config.JsonsSchemaConstants.TYPE
import io.airbyte.config.JsonsSchemaConstants.TYPE_ARRAY
import io.airbyte.config.JsonsSchemaConstants.TYPE_BOOLEAN
import io.airbyte.config.JsonsSchemaConstants.TYPE_INTEGER
import io.airbyte.config.JsonsSchemaConstants.TYPE_NUMBER
import io.airbyte.config.JsonsSchemaConstants.TYPE_OBJECT
import io.airbyte.config.JsonsSchemaConstants.TYPE_ONE_OF
import io.airbyte.config.JsonsSchemaConstants.TYPE_STRING
import io.airbyte.config.JsonsSchemaConstants.TYPE_UNKNOWN

enum class FieldType(
  val type: String,
  val format: String? = null,
  val airbyteType: String? = null,
) {
  STRING(TYPE_STRING),
  BOOLEAN(TYPE_BOOLEAN),
  DATE(TYPE_STRING, format = FORMAT_DATE),
  TIMESTAMP_WITHOUT_TIMEZONE(TYPE_STRING, format = FORMAT_DATE_TIME, airbyteType = AIRBYTE_TYPE_TIMESTAMP_WITHOUT_TIMEZONE),
  TIMESTAMP_WITH_TIMEZONE(TYPE_STRING, format = FORMAT_DATE_TIME, airbyteType = AIRBYTE_TYPE_TIMESTAMP_WITH_TIMEZONE),
  TIME_WITHOUT_TIMEZONE(TYPE_STRING, format = FORMAT_TIME, airbyteType = AIRBYTE_TYPE_TIME_WITHOUT_TIMEZONE),
  TIME_WITH_TIMEZONE(TYPE_STRING, format = FORMAT_TIME, airbyteType = AIRBYTE_TYPE_TIME_WITH_TIMEZONE),
  INTEGER(TYPE_INTEGER),
  NUMBER(TYPE_NUMBER),
  ARRAY(TYPE_ARRAY),
  OBJECT(TYPE_OBJECT),
  MULTI(TYPE_ONE_OF),

  // This type is needed in order to support invalid configuration. The way the destination handles such schema is by storing the records related to
  // it as a blob. The platform should thus make sure that we don't remove those entries from the catalog.
  UNKNOWN(TYPE_UNKNOWN),
  ;

  fun toMap(): Map<String, String> {
    val result =
      mutableMapOf(
        TYPE to type,
      )
    format?.let { result[FORMAT] = it }
    airbyteType?.let { result[AIRBYTE_TYPE] = it }

    return result
  }
}

/**
 * Field definition, intended to replace AirbyteStream.jsonSchema.
 */
data class Field(
  val name: String,
  val type: FieldType,
  val required: Boolean = false,
  // TODO in order to fully support nested configuration, we need to support Array, Object and Union.
  // This is required in order to be able to deprecate `ConfiguredAirbyteStream.stream.jsonSchema`
)
