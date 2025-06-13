/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

object JsonsSchemaConstants {
  const val TYPE = "type"
  const val FORMAT = "format"
  const val AIRBYTE_TYPE = "airbyte_type"
  const val PROPERTIES = "properties"
  const val REQUIRED = "required"
  const val ADDITIONAL_PROPERTIES = "additionalProperties"

  const val TYPE_STRING = "string"
  const val TYPE_BOOLEAN = "boolean"
  const val TYPE_INTEGER = "integer"
  const val TYPE_NUMBER = "number"
  const val TYPE_ARRAY = "array"
  const val TYPE_OBJECT = "object"
  const val TYPE_ONE_OF = "oneOf"
  const val TYPE_UNKNOWN = "unknown"

  const val FORMAT_DATE = "date"
  const val FORMAT_DATE_TIME = "date-time"
  const val FORMAT_TIME = "time"

  const val AIRBYTE_TYPE_TIMESTAMP_WITHOUT_TIMEZONE = "timestamp_without_timezone"
  const val AIRBYTE_TYPE_TIMESTAMP_WITH_TIMEZONE = "timestamp_with_timezone"
  const val AIRBYTE_TYPE_TIME_WITHOUT_TIMEZONE = "time_without_timezone"
  const val AIRBYTE_TYPE_TIME_WITH_TIMEZONE = "time_with_timezone"
  const val AIRBYTE_TYPE_INTEGER = "integer"
}
