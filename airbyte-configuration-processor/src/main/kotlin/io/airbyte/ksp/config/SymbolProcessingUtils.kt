/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.ksp.config

internal const val CONSTANT_PREFIX = "DEFAULT_"
internal const val VALUE = "value"
internal const val VALUE_SEPARATOR = ":"

internal val propertyPlaceholderRegEx = "\\$\\{(.*)}".toRegex()

internal fun parseProperty(propertyName: String) =
  if (propertyName.contains(
      VALUE_SEPARATOR,
    )
  ) {
    propertyName.split(VALUE_SEPARATOR)
  } else {
    listOf(propertyName, null)
  }

internal fun removePlaceholderSyntax(property: String) = property.replace(propertyPlaceholderRegEx) { m -> m.groupValues[1] }

internal fun convertStringToType(
  type: String,
  value: String,
) = when (type.lowercase()) {
  "boolean" -> value.toBoolean()
  "double" -> value.toDouble()
  "duration" -> value
  "float" -> value.replace(oldValue = "F", newValue = "", ignoreCase = true).toFloat()
  "int",
  "integer",
  -> value.toInt()
  "list" -> if (value == "emptyList()") emptyArray<Any>() else "listOf\\((.+)\\)".toRegex().find(value)?.groups[1]?.value
  "long" -> value.replace(oldValue = "L", newValue = "", ignoreCase = true).toLong()
  "path" -> value
  "uuid" -> value
  else -> value
}
