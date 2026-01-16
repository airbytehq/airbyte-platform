/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

/**
 * Acceptance Test Connector Config
 * @param dataset Can override to specify a dataset that is included in the acceptance test connector, but you really shouldn't.
 * Instead, leave as `custom` and provide a different [AtcData] implementation.
 * @param data the data this connector will either send (if source) or should expect to receive (if destination).
 * Defaults to [AtcDataMovies].
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
data class AtcConfig(
  val read: PassFail = PassFail.Pass,
  val check: PassFail = PassFail.Pass,
  val write: PassFail = PassFail.Pass,
  val discover: PassFail = PassFail.Pass,
  val dataset: String = "custom",
  @JsonIgnore
  val data: AtcData = AtcDataMovies,
) {
  @JsonProperty("custom_cursor")
  @JsonSerialize(using = CursorRequiredSerializer::class)
  fun cursor() = data.cursor()

  @JsonProperty("custom_required")
  @JsonSerialize(using = CursorRequiredSerializer::class)
  fun required() = data.required()

  @JsonSerialize(using = CustomPropertiesSerializer::class)
  @JsonProperty("custom_properties")
  fun properties() = data.properties()

  @JsonProperty("custom_records")
  @JsonSerialize(using = CustomRecordsSerializer::class)
  fun records() = data.records()
}

enum class PassFail {
  @JsonProperty("pass")
  Pass,

  @JsonProperty("fail")
  Fail,
}

/**
 * Custom serializer that converts a list of strings to a comma-delimited string
 */
private class CursorRequiredSerializer : JsonSerializer<List<String>>() {
  override fun serialize(
    value: List<String>?,
    gen: JsonGenerator,
    serializers: SerializerProvider,
  ) {
    if (value == null || value.isEmpty()) {
      return
    }

    gen.writeString(value.joinToString(separator = ",", prefix = "\"", postfix = "\""))
  }

  override fun isEmpty(
    provider: SerializerProvider?,
    value: List<String>?,
  ): Boolean = value == null || value.isEmpty()
}

/**
 * Custom serializer that converts a map of string to string into a nested type structure
 */
private class CustomPropertiesSerializer : JsonSerializer<Map<String, AtcDataProperty>>() {
  override fun serialize(
    value: Map<String, AtcDataProperty>?,
    gen: JsonGenerator,
    serializers: SerializerProvider,
  ) {
    if (value == null || value.isEmpty()) {
      return
    }
    val jsonString = jacksonObjectMapper().writeValueAsString(value)
    gen.writeString(jsonString)
  }

  override fun isEmpty(
    provider: SerializerProvider?,
    value: Map<String, AtcDataProperty>?,
  ): Boolean = value == null || value.isEmpty()
}

/**
 * Custom serializer that converts a list of objects to JSON strings and joins them with commas
 */
private class CustomRecordsSerializer : JsonSerializer<List<Any>>() {
  private val objectMapper = jacksonObjectMapper()

  override fun serialize(
    value: List<Any>?,
    gen: JsonGenerator,
    serializers: SerializerProvider,
  ) {
    if (value == null || value.isEmpty()) {
      return
    }

    val jsonStrings = value.map { objectMapper.writeValueAsString(it) }
    gen.writeString(jsonStrings.joinToString(separator = ",", prefix = "[", postfix = "]"))
  }

  override fun isEmpty(
    provider: SerializerProvider?,
    value: List<Any>?,
  ): Boolean = value == null || value.isEmpty()
}
