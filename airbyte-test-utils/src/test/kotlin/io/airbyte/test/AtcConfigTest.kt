/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class AtcConfigTest {
  private val objectMapper = jacksonObjectMapper().apply { disable(SerializationFeature.INDENT_OUTPUT) }

  @Test
  fun `override cursor should serialize with double quotes`() {
    val data =
      object : AtcData by AtcDataMovies {
        override fun cursor(): List<String> = listOf("test")
      }

    val config = AtcConfig(data = data)
    val json = objectMapper.writeValueAsString(config)

    Assertions.assertTrue(json.contains(""""custom_cursor":"\"test\""""))
  }

  @Test
  fun `override cursor should serialize multiple values with double quotes`() {
    val data =
      object : AtcData by AtcDataMovies {
        override fun cursor(): List<String> = listOf("abc", "xyz")
      }
    val config = AtcConfig(data = data)
    val json = objectMapper.writeValueAsString(config)

    Assertions.assertTrue(json.contains(""""custom_cursor":"\"abc,xyz\""""))
  }

  @Test
  fun `override cursor should not be included when null`() {
    val data =
      object : AtcData by AtcDataMovies {
        override fun cursor(): List<String> = listOf()
      }
    val config = AtcConfig(data = data)
    val json = objectMapper.writeValueAsString(config)

    // Should not include the field due to @JsonInclude(JsonInclude.Include.NON_NULL)
    Assertions.assertFalse(json.contains("custom_cursor"))
  }

  @Test
  fun `override required should serialize as comma-delimited string`() {
    val data =
      object : AtcData by AtcDataMovies {
        override fun required(): List<String> = listOf("abc", "xyz")
      }
    val config = AtcConfig(data = data)
    val json = objectMapper.writeValueAsString(config)

    Assertions.assertTrue(json.contains(""""custom_required":"\"abc,xyz\""""))
  }

  @Test
  fun `override required should serialize as null when empty`() {
    val data =
      object : AtcData by AtcDataMovies {
        override fun required(): List<String> = emptyList()
      }
    val config = AtcConfig(data = data)
    val json = objectMapper.writeValueAsString(config)

    // Should not include the field due to @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Assertions.assertFalse(json.contains("custom_required"))
  }

  @Test
  fun `override properties should serialize as nested type objects`() {
    val data =
      object : AtcData by AtcDataMovies {
        override fun properties() =
          mapOf(
            "a" to AtcDataProperty(type = "number"),
            "b" to AtcDataProperty(type = "string"),
            "c" to AtcDataProperty(type = "string", secret = true),
          )
      }
    val config = AtcConfig(data = data)
    val json = objectMapper.writeValueAsString(config)

    Assertions.assertTrue(
      json.contains(
        """"custom_properties":"{\"a\":{\"type\":\"number\"},\"b\":{\"type\":\"string\"},\"c\":{\"type\":\"string\",\"airbyte_secret\":true}}"""",
      ),
      "custom properties serialization doesn't match",
    )
  }

  @Test
  fun `override properties should not be included when empty`() {
    val data =
      object : AtcData by AtcDataMovies {
        override fun properties() = emptyMap<String, AtcDataProperty>()
      }
    val config = AtcConfig(data = data)
    val json = objectMapper.writeValueAsString(config)

    // Should not include the field due to @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Assertions.assertFalse(json.contains("custom_properties"))
  }

  @Test
  fun `override records should serialize as comma-delimited JSON strings`() {
    data class Record(
      val id: Int,
      val name: String,
    )

    val data =
      object : AtcData by AtcDataMovies {
        override fun records(): List<Any> = listOf(Record(id = 101, name = "foo"), Record(id = 202, name = "bar"))
      }
    val config = AtcConfig(data = data)
    val json = objectMapper.writeValueAsString(config)

    Assertions.assertTrue(json.contains(""""custom_records":"[{\"id\":101,\"name\":\"foo\"},{\"id\":202,\"name\":\"bar\"}]""""))
  }

  @Test
  fun `override records should not be included when empty`() {
    val data =
      object : AtcData by AtcDataMovies {
        override fun records(): List<Any> = emptyList()
      }
    val config = AtcConfig(data = data)
    val json = objectMapper.writeValueAsString(config)

    // Should not include the field due to @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Assertions.assertFalse(json.contains("custom_records"))
  }

  @Test
  fun `default values should serialize correctly`() {
    val config = AtcConfig()
    val json = objectMapper.writeValueAsString(config)

    Assertions.assertTrue(json.contains(""""dataset":"custom""""))
    Assertions.assertTrue(json.contains(""""read":"pass""""))
    Assertions.assertTrue(json.contains(""""check":"pass""""))
    Assertions.assertTrue(json.contains(""""write":"pass""""))
    Assertions.assertTrue(json.contains(""""discover":"pass""""))
  }
}
