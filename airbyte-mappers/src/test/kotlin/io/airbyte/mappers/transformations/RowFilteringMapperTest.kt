/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.contains
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.adapters.AirbyteJsonRecordAdapter
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.config.mapper.configs.RowFilteringMapperConfig
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Instant

@MicronautTest
class RowFilteringMapperTest {
  @Inject
  lateinit var mapper: RowFilteringMapper

  companion object {
    private const val CONDITIONS = "conditions"
    private const val NAMESPACE = "NAMESPACE"
    private const val STREAM = "STREAM"
  }

  private fun getRecord(recordData: Map<String, Any>): AirbyteRecord =
    AirbyteJsonRecordAdapter(
      AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(
          AirbyteRecordMessage()
            .withNamespace(NAMESPACE)
            .withStream(STREAM)
            .withEmittedAt(Instant.now().toEpochMilli())
            .withData(Jsons.jsonNode(recordData)),
        ),
    )

  private fun assertNoChangesMadeToRecord(
    record: AirbyteRecord,
    expectedData: Map<String, Any>,
  ) {
    expectedData.forEach { (key, value) ->
      Assertions.assertTrue(record.has(key), "Field '$key' should exist in the record")
      Assertions.assertEquals(value.toString(), record.get(key).asString())
    }
  }

  @Test
  fun `should handle simple comparison condition for exclusion`() {
    val jsonString = """
            {
                "name": "row-filtering",
                "config": {
                    "conditions": {
                        "type": "NOT",
                        "conditions": [
                            {
                                "type": "EQUAL",
                                "fieldName": "status",
                                "comparisonValue": "active"
                            }
                        ]
                    }
                }
            }
            """
    val rowFilteringMapperConfig = Jsons.deserialize(jsonString, RowFilteringMapperConfig::class.java)
    val rowFilteringMapperConfigDeserializedFromSpec =
      mapper
        .spec()
        .deserialize(Jsons.deserialize(jsonString, ConfiguredMapper::class.java))
    Assertions.assertEquals(rowFilteringMapperConfig, rowFilteringMapperConfigDeserializedFromSpec)

    val recordToExcludeData =
      mapOf(
        "status" to "active",
        "otherField" to "otherValue",
      )
    val recordToExclude = getRecord(recordToExcludeData)

    mapper.map(rowFilteringMapperConfig, recordToExclude)

    Assertions.assertFalse(recordToExclude.shouldInclude(), "Record should be discarded")
    assertNoChangesMadeToRecord(recordToExclude, recordToExcludeData)

    val recordToIncludeData =
      mapOf(
        "status" to "inactive",
        "otherField" to "otherValue2",
      )
    val recordToInclude = getRecord(recordToIncludeData)

    mapper.map(rowFilteringMapperConfig, recordToInclude)

    Assertions.assertTrue(recordToInclude.shouldInclude(), "Record should not be discarded")
    assertNoChangesMadeToRecord(recordToInclude, recordToIncludeData)
  }

  @Test
  fun `should handle simple comparison condition`() {
    val jsonString = """
            {
                "name": "row-filtering",
                "config": {
                    "conditions": {
                        "type": "EQUAL",
                        "fieldName": "status",
                        "comparisonValue": "active"
                    }
                }
            }
            """
    val rowFilteringMapperConfig = Jsons.deserialize(jsonString, RowFilteringMapperConfig::class.java)
    val rowFilteringMapperConfigDeserializedFromSpec =
      mapper
        .spec()
        .deserialize(Jsons.deserialize(jsonString, ConfiguredMapper::class.java))
    Assertions.assertEquals(rowFilteringMapperConfig, rowFilteringMapperConfigDeserializedFromSpec)

    val recordToIncludeData =
      mapOf(
        "status" to "active",
        "otherField" to "otherValue",
      )
    val recordToInclude = getRecord(recordToIncludeData)

    mapper.map(rowFilteringMapperConfig, recordToInclude)

    Assertions.assertTrue(recordToInclude.shouldInclude(), "Record should not be discarded")
    assertNoChangesMadeToRecord(recordToInclude, recordToIncludeData)

    val recordToExcludeData =
      mapOf(
        "status" to "inactive",
        "otherField" to "otherValue2",
      )
    val recordToExclude = getRecord(recordToExcludeData)

    mapper.map(rowFilteringMapperConfig, recordToExclude)

    Assertions.assertFalse(recordToExclude.shouldInclude(), "Record should be discarded")
    assertNoChangesMadeToRecord(recordToExclude, recordToExcludeData)
  }

  @Test
  @Disabled("Disabled cause we are not exposing AND/OR operator to the end user for now, re-enable when making that change")
  fun `should handle logical condition with multiple comparisons`() {
    val jsonString = """
            {
                "name": "row-filtering",
                "config": {
                    "conditions": {
                        "type": "AND",
                        "conditions": [
                            {
                                "type": "EQUAL",
                                "fieldName": "status",
                                "comparisonValue": "active"
                            },
                            {
                                "type": "NOT",
                                "conditions": [
                                    {
                                        "type": "EQUAL",
                                        "fieldName": "region",
                                        "comparisonValue": "eu"
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
            """
    val rowFilteringMapperConfig = Jsons.deserialize(jsonString, RowFilteringMapperConfig::class.java)
    val rowFilteringMapperConfigDeserializedFromSpec =
      mapper
        .spec()
        .deserialize(Jsons.deserialize(jsonString, ConfiguredMapper::class.java))
    Assertions.assertEquals(rowFilteringMapperConfig, rowFilteringMapperConfigDeserializedFromSpec)

    val recordToIncludeData =
      mapOf(
        "status" to "active",
        "region" to "us",
        "otherField" to "otherValue",
      )
    val recordToInclude = getRecord(recordToIncludeData)

    mapper.map(rowFilteringMapperConfig, recordToInclude)

    Assertions.assertTrue(recordToInclude.shouldInclude(), "Record should not be discarded")
    assertNoChangesMadeToRecord(recordToInclude, recordToIncludeData)

    val recordToExcludeData =
      mapOf(
        "status" to "active",
        "region" to "eu",
        "otherField" to "otherValue2",
      )
    val recordToExclude = getRecord(recordToExcludeData)

    mapper.map(rowFilteringMapperConfig, recordToExclude)

    Assertions.assertFalse(recordToExclude.shouldInclude(), "Record should be discarded")
    assertNoChangesMadeToRecord(recordToExclude, recordToExcludeData)
  }

  @Test
  @Disabled("Disabled cause we are not exposing AND/OR operator to the end user for now, re-enable when making that change")
  fun `should handle nested logical conditions`() {
    val jsonString = """
    {
        "name": "row-filtering",
        "config": {
            "conditions": {
                "type": "NOT",
                "conditions": [
                    {
                        "type": "OR",
                        "conditions": [
                            {
                                "type": "EQUAL",
                                "fieldName": "region",
                                "comparisonValue": "us"
                            },
                            {
                                "type": "AND",
                                "conditions": [
                                    {
                                        "type": "EQUAL",
                                        "fieldName": "status",
                                        "comparisonValue": "inactive"
                                    },
                                    {
                                        "type": "EQUAL",
                                        "fieldName": "region",
                                        "comparisonValue": "eu"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        }
    }
            """
    val rowFilteringMapperConfig =
      Jsons.deserialize(
        jsonString,
        RowFilteringMapperConfig::class.java,
      )
    val rowFilteringMapperConfigDeserializedFromSpec =
      mapper
        .spec()
        .deserialize(Jsons.deserialize(jsonString, ConfiguredMapper::class.java))

    Assertions.assertEquals(rowFilteringMapperConfig, rowFilteringMapperConfigDeserializedFromSpec)

    val recordToExcludeData1 =
      mapOf(
        "status" to "active",
        "region" to "us",
        "otherField" to "otherValue",
      )
    val recordToExclude1 = getRecord(recordToExcludeData1)

    mapper.map(rowFilteringMapperConfig, recordToExclude1)

    Assertions.assertFalse(recordToExclude1.shouldInclude(), "Record should be discarded")
    assertNoChangesMadeToRecord(recordToExclude1, recordToExcludeData1)

    val recordToExcludeData2 =
      mapOf(
        "status" to "inactive",
        "region" to "eu",
        "otherField" to "otherValue2",
      )
    val recordToExclude2 = getRecord(recordToExcludeData2)

    mapper.map(rowFilteringMapperConfig, recordToExclude2)

    Assertions.assertFalse(recordToExclude2.shouldInclude(), "Record should be discarded")
    assertNoChangesMadeToRecord(recordToExclude2, recordToExcludeData2)

    val recordToIncludeData =
      mapOf(
        "status" to "inactive",
        "region" to "asia",
        "otherField" to "otherValue3",
      )
    val recordToInclude = getRecord(recordToIncludeData)

    mapper.map(rowFilteringMapperConfig, recordToInclude)

    Assertions.assertTrue(recordToInclude.shouldInclude(), "Record should not be discarded")
    assertNoChangesMadeToRecord(recordToInclude, recordToIncludeData)
  }

  @Nested
  internal inner class RowFilteringMapperSpecTest {
    @Test
    fun `should return expected spec`() {
      val spec = mapper.spec().jsonSchema()
      Assertions.assertEquals(mapper.name, spec["properties"]["name"]["const"].asText())
      Assertions.assertTrue("documentationUrl" in spec["properties"])
      Assertions.assertTrue(CONDITIONS in spec["definitions"]["RowFilteringConfig"]["properties"])
    }

    // If making changes to the mapper spec, ensure this test passes without modifying the examples to guarantee backward compatibility.
    @Test
    fun `should successfully validate config examples against spec`() {
      val resource =
        javaClass.classLoader.getResource("RowFilteringMapperConfigExamples.json")
          ?: throw IllegalArgumentException("File not found: RowFilteringMapperConfigExamples.json")

      val configExamples =
        Jsons.deserialize(
          File(resource.toURI()),
          object : TypeReference<List<ConfiguredMapper>>() {},
        )
      configExamples.forEachIndexed { index, configExample ->
        try {
          mapper.spec().deserialize(configExample)
        } catch (e: Exception) {
          throw AssertionError("Example at index $index is not valid: ${e.message}", e)
        }
      }
    }

    @Test
    fun `should throw exception when config is missing expected attribute`() {
      val configuredMapper =
        Jsons.deserialize(
          """
            {
                "name": "row-filtering",
                "config": {
                    "conditions": {
                        "fieldName": "status",
                        "comparisonValue": "active"
                    }
                }
            }
            """,
          ConfiguredMapper::class.java,
        )
      val exception =
        Assertions.assertThrows(IllegalArgumentException::class.java) {
          mapper.spec().deserialize(configuredMapper)
        }

      Assertions.assertEquals(
        "Mapper Config not valid: \$.config.conditions: must be valid to one and only one schema, " +
          "but 0 are valid,\$.config.conditions: required property 'type' not found,\$.config.conditions: required property 'conditions' " +
          "not found,\$.config.conditions: required property 'type' not found",
        exception.message,
      )
    }
  }
}
