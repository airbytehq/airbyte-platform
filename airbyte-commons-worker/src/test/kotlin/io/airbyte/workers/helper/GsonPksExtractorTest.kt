/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.SyncMode
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.List

internal class GsonPksExtractorTest {
  private val gsonPksExtractor = GsonPksExtractor()

  @Test
  fun testNonExistingPk() {
    val record =
      """
      {
          "record": {
              "stream": "test",
              "data": {
                  "field": "value"
              }
          }
      }
      
      """.trimIndent()

    val configureAirbyteCatalog = getCatalogWithPk(STREAM_NAME, java.util.List.of(java.util.List.of(FIELD_1_NAME)) as List<List<String>>?)

    Assertions.assertEquals("field1=[MISSING]", gsonPksExtractor.extractPks(configureAirbyteCatalog, record))
  }

  @Test
  fun testExistingPkLevel1() {
    val record =
      """
      {
          "record": {
              "stream": "test",
              "data": {
                  "field1": "value"
              }
          }
      }
      
      """.trimIndent()

    val configureAirbyteCatalog = getCatalogWithPk(STREAM_NAME, java.util.List.of(java.util.List.of(FIELD_1_NAME)) as List<List<String>>?)

    Assertions.assertEquals("field1=value", gsonPksExtractor.extractPks(configureAirbyteCatalog, record))
  }

  @Test
  fun testExistingPkLevel2() {
    val record =
      """
      {
          "record": {
              "stream": "test",
              "data": {
                  "level1": {
                      "field1": "value"
                  }
              }
          }
      }
      
      """.trimIndent()

    val configureAirbyteCatalog =
      getCatalogWithPk(STREAM_NAME, java.util.List.of(java.util.List.of("level1", FIELD_1_NAME)) as List<List<String>>?)

    Assertions.assertEquals("level1.field1=value", gsonPksExtractor.extractPks(configureAirbyteCatalog, record))
  }

  @Test
  fun testExistingPkMultipleKey() {
    val record =
      """
      {
          "record": {
              "stream": "test",
              "data": {
                  "field1": "value",
                  "field2": "value2"
              }
          }
      }
      
      """.trimIndent()

    val configureAirbyteCatalog =
      getCatalogWithPk(STREAM_NAME, java.util.List.of(java.util.List.of(FIELD_1_NAME), java.util.List.of("field2")) as List<List<String>>?)

    Assertions.assertEquals("field1=value,field2=value2", gsonPksExtractor.extractPks(configureAirbyteCatalog, record))
  }

  @Test
  fun testExistingVeryLongRecord() {
    val longStringBuilder = StringBuilder(5000000)
    for (i in 0..50000000 - 1) {
      longStringBuilder.append("a")
    }
    val record =
      String.format(
        """
        {
            "record": {
                "stream": "test",
                "data": {
                    "field1": "value",
                    "veryLongField": "%s"
                }
            }
        }
        
        """.trimIndent(),
        longStringBuilder,
      )

    val configureAirbyteCatalog = getCatalogWithPk(STREAM_NAME, java.util.List.of(java.util.List.of(FIELD_1_NAME)) as List<List<String>>?)

    Assertions.assertEquals("field1=value", gsonPksExtractor.extractPks(configureAirbyteCatalog, record))
  }

  private fun getCatalogWithPk(
    streamName: String,
    pksList: Any?,
  ): ConfiguredAirbyteCatalog =
    ConfiguredAirbyteCatalog()
      .withStreams(
        List.of<ConfiguredAirbyteStream>(
          ConfiguredAirbyteStream(
            AirbyteStream(streamName, emptyObject(), List.of<SyncMode?>(SyncMode.INCREMENTAL)),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND_DEDUP,
          ).withPrimaryKey(pksList as kotlin.collections.List<kotlin.collections.List<String>>?),
        ),
      )

  companion object {
    private const val STREAM_NAME = "test"
    private const val FIELD_1_NAME = "field1"
  }
}
