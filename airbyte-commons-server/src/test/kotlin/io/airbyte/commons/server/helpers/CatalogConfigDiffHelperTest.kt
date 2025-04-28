/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.CatalogConfigDiff
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.StreamCursorFieldDiff
import io.airbyte.api.model.generated.StreamFieldStatusChanged
import io.airbyte.api.model.generated.StreamPrimaryKeyDiff
import io.airbyte.api.model.generated.StreamSyncModeDiff
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class CatalogConfigDiffHelperTest {
  private var catalogConfigDiffHelper: CatalogConfigDiffHelper = CatalogConfigDiffHelper()

  @Test
  fun testEmptyAirbyteCatalogDiff() {
    val catalogDiff = CatalogDiff().apply { transforms = listOf() }
    val catalogConfigDiff =
      CatalogConfigDiff().apply {
        this.streamsEnabled = mutableListOf<StreamFieldStatusChanged>()
        this.streamsDisabled = mutableListOf<StreamFieldStatusChanged>()
        this.fieldsEnabled = mutableListOf<StreamFieldStatusChanged>()
        this.fieldsDisabled = mutableListOf<StreamFieldStatusChanged>()
        this.primaryKeysChanged = mutableListOf<StreamPrimaryKeyDiff>()
        this.cursorFieldsChanged = mutableListOf<StreamCursorFieldDiff>()
        this.syncModesChanged = mutableListOf<StreamSyncModeDiff>()
      }
    assertEquals(null, catalogConfigDiffHelper.getAirbyteCatalogDiff(catalogDiff, catalogConfigDiff))
  }

  @Nested
  inner class TestStreamLevelCatalogDiff {
    private val jsonString = """
        {
          "type": "object",
          "properties": {
            "field1": {
              "type": "boolean"
            },
            "field2": {
              "type": "string"
            }
          }
        }
    """
    private val streamSchema = ObjectMapper().readTree(jsonString)
    private val stream1 =
      AirbyteStream().apply {
        name = "stream1"
        jsonSchema = streamSchema
        supportedSyncModes = listOf()
      }
    private val stream2 =
      AirbyteStream().apply {
        name = "stream2"
        jsonSchema = streamSchema
        supportedSyncModes = listOf()
      }

    private val sourceCatalog =
      AirbyteCatalog().apply {
        streams =
          listOf(
            AirbyteStreamAndConfiguration().apply { stream = stream1 },
            AirbyteStreamAndConfiguration().apply { stream = stream2 },
          )
      }

    private val stream1ConfigOld =
      AirbyteStreamAndConfiguration().apply {
        stream = stream1
        config =
          AirbyteStreamConfiguration().apply {
            selected = true
            selectedFields = listOf()
            suggested = false
          }
      }

    private val oldConfig =
      AirbyteCatalog().apply {
        streams = listOf(stream1ConfigOld)
      }

    @Test
    fun testEnabledStreams() {
      val stream1ConfigNew =
        AirbyteStreamAndConfiguration().apply {
          stream = stream1
          config =
            AirbyteStreamConfiguration().apply {
              fieldSelectionEnabled = false
              selected = true // stream enabled
              selectedFields = listOf() // all fields are enabled
              suggested = false
            }
        }

      val stream2ConfigNew =
        AirbyteStreamAndConfiguration().apply {
          stream = stream2
          config =
            AirbyteStreamConfiguration().apply {
              fieldSelectionEnabled = false
              selected = true // stream2 is enabled
              selectedFields = listOf() // all fields are enabled
              suggested = false
            }
        }

      val newConfig =
        AirbyteCatalog().apply {
          streams = listOf(stream1ConfigNew, stream2ConfigNew)
        }

      val catalogConfigDiff = catalogConfigDiffHelper.getCatalogConfigDiff(sourceCatalog, oldConfig, newConfig)
      assertEquals(1, catalogConfigDiff.streamsEnabled.size)
    }

    @Test
    fun testDisabledStreams() {
      val stream1ConfigNew =
        AirbyteStreamAndConfiguration().apply {
          stream = stream1
          config =
            AirbyteStreamConfiguration().apply {
              selected = false // stream disabled
              selectedFields = listOf() // all fields are enabled
              suggested = false
            }
        }

      val newConfig =
        AirbyteCatalog().apply {
          streams = listOf(stream1ConfigNew)
        }

      val catalogConfigDiff = catalogConfigDiffHelper.getCatalogConfigDiff(sourceCatalog, oldConfig, newConfig)
      assertEquals(1, catalogConfigDiff.streamsDisabled.size)
    }
  }
}
