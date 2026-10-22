/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteStream
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.CatalogConfigDiff
import io.airbyte.api.server.generated.models.CatalogDiff
import io.airbyte.api.server.generated.models.DestinationSyncMode
import io.airbyte.api.server.generated.models.StreamCursorFieldDiff
import io.airbyte.api.server.generated.models.StreamFieldStatusChanged
import io.airbyte.api.server.generated.models.StreamPrimaryKeyDiff
import io.airbyte.api.server.generated.models.StreamSyncModeDiff
import io.airbyte.api.server.generated.models.SyncMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class CatalogConfigDiffHelperTest {
  private var catalogConfigDiffHelper: CatalogConfigDiffHelper = CatalogConfigDiffHelper()

  @Test
  fun testEmptyAirbyteCatalogDiff() {
    val catalogDiff = CatalogDiff(transforms = listOf())
    val catalogConfigDiff =
      CatalogConfigDiff(
        streamsEnabled = mutableListOf<StreamFieldStatusChanged>(),
        streamsDisabled = mutableListOf<StreamFieldStatusChanged>(),
        fieldsEnabled = mutableListOf<StreamFieldStatusChanged>(),
        fieldsDisabled = mutableListOf<StreamFieldStatusChanged>(),
        primaryKeysChanged = mutableListOf<StreamPrimaryKeyDiff>(),
        cursorFieldsChanged = mutableListOf<StreamCursorFieldDiff>(),
        syncModesChanged = mutableListOf<StreamSyncModeDiff>(),
      )
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
      AirbyteStream(name = "stream1", jsonSchema = streamSchema, supportedSyncModes = listOf())
    private val stream2 =
      AirbyteStream(name = "stream2", jsonSchema = streamSchema, supportedSyncModes = listOf())

    private val sourceCatalog =
      AirbyteCatalog(
        streams =
          listOf(
            AirbyteStreamAndConfiguration(stream = stream1),
            AirbyteStreamAndConfiguration(stream = stream2),
          ),
      )

    private val stream1ConfigOld =
      AirbyteStreamAndConfiguration(
        stream = stream1,
        config = streamConfig(selected = true),
      )

    private val oldConfig =
      AirbyteCatalog(streams = listOf(stream1ConfigOld))

    @Test
    fun testEnabledStreams() {
      val stream1ConfigNew =
        AirbyteStreamAndConfiguration(
          stream = stream1,
          // stream enabled, all fields are enabled
          config = streamConfig(selected = true, fieldSelectionEnabled = false),
        )

      val stream2ConfigNew =
        AirbyteStreamAndConfiguration(
          stream = stream2,
          // stream2 is enabled, all fields are enabled
          config = streamConfig(selected = true, fieldSelectionEnabled = false),
        )

      val newConfig =
        AirbyteCatalog(streams = listOf(stream1ConfigNew, stream2ConfigNew))

      val catalogConfigDiff = catalogConfigDiffHelper.getCatalogConfigDiff(sourceCatalog, oldConfig, newConfig)
      assertEquals(1, catalogConfigDiff.streamsEnabled!!.size)
    }

    @Test
    fun testDisabledStreams() {
      val stream1ConfigNew =
        AirbyteStreamAndConfiguration(
          stream = stream1,
          // stream disabled, all fields are enabled
          config = streamConfig(selected = false),
        )

      val newConfig =
        AirbyteCatalog(streams = listOf(stream1ConfigNew))

      val catalogConfigDiff = catalogConfigDiffHelper.getCatalogConfigDiff(sourceCatalog, oldConfig, newConfig)
      assertEquals(1, catalogConfigDiff.streamsDisabled!!.size)
    }

    private fun streamConfig(
      selected: Boolean,
      fieldSelectionEnabled: Boolean? = null,
    ): AirbyteStreamConfiguration =
      AirbyteStreamConfiguration(
        syncMode = SyncMode.FULL_REFRESH,
        destinationSyncMode = DestinationSyncMode.OVERWRITE,
        fieldSelectionEnabled = fieldSelectionEnabled,
        selected = selected,
        selectedFields = listOf(),
        suggested = false,
      )
  }
}
