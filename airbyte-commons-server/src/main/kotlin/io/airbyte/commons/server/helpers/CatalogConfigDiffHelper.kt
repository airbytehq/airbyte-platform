/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteCatalogDiff
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.CatalogConfigDiff
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.StreamCursorFieldDiff
import io.airbyte.api.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.StreamFieldStatusChanged
import io.airbyte.api.model.generated.StreamPrimaryKeyDiff
import io.airbyte.api.model.generated.StreamSyncModeDiff
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.commons.server.handlers.logger
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException

private val log = KotlinLogging.logger {}

@Singleton
class CatalogConfigDiffHelper {
  companion object {
    /**
     * Parses a connectorSchema to retrieve top level fields only, ignoring the nested fields.
     *
     * @param connectorSchema source or destination schema
     * @return A list of top level fields, ignoring the nested fields.
     */
    fun getStreamTopLevelFields(connectorSchema: JsonNode): List<String> {
      val jsonMapper = ObjectMapper(JsonFactory())
      val streamFields: MutableList<String> = mutableListOf()

      val spec: JsonNode =
        try {
          jsonMapper.readTree(connectorSchema.traverse())
        } catch (e: IOException) {
          log.error { "Error getting stream fields from schema: ${e.message}" }
          throw UnexpectedProblem()
        }

      // Ensure we only extract top-level fields under "properties"
      if (spec.has("properties")) {
        val propertyFields = spec.get("properties").fields()
        while (propertyFields.hasNext()) {
          val (propertyName, _) = propertyFields.next()
          streamFields.add(propertyName) // Add only top-level fields
        }
      }

      return streamFields.toList()
    }
  }

  /**
   * Get differences between old and new catalog configurations
   */
  fun getCatalogConfigDiff(
    sourceCatalog: AirbyteCatalog,
    oldConfig: AirbyteCatalog,
    newConfig: AirbyteCatalog,
  ): CatalogConfigDiff {
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

    // Convert catalogs to maps with StreamDescriptor as key
    val oldStreamMap = streamDescriptorToMap(oldConfig)
    val newStreamMap = streamDescriptorToMap(newConfig)
    val sourceStreamMap = streamDescriptorToMap(sourceCatalog)
    val oldStreamKeys = oldStreamMap.keys
    val newStreamKeys = newStreamMap.keys

    // Find new streams enabled
    newStreamKeys.subtract(oldStreamKeys).forEach { streamKey ->
      val newStream = newStreamMap[streamKey] ?: return@forEach
      if (newStream.config.selected == true) {
        catalogConfigDiff.streamsEnabled.add(
          StreamFieldStatusChanged().apply {
            this.streamName = newStream.stream.name
            this.streamNamespace = newStream.stream.namespace
            this.status = StreamFieldStatusChanged.StatusEnum.ENABLED
            // Note: if all fields are selected, fieldSelectionEnabled will be false and selectedFields will be empty
            this.fields =
              newStream.config.selectedFields
                ?.takeIf { it.isNotEmpty() }
                ?.mapNotNull { it.fieldPath.firstOrNull()?.toString() }
          },
        )
      }
    }

    // Compare configurations for streams that exist in both catalogs
    oldStreamKeys.intersect(newStreamKeys).forEach { streamKey ->
      val oldStream = oldStreamMap[streamKey] ?: return@forEach
      val newStream = newStreamMap[streamKey] ?: return@forEach
      sourceStreamMap[streamKey]?.let {
        compareStreamConfigs(
          it.stream,
          oldStream.config,
          newStream.config,
          catalogConfigDiff,
        )
      }
    }
    return catalogConfigDiff
  }

  private fun streamDescriptorToMap(catalog: AirbyteCatalog): Map<StreamDescriptor, AirbyteStreamAndConfiguration> =
    catalog.streams.associateBy {
      StreamDescriptor().apply {
        name = it.stream.name
        namespace = it.stream.namespace
      }
    }

  private fun AirbyteStream.toStreamDescriptor(): StreamDescriptor =
    StreamDescriptor().apply {
      name = this@toStreamDescriptor.name
      namespace = this@toStreamDescriptor.namespace
    }

  fun compareStreamConfigs(
    stream: AirbyteStream,
    oldConfig: AirbyteStreamConfiguration,
    newConfig: AirbyteStreamConfiguration,
    catalogConfigDiff: CatalogConfigDiff,
  ) {
    val streamDescriptor = stream.toStreamDescriptor()
    // Compare stream enablement status
    val streamWasEnabled = oldConfig.selected
    val streamIsEnabled = newConfig.selected
    // Get all fields from the stream schema
    val allFields = getStreamTopLevelFields(stream.jsonSchema)

    // Stream was enabled and is now disabled
    if (streamWasEnabled == true && streamIsEnabled == false) {
      val streamFieldStatusChanged =
        StreamFieldStatusChanged().apply {
          this.streamName = streamDescriptor.name
          this.streamNamespace = streamDescriptor.namespace
          this.status = StreamFieldStatusChanged.StatusEnum.DISABLED
        }
      catalogConfigDiff.streamsDisabled.add(streamFieldStatusChanged)
    }
    // Stream is still enabled, further compare field selection changes
    if (streamWasEnabled == true && streamIsEnabled == true) {
      compareSelectedFields(
        prevSelectedFields = oldConfig.selectedFields?.mapNotNull { it.fieldPath.firstOrNull()?.toString() },
        newSelectedFields = newConfig.selectedFields?.mapNotNull { it.fieldPath.firstOrNull()?.toString() },
        allFields = allFields.toSet(),
        streamDescriptor = streamDescriptor,
        catalogConfigDiff = catalogConfigDiff,
      )
    }

    // Compare primary keys
    // Primary keys are array of multiple field paths, e.g. [["f1", "f2", ...], ["c1", "c2", ...], ...]
    if (!(oldConfig.primaryKey.isNullOrEmpty() && newConfig.primaryKey.isNullOrEmpty())) {
      if (!arePrimaryKeysEqual(oldConfig.primaryKey, newConfig.primaryKey)) {
        catalogConfigDiff.primaryKeysChanged.add(
          StreamPrimaryKeyDiff().apply {
            this.streamName = streamDescriptor.name
            this.streamNamespace = streamDescriptor.namespace
            this.current = newConfig.primaryKey
            this.prev = oldConfig.primaryKey
          },
        )
      }
    }

    // Compare cursor fields
    // Cursor fields are array of one single field path, e.g. ["f1", "f2", "f3", ...]
    if (!(oldConfig.cursorField.isNullOrEmpty() && newConfig.cursorField.isNullOrEmpty()) &&
      !oldConfig.cursorField.equals(newConfig.cursorField)
    ) {
      catalogConfigDiff.cursorFieldsChanged.add(
        StreamCursorFieldDiff().apply {
          this.streamName = streamDescriptor.name
          this.streamNamespace = streamDescriptor.namespace
          this.current = newConfig.cursorField
          this.prev = oldConfig.cursorField
        },
      )
    }

    // Compare sync modes
    // syncMode and destinationSyncMode are required fields
    if (oldConfig.syncMode != newConfig.syncMode || oldConfig.destinationSyncMode != newConfig.destinationSyncMode) {
      catalogConfigDiff.syncModesChanged.add(
        StreamSyncModeDiff().apply {
          this.streamName = streamDescriptor.name
          this.streamNamespace = streamDescriptor.namespace
          this.currentDestinationSyncMode = newConfig.destinationSyncMode
          this.currentSourceSyncMode = newConfig.syncMode
          this.prevDestinationSyncMode = oldConfig.destinationSyncMode
          this.prevSourceSyncMode = oldConfig.syncMode
        },
      )
    }
  }

  private fun arePrimaryKeysEqual(
    prevKeys: List<List<String>>,
    newKeys: List<List<String>>,
  ): Boolean {
    val prevKeysSets = prevKeys.map { it.toSet() }.toSet()
    val newKeysSets = newKeys.map { it.toSet() }.toSet()
    return prevKeysSets == newKeysSets
  }

  private fun compareSelectedFields(
    prevSelectedFields: List<String>?,
    newSelectedFields: List<String>?,
    allFields: Set<String>,
    streamDescriptor: StreamDescriptor,
    catalogConfigDiff: CatalogConfigDiff,
  ) {
    val prevSelectedFieldsSet = prevSelectedFields?.toSet() ?: emptySet()
    val newSelectedFieldsSet = newSelectedFields?.toSet() ?: emptySet()

    logger.debug { "Checking selected fields. All fields: $allFields. Prev selected: $prevSelectedFields. Current selected: $newSelectedFields" }

    // Get all fields from the stream schema
    // Note: If a stream has been selected/enabled, but selectedFields is empty, it actually means all fields are selected.

    // case 1: deselect a few fields from ALL fields
    if (prevSelectedFieldsSet.isEmpty() && newSelectedFieldsSet.isNotEmpty()) {
      catalogConfigDiff.fieldsDisabled.add(
        StreamFieldStatusChanged().apply {
          this.streamName = streamDescriptor.name
          this.streamNamespace = streamDescriptor.namespace
          this.status = StreamFieldStatusChanged.StatusEnum.DISABLED
          this.fields = (allFields - newSelectedFieldsSet).toList()
        },
      )
    }
    // case 2: select more fields to enable ALL
    if (prevSelectedFieldsSet.isNotEmpty() && newSelectedFieldsSet.isEmpty()) {
      catalogConfigDiff.fieldsEnabled.add(
        StreamFieldStatusChanged().apply {
          this.streamName = streamDescriptor.name
          this.streamNamespace = streamDescriptor.namespace
          this.status = StreamFieldStatusChanged.StatusEnum.ENABLED
          this.fields = (allFields - prevSelectedFieldsSet).toList()
        },
      )
    }
    if (prevSelectedFieldsSet.isNotEmpty() && newSelectedFieldsSet.isNotEmpty()) {
      // case 3: deselect a few fields from partial selected fields
      val disabledFields = prevSelectedFieldsSet - newSelectedFieldsSet
      if (disabledFields.isNotEmpty()) {
        catalogConfigDiff.fieldsDisabled.add(
          StreamFieldStatusChanged().apply {
            this.streamName = streamDescriptor.name
            this.streamNamespace = streamDescriptor.namespace
            this.status = StreamFieldStatusChanged.StatusEnum.DISABLED
            this.fields = disabledFields.toList()
          },
        )
      }
      // case 4: select more fields but not all
      val enabledFields = newSelectedFieldsSet - prevSelectedFieldsSet
      if (enabledFields.isNotEmpty()) {
        catalogConfigDiff.fieldsEnabled.add(
          StreamFieldStatusChanged().apply {
            this.streamName = streamDescriptor.name
            this.streamNamespace = streamDescriptor.namespace
            this.status = StreamFieldStatusChanged.StatusEnum.ENABLED
            this.fields = enabledFields.toList()
          },
        )
      }
    }
  }

  fun getAirbyteCatalogDiff(
    catalogDiff: CatalogDiff?,
    catalogConfigDiff: CatalogConfigDiff?,
  ): AirbyteCatalogDiff? {
    if (catalogDiff?.transforms.isNullOrEmpty() && (catalogConfigDiff == null || isCatalogConfigDiffEmpty(catalogConfigDiff))) {
      return null
    }
    return AirbyteCatalogDiff().apply {
      this.catalogDiff = catalogDiff
      this.catalogConfigDiff = catalogConfigDiff
    }
  }

  private fun isCatalogConfigDiffEmpty(catalogConfigDiff: CatalogConfigDiff): Boolean =
    catalogConfigDiff.streamsEnabled.isNullOrEmpty() &&
      catalogConfigDiff.streamsDisabled.isNullOrEmpty() &&
      catalogConfigDiff.fieldsEnabled.isNullOrEmpty() &&
      catalogConfigDiff.fieldsDisabled.isNullOrEmpty() &&
      catalogConfigDiff.primaryKeysChanged.isNullOrEmpty() &&
      catalogConfigDiff.cursorFieldsChanged.isNullOrEmpty() &&
      catalogConfigDiff.syncModesChanged.isNullOrEmpty()
}
