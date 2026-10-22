/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteCatalogDiff
import io.airbyte.api.server.generated.models.AirbyteStream
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.CatalogConfigDiff
import io.airbyte.api.server.generated.models.CatalogDiff
import io.airbyte.api.server.generated.models.StreamCursorFieldDiff
import io.airbyte.api.server.generated.models.StreamDescriptor
import io.airbyte.api.server.generated.models.StreamFieldStatusChanged
import io.airbyte.api.server.generated.models.StreamPrimaryKeyDiff
import io.airbyte.api.server.generated.models.StreamSyncModeDiff
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException

private val logger = KotlinLogging.logger {}

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
          logger.error { "Error getting stream fields from schema: ${e.message}" }
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
    val catalogConfigDiff = CatalogConfigDiffBuilder()

    // Convert catalogs to maps with StreamDescriptor as key
    val oldStreamMap = streamDescriptorToMap(oldConfig)
    val newStreamMap = streamDescriptorToMap(newConfig)
    val sourceStreamMap = streamDescriptorToMap(sourceCatalog)
    val oldStreamKeys = oldStreamMap.keys
    val newStreamKeys = newStreamMap.keys

    // Find new streams enabled
    newStreamKeys.subtract(oldStreamKeys).forEach { streamKey ->
      val newStream = newStreamMap[streamKey] ?: return@forEach
      if (newStream.config!!.selected == true) {
        catalogConfigDiff.streamsEnabled.add(
          StreamFieldStatusChanged(
            streamName = newStream.stream!!.name,
            streamNamespace = newStream.stream!!.namespace,
            status = StreamFieldStatusChanged.Status.ENABLED,
            // Note: if all fields are selected, fieldSelectionEnabled will be false and selectedFields will be empty
            fields =
              newStream.config!!
                .selectedFields
                ?.takeIf { it.isNotEmpty() }
                ?.mapNotNull { it.fieldPath?.firstOrNull()?.toString() },
          ),
        )
      }
    }

    // Compare configurations for streams that exist in both catalogs
    oldStreamKeys.intersect(newStreamKeys).forEach { streamKey ->
      val oldStream = oldStreamMap[streamKey] ?: return@forEach
      val newStream = newStreamMap[streamKey] ?: return@forEach
      sourceStreamMap[streamKey]?.let {
        compareStreamConfigs(
          it.stream!!,
          oldStream.config!!,
          newStream.config!!,
          catalogConfigDiff,
        )
      }
    }
    return catalogConfigDiff.build()
  }

  private fun streamDescriptorToMap(catalog: AirbyteCatalog): Map<StreamDescriptor, AirbyteStreamAndConfiguration> =
    catalog.streams.associateBy {
      StreamDescriptor(name = it.stream!!.name, namespace = it.stream!!.namespace)
    }

  private fun AirbyteStream.toStreamDescriptor(): StreamDescriptor =
    StreamDescriptor(name = this@toStreamDescriptor.name, namespace = this@toStreamDescriptor.namespace)

  private fun compareStreamConfigs(
    stream: AirbyteStream,
    oldConfig: AirbyteStreamConfiguration,
    newConfig: AirbyteStreamConfiguration,
    catalogConfigDiff: CatalogConfigDiffBuilder,
  ) {
    val streamDescriptor = stream.toStreamDescriptor()
    // Compare stream enablement status
    val streamWasEnabled = oldConfig.selected
    val streamIsEnabled = newConfig.selected
    // Get all fields from the stream schema
    val allFields = getStreamTopLevelFields(stream.jsonSchema!!)

    // Stream was enabled and is now disabled
    if (streamWasEnabled == true && streamIsEnabled == false) {
      val streamFieldStatusChanged =
        StreamFieldStatusChanged(
          streamName = streamDescriptor.name,
          streamNamespace = streamDescriptor.namespace,
          status = StreamFieldStatusChanged.Status.DISABLED,
        )
      catalogConfigDiff.streamsDisabled.add(streamFieldStatusChanged)
    }
    // Stream is still enabled, further compare field selection changes
    if (streamWasEnabled == true && streamIsEnabled == true) {
      compareSelectedFields(
        prevSelectedFields = oldConfig.selectedFields?.mapNotNull { it.fieldPath?.firstOrNull()?.toString() },
        newSelectedFields = newConfig.selectedFields?.mapNotNull { it.fieldPath?.firstOrNull()?.toString() },
        allFields = allFields.toSet(),
        streamDescriptor = streamDescriptor,
        catalogConfigDiff = catalogConfigDiff,
      )
    }

    // Compare primary keys
    // Primary keys are array of multiple field paths, e.g. [["f1", "f2", ...], ["c1", "c2", ...], ...]
    if (!(oldConfig.primaryKey.isNullOrEmpty() && newConfig.primaryKey.isNullOrEmpty())) {
      if (!arePrimaryKeysEqual(oldConfig.primaryKey.orEmpty(), newConfig.primaryKey.orEmpty())) {
        catalogConfigDiff.primaryKeysChanged.add(
          StreamPrimaryKeyDiff(
            streamName = streamDescriptor.name,
            streamNamespace = streamDescriptor.namespace,
            current = newConfig.primaryKey,
            prev = oldConfig.primaryKey,
          ),
        )
      }
    }

    // Compare cursor fields
    // Cursor fields are array of one single field path, e.g. ["f1", "f2", "f3", ...]
    if (!(oldConfig.cursorField.isNullOrEmpty() && newConfig.cursorField.isNullOrEmpty()) &&
      oldConfig.cursorField != newConfig.cursorField
    ) {
      catalogConfigDiff.cursorFieldsChanged.add(
        StreamCursorFieldDiff(
          streamName = streamDescriptor.name,
          streamNamespace = streamDescriptor.namespace,
          current = newConfig.cursorField,
          prev = oldConfig.cursorField,
        ),
      )
    }

    // Compare sync modes
    // syncMode and destinationSyncMode are required fields
    if (oldConfig.syncMode != newConfig.syncMode || oldConfig.destinationSyncMode != newConfig.destinationSyncMode) {
      catalogConfigDiff.syncModesChanged.add(
        StreamSyncModeDiff(
          streamName = streamDescriptor.name,
          streamNamespace = streamDescriptor.namespace,
          currentDestinationSyncMode = newConfig.destinationSyncMode,
          currentSourceSyncMode = newConfig.syncMode,
          prevDestinationSyncMode = oldConfig.destinationSyncMode,
          prevSourceSyncMode = oldConfig.syncMode,
        ),
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
    catalogConfigDiff: CatalogConfigDiffBuilder,
  ) {
    val prevSelectedFieldsSet = prevSelectedFields?.toSet() ?: emptySet()
    val newSelectedFieldsSet = newSelectedFields?.toSet() ?: emptySet()

    logger.debug { "Checking selected fields. All fields: $allFields. Prev selected: $prevSelectedFields. Current selected: $newSelectedFields" }

    // Get all fields from the stream schema
    // Note: If a stream has been selected/enabled, but selectedFields is empty, it actually means all fields are selected.

    // case 1: deselect a few fields from ALL fields
    if (prevSelectedFieldsSet.isEmpty() && newSelectedFieldsSet.isNotEmpty()) {
      catalogConfigDiff.fieldsDisabled.add(
        StreamFieldStatusChanged(
          streamName = streamDescriptor.name,
          streamNamespace = streamDescriptor.namespace,
          status = StreamFieldStatusChanged.Status.DISABLED,
          fields = (allFields - newSelectedFieldsSet).toList(),
        ),
      )
    }
    // case 2: select more fields to enable ALL
    if (prevSelectedFieldsSet.isNotEmpty() && newSelectedFieldsSet.isEmpty()) {
      catalogConfigDiff.fieldsEnabled.add(
        StreamFieldStatusChanged(
          streamName = streamDescriptor.name,
          streamNamespace = streamDescriptor.namespace,
          status = StreamFieldStatusChanged.Status.ENABLED,
          fields = (allFields - prevSelectedFieldsSet).toList(),
        ),
      )
    }
    if (prevSelectedFieldsSet.isNotEmpty() && newSelectedFieldsSet.isNotEmpty()) {
      // case 3: deselect a few fields from partial selected fields
      val disabledFields = prevSelectedFieldsSet - newSelectedFieldsSet
      if (disabledFields.isNotEmpty()) {
        catalogConfigDiff.fieldsDisabled.add(
          StreamFieldStatusChanged(
            streamName = streamDescriptor.name,
            streamNamespace = streamDescriptor.namespace,
            status = StreamFieldStatusChanged.Status.DISABLED,
            fields = disabledFields.toList(),
          ),
        )
      }
      // case 4: select more fields but not all
      val enabledFields = newSelectedFieldsSet - prevSelectedFieldsSet
      if (enabledFields.isNotEmpty()) {
        catalogConfigDiff.fieldsEnabled.add(
          StreamFieldStatusChanged(
            streamName = streamDescriptor.name,
            streamNamespace = streamDescriptor.namespace,
            status = StreamFieldStatusChanged.Status.ENABLED,
            fields = enabledFields.toList(),
          ),
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
    return AirbyteCatalogDiff(catalogDiff = catalogDiff, catalogConfigDiff = catalogConfigDiff)
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

/** Collects the changes as [CatalogConfigDiffHelper] walks the streams, then builds the immutable diff. */
private class CatalogConfigDiffBuilder {
  val streamsEnabled = mutableListOf<StreamFieldStatusChanged>()
  val streamsDisabled = mutableListOf<StreamFieldStatusChanged>()
  val fieldsEnabled = mutableListOf<StreamFieldStatusChanged>()
  val fieldsDisabled = mutableListOf<StreamFieldStatusChanged>()
  val primaryKeysChanged = mutableListOf<StreamPrimaryKeyDiff>()
  val cursorFieldsChanged = mutableListOf<StreamCursorFieldDiff>()
  val syncModesChanged = mutableListOf<StreamSyncModeDiff>()

  fun build(): CatalogConfigDiff =
    CatalogConfigDiff(
      streamsEnabled = streamsEnabled,
      streamsDisabled = streamsDisabled,
      fieldsEnabled = fieldsEnabled,
      fieldsDisabled = fieldsDisabled,
      primaryKeysChanged = primaryKeysChanged,
      cursorFieldsChanged = cursorFieldsChanged,
      syncModesChanged = syncModesChanged,
    )
}
