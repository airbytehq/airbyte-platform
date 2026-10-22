/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.client.model.generated.StreamDescriptor
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteStream
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.SelectedFieldInfo
import io.airbyte.commons.json.Jsons
import io.airbyte.config.helpers.FieldGenerator
import jakarta.inject.Singleton

/**
 * Helper class for merging catalog configurations.
 * Provides utilities for combining newly discovered catalogs with existing configured catalogs,
 * preserving user selections and configurations.
 */
@Singleton
class CatalogMergeHelper(
  private val fieldGenerator: FieldGenerator,
) {
  /**
   * Merges a newly discovered catalog with an existing configured catalog.
   * Preserves user configuration (selected streams, sync modes, field selection, etc.) from the configured catalog
   * and applies it to the newly discovered catalog.
   *
   * This is the canonical implementation of catalog merging logic, used by both:
   * - WebBackendConnectionsHandler.updateSchemaWithRefreshedDiscoveredCatalog
   * - CatalogDiffService.mergeCatalogWithConfiguration
   *
   * @param originalConfigured fully configured, original catalog with user selections
   * @param originalDiscovered the original discovered catalog used to make the original configured catalog
   * @param discovered newly discovered catalog, no configurations set
   * @return merged catalog with most up-to-date schema and preserved configurations from old catalog
   */
  fun mergeCatalogWithConfiguration(
    originalConfigured: AirbyteCatalog,
    originalDiscovered: AirbyteCatalog,
    discovered: AirbyteCatalog,
  ): AirbyteCatalog {
    val streamDescriptorToOriginalStream =
      originalConfigured.streams.associate {
        StreamDescriptor(it.stream!!.name, it.stream!!.namespace) to it
      }
    val streamDescriptorToOriginalDiscoveredStream =
      originalDiscovered.streams.associate {
        StreamDescriptor(it.stream!!.name, it.stream!!.namespace) to it
      }

    val streams = mutableListOf<AirbyteStreamAndConfiguration>()

    for (discoveredStream in discovered.streams) {
      val stream = discoveredStream.stream!!
      val discoveredStreamConfig = discoveredStream.config!!
      val originalConfiguredStream =
        streamDescriptorToOriginalStream[StreamDescriptor(stream.name, stream.namespace)]
      val originalDiscoveredStream =
        streamDescriptorToOriginalDiscoveredStream[StreamDescriptor(stream.name, stream.namespace)]

      val outputStreamConfig =
        if (originalConfiguredStream != null) {
          mergeStreamConfig(stream, discoveredStreamConfig, originalConfiguredStream.config!!, originalDiscoveredStream)
        } else {
          discoveredStreamConfig.copy(selected = false)
        }
      val outputStream =
        AirbyteStreamAndConfiguration(stream = Jsons.clone(stream), config = outputStreamConfig)
      streams.add(outputStream)
    }
    return AirbyteCatalog(streams = streams)
  }

  private fun mergeStreamConfig(
    stream: AirbyteStream,
    discoveredStreamConfig: AirbyteStreamConfiguration,
    originalStreamConfig: AirbyteStreamConfiguration,
    originalDiscoveredStream: AirbyteStreamAndConfiguration?,
  ): AirbyteStreamConfiguration {
    val syncMode =
      if (stream.supportedSyncModes.orEmpty().contains(originalStreamConfig.syncMode)) {
        originalStreamConfig.syncMode
      } else {
        discoveredStreamConfig.syncMode
      }

    val cursorField =
      if (!originalStreamConfig.cursorField.isNullOrEmpty()) {
        originalStreamConfig.cursorField
      } else {
        discoveredStreamConfig.cursorField
      }

    val primaryKey =
      if (!stream.sourceDefinedPrimaryKey.isNullOrEmpty()) {
        stream.sourceDefinedPrimaryKey
      } else if (!originalStreamConfig.primaryKey.isNullOrEmpty()) {
        originalStreamConfig.primaryKey
      } else {
        discoveredStreamConfig.primaryKey
      }

    // Keep the hashed field configs that are still present in the schema
    val originalHashedFields = originalStreamConfig.hashedFields.orEmpty()
    val hashedFields =
      if (originalHashedFields.isEmpty()) {
        emptyList()
      } else {
        val discoveredFields =
          fieldGenerator
            .getFieldsFromSchema(stream.jsonSchema!!)
            .map { it.name }
            .toSet()
        originalHashedFields.filter { hashedField ->
          val fieldName = hashedField.fieldPath?.firstOrNull()
          fieldName != null && discoveredFields.contains(fieldName)
        }
      }

    val selectedFields =
      if (originalStreamConfig.fieldSelectionEnabled == true) {
        // If field selection is enabled, populate the selected fields.
        // NOTE: by only taking the first element of the path, we're restricting to top-level fields.
        val originallySelected =
          originalStreamConfig.selectedFields
            .orEmpty()
            .map { it.fieldPath!![0] }
            .toSet()
        val originallyDiscovered = originalDiscoveredStream?.let { topLevelFieldNames(it.stream!!) }.orEmpty()
        val refreshDiscovered = topLevelFieldNames(stream)
        // We include a selected field if it:
        // (is in the newly discovered schema) AND (it was either originally selected OR not in the
        // originally discovered schema at all)
        // NOTE: this implies that the default behaviour for newly-discovered columns is to add them.
        refreshDiscovered
          .filter { originallySelected.contains(it) || !originallyDiscovered.contains(it) }
          .map { SelectedFieldInfo(fieldPath = listOf(it)) }
      } else {
        emptyList()
      }

    return AirbyteStreamConfiguration(
      syncMode = syncMode,
      cursorField = cursorField,
      destinationSyncMode = originalStreamConfig.destinationSyncMode,
      primaryKey = primaryKey,
      aliasName = originalStreamConfig.aliasName,
      selected = originalStreamConfig.selected,
      suggested = originalStreamConfig.suggested,
      includeFiles = originalStreamConfig.includeFiles,
      fieldSelectionEnabled = originalStreamConfig.fieldSelectionEnabled,
      mappers = originalStreamConfig.mappers,
      destinationObjectName = originalStreamConfig.destinationObjectName,
      hashedFields = hashedFields,
      selectedFields = selectedFields,
    )
  }

  private fun topLevelFieldNames(stream: AirbyteStream): Set<String> =
    stream.jsonSchema!!
      .findPath("properties")
      .fieldNames()
      .asSequence()
      .toCollection(LinkedHashSet())
}
