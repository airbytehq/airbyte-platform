/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.client.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Field
import io.airbyte.config.helpers.FieldGenerator
import jakarta.inject.Singleton
import jakarta.validation.Valid

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
        StreamDescriptor(it.stream.name, it.stream.namespace) to it
      }
    val streamDescriptorToOriginalDiscoveredStream =
      originalDiscovered.streams.associate {
        StreamDescriptor(it.stream.name, it.stream.namespace) to it
      }

    val streams = mutableListOf<AirbyteStreamAndConfiguration>()

    for (discoveredStream in discovered.streams) {
      val stream = discoveredStream.stream
      val originalConfiguredStream =
        streamDescriptorToOriginalStream[StreamDescriptor(stream.name, stream.namespace)]
      val originalDiscoveredStream =
        streamDescriptorToOriginalDiscoveredStream[StreamDescriptor(stream.name, stream.namespace)]
      val outputStreamConfig: AirbyteStreamConfiguration

      if (originalConfiguredStream != null) {
        val originalStreamConfig = originalConfiguredStream.config
        val discoveredStreamConfig = discoveredStream.config
        outputStreamConfig = AirbyteStreamConfiguration()

        if (stream.supportedSyncModes.contains(originalStreamConfig.syncMode)) {
          outputStreamConfig.syncMode = originalStreamConfig.syncMode
        } else {
          outputStreamConfig.syncMode = discoveredStreamConfig.syncMode
        }

        if (!originalStreamConfig.cursorField.isEmpty()) {
          outputStreamConfig.cursorField = originalStreamConfig.cursorField
        } else {
          outputStreamConfig.cursorField = discoveredStreamConfig.cursorField
        }

        outputStreamConfig.destinationSyncMode = originalStreamConfig.destinationSyncMode

        val hasSourceDefinedPK = stream.sourceDefinedPrimaryKey != null && !stream.sourceDefinedPrimaryKey.isEmpty()
        if (hasSourceDefinedPK) {
          outputStreamConfig.primaryKey = stream.sourceDefinedPrimaryKey
        } else if (!originalStreamConfig.primaryKey.isEmpty()) {
          outputStreamConfig.primaryKey = originalStreamConfig.primaryKey
        } else {
          outputStreamConfig.primaryKey = discoveredStreamConfig.primaryKey
        }

        outputStreamConfig.aliasName = originalStreamConfig.aliasName
        outputStreamConfig.selected = originalConfiguredStream.config.selected
        outputStreamConfig.suggested = originalConfiguredStream.config.suggested
        outputStreamConfig.includeFiles = originalConfiguredStream.config.includeFiles
        outputStreamConfig.fieldSelectionEnabled = originalStreamConfig.fieldSelectionEnabled
        outputStreamConfig.mappers = originalStreamConfig.mappers
        outputStreamConfig.destinationObjectName = originalStreamConfig.destinationObjectName

        // Add hashed field configs that are still present in the schema
        if (originalStreamConfig.hashedFields != null && !originalStreamConfig.hashedFields.isEmpty()) {
          val discoveredFields =
            fieldGenerator
              .getFieldsFromSchema(stream.jsonSchema)
              .map { it.name }
              .toSet()
          for (hashedField in originalStreamConfig.hashedFields) {
            val fieldName = hashedField.fieldPath?.firstOrNull()
            if (fieldName != null && discoveredFields.contains(fieldName)) {
              outputStreamConfig.addHashedFieldsItem(hashedField)
            }
          }
        }

        if (outputStreamConfig.fieldSelectionEnabled) {
          // If field selection is enabled, populate the selected fields.
          val originallyDiscovered = mutableSetOf<String>()
          val refreshDiscovered = mutableSetOf<String>()
          // NOTE: by only taking the first element of the path, we're restricting to top-level fields.
          val originallySelected =
            originalConfiguredStream.config.selectedFields
              .map { it.fieldPath[0] }
              .toSet()
          originalDiscoveredStream?.let {
            it.stream.jsonSchema
              .findPath("properties")
              .fieldNames()
              .forEachRemaining { e: String -> originallyDiscovered.add(e) }
          }
          stream.jsonSchema
            .findPath("properties")
            .fieldNames()
            .forEachRemaining { e: String -> refreshDiscovered.add(e) }
          // We include a selected field if it:
          // (is in the newly discovered schema) AND (it was either originally selected OR not in the
          // originally discovered schema at all)
          // NOTE: this implies that the default behaviour for newly-discovered columns is to add them.
          for (discoveredField in refreshDiscovered) {
            if (originallySelected.contains(discoveredField) || !originallyDiscovered.contains(discoveredField)) {
              outputStreamConfig.addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem(discoveredField))
            }
          }
        } else {
          outputStreamConfig.selectedFields = listOf<@Valid SelectedFieldInfo?>()
        }
      } else {
        outputStreamConfig = discoveredStream.config
        outputStreamConfig.selected = false
      }
      val outputStream =
        AirbyteStreamAndConfiguration()
          .stream(Jsons.clone(stream))
          .config(outputStreamConfig)
      streams.add(outputStream)
    }
    return AirbyteCatalog().streams(streams)
  }
}
