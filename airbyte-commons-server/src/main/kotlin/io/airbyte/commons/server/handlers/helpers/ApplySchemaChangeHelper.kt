/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.FieldTransform
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.api.model.generated.StreamAttributeTransform
import io.airbyte.api.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.commons.json.Jsons
import jakarta.inject.Singleton
import jakarta.validation.Valid
import jakarta.ws.rs.NotSupportedException
import java.util.function.Consumer
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * Helper that allows to generate the catalogs to be auto propagated.
 */
@Singleton
class ApplySchemaChangeHelper(
  private val catalogConverter: CatalogConverter,
) {
  /**
   * Return value for `getUpdatedSchema` method. Returns the updated catalog and a description of the
   * changes
   */
  @JvmRecord
  data class UpdateSchemaResult(
    @JvmField val catalog: AirbyteCatalog,
    @JvmField val appliedDiff: CatalogDiff,
    @JvmField val changeDescription: List<String>,
  )

  /**
   * Generate a summary of the changes that will be applied to the destination if the schema is
   * updated.
   *
   * @param transform the transformation to be applied to the destination
   * @return a string describing the changes that will be applied to the destination.
   */
  @VisibleForTesting
  fun formatDiff(transform: StreamTransform): String {
    val namespace = transform.streamDescriptor.namespace
    val nsPrefix = if (namespace != null) String.format("%s.", namespace) else ""
    val streamName = transform.streamDescriptor.name
    when (transform.transformType) {
      StreamTransform.TransformTypeEnum.ADD_STREAM -> {
        return String.format("Added new stream '%s%s'", nsPrefix, streamName)
      }

      StreamTransform.TransformTypeEnum.REMOVE_STREAM -> {
        return String.format("Removed stream '%s%s'", nsPrefix, streamName)
      }

      StreamTransform.TransformTypeEnum.UPDATE_STREAM -> {
        val returnValue = StringBuilder(String.format("Modified stream '%s%s': ", nsPrefix, streamName))
        if (transform.updateStream == null) {
          return returnValue.toString()
        }
        val addedFields: MutableList<String> = ArrayList()
        val removedFields: MutableList<String> = ArrayList()
        val updatedFields: MutableList<String> = ArrayList()

        for (fieldTransform in transform.updateStream.fieldTransforms) {
          val fieldName = java.lang.String.join(".", fieldTransform.fieldName)
          when (fieldTransform.transformType) {
            FieldTransform.TransformTypeEnum.ADD_FIELD ->
              addedFields.add(
                String.format("'%s'", fieldName),
              )

            FieldTransform.TransformTypeEnum.REMOVE_FIELD -> removedFields.add(String.format("'%s'", fieldName))
            FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA -> updatedFields.add(String.format("'%s'", fieldName))
            else -> throw NotSupportedException("Not supported transformation.")
          }
        }
        val detailedUpdates: MutableList<String> = ArrayList()
        if (!addedFields.isEmpty()) {
          detailedUpdates.add(String.format("Added fields [%s]", java.lang.String.join(", ", addedFields)))
        }
        if (!removedFields.isEmpty()) {
          detailedUpdates.add(String.format("Removed fields [%s]", java.lang.String.join(", ", removedFields)))
        }
        if (!updatedFields.isEmpty()) {
          detailedUpdates.add(String.format("Altered fields [%s]", java.lang.String.join(", ", updatedFields)))
        }
        returnValue.append(java.lang.String.join(", ", detailedUpdates))
        return returnValue.toString()
      }

      else -> {
        return "Unknown Transformation"
      }
    }
  }

  /**
   * This method auto-propagates schema changes by replacing streams in the old catalog with those
   * from the new catalog based on the provided transformations.
   *
   * @param oldCatalog the currently saved catalog
   * @param newCatalog the new catalog, which contains all the streams, even the unselected ones
   * @param transformations list of transformations per stream
   * @param nonBreakingChangesPreference user preference for the auto propagation
   * @return an UpdateSchemaResult containing the updated catalog, applied diff, and change
   * descriptions
   */
  fun getUpdatedSchema(
    oldCatalog: AirbyteCatalog,
    newCatalog: AirbyteCatalog,
    transformations: List<StreamTransform>,
    nonBreakingChangesPreference: NonBreakingChangesPreference,
    supportedDestinationSyncModes: List<DestinationSyncMode?>,
  ): UpdateSchemaResult {
    val copiedOldCatalog = Jsons.clone(oldCatalog)
    val oldCatalogPerStream = extractStreamAndConfigPerStreamDescriptor(copiedOldCatalog).toMutableMap()
    val newCatalogPerStream: Map<StreamDescriptor, AirbyteStreamAndConfiguration?> = extractStreamAndConfigPerStreamDescriptor(newCatalog)

    val changes: MutableList<String> = ArrayList()
    val appliedDiff = CatalogDiff()

    transformations.forEach(
      Consumer<StreamTransform> { transformation: StreamTransform ->
        val streamDescriptor = transformation.streamDescriptor
        when (transformation.transformType) {
          StreamTransform.TransformTypeEnum.UPDATE_STREAM -> {
            if (oldCatalogPerStream.containsKey(streamDescriptor)) {
              oldCatalogPerStream[streamDescriptor]
                ?.stream(newCatalogPerStream[streamDescriptor]!!.stream)
              val streamConfig = oldCatalogPerStream[streamDescriptor]!!.config
              if (java.lang.Boolean.TRUE === streamConfig.fieldSelectionEnabled) {
                val selectedFields =
                  streamConfig.selectedFields
                    .stream()
                    .map { i: SelectedFieldInfo -> i.fieldPath[0] }
                    .collect(Collectors.toSet())
                val newlySelectedFields: MutableSet<String> = HashSet()
                for (fieldTransform in transformation.updateStream.fieldTransforms) {
                  if (fieldTransform.transformType == FieldTransform.TransformTypeEnum.ADD_FIELD &&
                    fieldTransform.fieldName.size == 1
                  ) {
                    val newTopLevelField = fieldTransform.fieldName[0]
                    if (!selectedFields.contains(newTopLevelField)) {
                      newlySelectedFields.add(fieldTransform.fieldName[0])
                    }
                  }
                }
                val allSelectedFields =
                  Stream
                    .concat<@Valid SelectedFieldInfo?>(
                      streamConfig.selectedFields.stream(),
                      newlySelectedFields.stream().map { field: String -> SelectedFieldInfo().fieldPath(java.util.List.of(field)) },
                    ).toList()
                streamConfig.selectedFields = allSelectedFields
              }
              changes.add(formatDiff(transformation))
              appliedDiff.addTransformsItem(transformation)
            }
          }

          StreamTransform.TransformTypeEnum.ADD_STREAM -> {
            if (nonBreakingChangesPreference == NonBreakingChangesPreference.PROPAGATE_FULLY) {
              val streamAndConfigurationToAdd = newCatalogPerStream[streamDescriptor]
              // Enable the stream if we're propagating it; otherwise, it'll get dropped when we update the
              // catalog.
              streamAndConfigurationToAdd!!.config.selected(true)
              catalogConverter.configureDefaultSyncModesForNewStream(
                streamAndConfigurationToAdd.stream,
                streamAndConfigurationToAdd.config,
              )
              catalogConverter.ensureCompatibleDestinationSyncMode(streamAndConfigurationToAdd, supportedDestinationSyncModes)
              // TODO(mfsiega-airbyte): handle the case where the chosen sync mode isn't actually one of the
              // supported sync modes.
              oldCatalogPerStream[streamDescriptor] = streamAndConfigurationToAdd
              changes.add(formatDiff(transformation))
              appliedDiff.addTransformsItem(transformation)
            }
          }

          StreamTransform.TransformTypeEnum.REMOVE_STREAM -> {
            if (nonBreakingChangesPreference == NonBreakingChangesPreference.PROPAGATE_FULLY) {
              oldCatalogPerStream.remove(streamDescriptor)
              changes.add(formatDiff(transformation))
              appliedDiff.addTransformsItem(transformation)
            }
          }

          else -> throw NotSupportedException("Not supported transformation.")
        }
      },
    )

    return UpdateSchemaResult(
      AirbyteCatalog().streams(java.util.List.copyOf<@Valid AirbyteStreamAndConfiguration?>(oldCatalogPerStream.values)),
      appliedDiff,
      changes,
    )
  }

  @VisibleForTesting
  fun extractStreamAndConfigPerStreamDescriptor(catalog: AirbyteCatalog): Map<StreamDescriptor, AirbyteStreamAndConfiguration> =
    catalog.streams.associateBy { streamAndConfig ->
      StreamDescriptor()
        .name(streamAndConfig.stream.name)
        .namespace(streamAndConfig.stream.namespace)
    }

  /**
   * Tests whether auto-propagation should be applied based on the connection/workspace configs and
   * the diff.
   *
   * @param diff the diff to be applied
   * @param connectionRead the connection info
   * @return whether the diff should be propagated
   */
  fun shouldAutoPropagate(
    diff: CatalogDiff,
    connectionRead: ConnectionRead,
  ): Boolean {
    if (!containsChanges(diff)) {
      // If there's no diff, we always propagate because it means there's a diff in a disabled stream or
      // some other metadata.
      // We want to acknowledge it and update to the latest source catalog ID without bothering the user.
      return true
    }
    val nonBreakingChange = !containsBreakingChange(diff)
    val autoPropagationIsEnabledForConnection =
      connectionRead.nonBreakingChangesPreference != null &&
        (
          connectionRead.nonBreakingChangesPreference == NonBreakingChangesPreference.PROPAGATE_COLUMNS ||
            connectionRead.nonBreakingChangesPreference == NonBreakingChangesPreference.PROPAGATE_FULLY
        )
    return nonBreakingChange && autoPropagationIsEnabledForConnection
  }

  fun shouldManuallyApply(
    diff: CatalogDiff,
    connectionRead: ConnectionRead,
  ): Boolean {
    if (!containsChanges(diff)) {
      // There is no schema diff to apply.
      return false
    }
    return (
      connectionRead.nonBreakingChangesPreference != null &&
        (
          connectionRead.nonBreakingChangesPreference == NonBreakingChangesPreference.IGNORE ||
            connectionRead.nonBreakingChangesPreference == NonBreakingChangesPreference.DISABLE
        )
    )
  }

  fun containsChanges(diff: CatalogDiff): Boolean = !diff.transforms.isEmpty()

  /**
   * Tests whether the provided catalog diff contains a breaking change.
   *
   * @param diff A [CatalogDiff].
   * @return `true` if any breaking field transforms are included in the diff, `false`
   * otherwise.
   */
  fun containsBreakingChange(diff: CatalogDiff): Boolean {
    for (streamTransform in diff.transforms) {
      if (streamTransform.transformType != StreamTransform.TransformTypeEnum.UPDATE_STREAM) {
        continue
      }

      val anyBreakingFieldTransforms =
        streamTransform.updateStream.fieldTransforms
          .stream()
          .anyMatch { obj: FieldTransform -> obj.breaking }

      val anyBreakingStreamAttributeTransforms =
        streamTransform.updateStream.streamAttributeTransforms
          .stream()
          .anyMatch { obj: StreamAttributeTransform -> obj.breaking }

      if (anyBreakingFieldTransforms || anyBreakingStreamAttributeTransforms) {
        return true
      }
    }
    return false
  }
}
