/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.SelectedFieldInfo;
import io.airbyte.api.model.generated.StreamAttributeTransform;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.commons.json.Jsons;
import jakarta.inject.Singleton;
import jakarta.ws.rs.NotSupportedException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper that allows to generate the catalogs to be auto propagated.
 */
@Singleton
public class ApplySchemaChangeHelper {

  private final CatalogConverter catalogConverter;

  public ApplySchemaChangeHelper(final CatalogConverter catalogConverter) {
    this.catalogConverter = catalogConverter;
  }

  /**
   * Return value for `getUpdatedSchema` method. Returns the updated catalog and a description of the
   * changes
   */
  public record UpdateSchemaResult(AirbyteCatalog catalog, CatalogDiff appliedDiff, List<String> changeDescription) {}

  /**
   * Generate a summary of the changes that will be applied to the destination if the schema is
   * updated.
   *
   * @param transform the transformation to be applied to the destination
   * @return a string describing the changes that will be applied to the destination.
   */
  @VisibleForTesting
  String formatDiff(final StreamTransform transform) {
    final String namespace = transform.getStreamDescriptor().getNamespace();
    final String nsPrefix = namespace != null ? String.format("%s.", namespace) : "";
    final String streamName = transform.getStreamDescriptor().getName();
    switch (transform.getTransformType()) {
      case ADD_STREAM -> {
        return String.format("Added new stream '%s%s'", nsPrefix, streamName);
      }
      case REMOVE_STREAM -> {
        return String.format("Removed stream '%s%s'", nsPrefix, streamName);
      }
      case UPDATE_STREAM -> {
        final StringBuilder returnValue = new StringBuilder(String.format("Modified stream '%s%s': ", nsPrefix, streamName));
        if (transform.getUpdateStream() == null) {
          return returnValue.toString();
        }
        final List<String> addedFields = new ArrayList<>();
        final List<String> removedFields = new ArrayList<>();
        final List<String> updatedFields = new ArrayList<>();

        for (final FieldTransform fieldTransform : transform.getUpdateStream().getFieldTransforms()) {
          final String fieldName = String.join(".", fieldTransform.getFieldName());
          switch (fieldTransform.getTransformType()) {
            case ADD_FIELD -> addedFields.add(String.format("'%s'", fieldName));
            case REMOVE_FIELD -> removedFields.add(String.format("'%s'", fieldName));
            case UPDATE_FIELD_SCHEMA -> updatedFields.add(String.format("'%s'", fieldName));
            default -> throw new NotSupportedException("Not supported transformation.");
          }
        }
        final List<String> detailedUpdates = new ArrayList<>();
        if (!addedFields.isEmpty()) {
          detailedUpdates.add(String.format("Added fields [%s]", String.join(", ", addedFields)));
        }
        if (!removedFields.isEmpty()) {
          detailedUpdates.add(String.format("Removed fields [%s]", String.join(", ", removedFields)));
        }
        if (!updatedFields.isEmpty()) {
          detailedUpdates.add(String.format("Altered fields [%s]", String.join(", ", updatedFields)));
        }
        returnValue.append(String.join(", ", detailedUpdates));
        return returnValue.toString();
      }
      default -> {
        return "Unknown Transformation";
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
   *         descriptions
   */
  public UpdateSchemaResult getUpdatedSchema(final AirbyteCatalog oldCatalog,
                                             final AirbyteCatalog newCatalog,
                                             final List<StreamTransform> transformations,
                                             final NonBreakingChangesPreference nonBreakingChangesPreference,
                                             final List<DestinationSyncMode> supportedDestinationSyncModes) {
    final AirbyteCatalog copiedOldCatalog = Jsons.clone(oldCatalog);
    final Map<StreamDescriptor, AirbyteStreamAndConfiguration> oldCatalogPerStream = extractStreamAndConfigPerStreamDescriptor(copiedOldCatalog);
    final Map<StreamDescriptor, AirbyteStreamAndConfiguration> newCatalogPerStream = extractStreamAndConfigPerStreamDescriptor(newCatalog);

    final List<String> changes = new ArrayList<>();
    final CatalogDiff appliedDiff = new CatalogDiff();

    transformations.forEach(transformation -> {
      final StreamDescriptor streamDescriptor = transformation.getStreamDescriptor();
      switch (transformation.getTransformType()) {
        case UPDATE_STREAM -> {
          if (oldCatalogPerStream.containsKey(streamDescriptor)) {
            oldCatalogPerStream.get(streamDescriptor)
                .stream(newCatalogPerStream.get(streamDescriptor).getStream());
            final var streamConfig = oldCatalogPerStream.get(streamDescriptor).getConfig();
            if (Boolean.TRUE == streamConfig.getFieldSelectionEnabled()) {
              final Set<String> selectedFields =
                  streamConfig.getSelectedFields().stream().map(i -> i.getFieldPath().get(0)).collect(Collectors.toSet());
              final Set<String> newlySelectedFields = new HashSet<>();
              for (final var fieldTransform : transformation.getUpdateStream().getFieldTransforms()) {
                if (fieldTransform.getTransformType().equals(FieldTransform.TransformTypeEnum.ADD_FIELD)
                    && fieldTransform.getFieldName().size() == 1) {
                  final String newTopLevelField = fieldTransform.getFieldName().get(0);
                  if (!selectedFields.contains(newTopLevelField)) {
                    newlySelectedFields.add(fieldTransform.getFieldName().get(0));
                  }
                }
              }
              final var allSelectedFields = Stream.concat(
                  streamConfig.getSelectedFields().stream(),
                  newlySelectedFields.stream().map(field -> new SelectedFieldInfo().fieldPath(List.of(field)))).toList();
              streamConfig.setSelectedFields(allSelectedFields);
            }
            changes.add(formatDiff(transformation));
            appliedDiff.addTransformsItem(transformation);
          }
        }
        case ADD_STREAM -> {
          if (nonBreakingChangesPreference.equals(NonBreakingChangesPreference.PROPAGATE_FULLY)) {
            final var streamAndConfigurationToAdd = newCatalogPerStream.get(streamDescriptor);
            // Enable the stream if we're propagating it; otherwise, it'll get dropped when we update the
            // catalog.
            streamAndConfigurationToAdd.getConfig().selected(true);
            catalogConverter.configureDefaultSyncModesForNewStream(streamAndConfigurationToAdd.getStream(),
                streamAndConfigurationToAdd.getConfig());
            catalogConverter.ensureCompatibleDestinationSyncMode(streamAndConfigurationToAdd, supportedDestinationSyncModes);
            // TODO(mfsiega-airbyte): handle the case where the chosen sync mode isn't actually one of the
            // supported sync modes.
            oldCatalogPerStream.put(streamDescriptor, streamAndConfigurationToAdd);
            changes.add(formatDiff(transformation));
            appliedDiff.addTransformsItem(transformation);
          }
        }
        case REMOVE_STREAM -> {
          if (nonBreakingChangesPreference.equals(NonBreakingChangesPreference.PROPAGATE_FULLY)) {
            oldCatalogPerStream.remove(streamDescriptor);
            changes.add(formatDiff(transformation));
            appliedDiff.addTransformsItem(transformation);
          }
        }
        default -> throw new NotSupportedException("Not supported transformation.");
      }
    });

    return new UpdateSchemaResult(new AirbyteCatalog().streams(List.copyOf(oldCatalogPerStream.values())), appliedDiff, changes);
  }

  @VisibleForTesting
  Map<StreamDescriptor, AirbyteStreamAndConfiguration> extractStreamAndConfigPerStreamDescriptor(final AirbyteCatalog catalog) {
    return catalog.getStreams().stream().collect(Collectors.toMap(
        airbyteStreamAndConfiguration -> new StreamDescriptor().name(airbyteStreamAndConfiguration.getStream().getName())
            .namespace(airbyteStreamAndConfiguration.getStream().getNamespace()),
        airbyteStreamAndConfiguration -> airbyteStreamAndConfiguration));
  }

  /**
   * Tests whether auto-propagation should be applied based on the connection/workspace configs and
   * the diff.
   *
   * @param diff the diff to be applied
   * @param connectionRead the connection info
   * @return whether the diff should be propagated
   */
  public boolean shouldAutoPropagate(final CatalogDiff diff,
                                     final ConnectionRead connectionRead) {
    if (!containsChanges(diff)) {
      // If there's no diff, we always propagate because it means there's a diff in a disabled stream or
      // some other metadata.
      // We want to acknowledge it and update to the latest source catalog ID without bothering the user.
      return true;
    }
    final boolean nonBreakingChange = !containsBreakingChange(diff);
    final boolean autoPropagationIsEnabledForConnection =
        connectionRead.getNonBreakingChangesPreference() != null
            && (connectionRead.getNonBreakingChangesPreference().equals(NonBreakingChangesPreference.PROPAGATE_COLUMNS)
                || connectionRead.getNonBreakingChangesPreference().equals(NonBreakingChangesPreference.PROPAGATE_FULLY));
    return nonBreakingChange && autoPropagationIsEnabledForConnection;
  }

  public boolean shouldManuallyApply(final CatalogDiff diff,
                                     final ConnectionRead connectionRead) {
    if (!containsChanges(diff)) {
      // There is no schema diff to apply.
      return false;
    }
    return (connectionRead.getNonBreakingChangesPreference() != null
        && connectionRead.getNonBreakingChangesPreference().equals(NonBreakingChangesPreference.IGNORE));
  }

  public boolean containsChanges(final CatalogDiff diff) {
    return !diff.getTransforms().isEmpty();
  }

  /**
   * Tests whether the provided catalog diff contains a breaking change.
   *
   * @param diff A {@link CatalogDiff}.
   * @return {@code true} if any breaking field transforms are included in the diff, {@code false}
   *         otherwise.
   */
  public boolean containsBreakingChange(final CatalogDiff diff) {
    for (final StreamTransform streamTransform : diff.getTransforms()) {
      if (streamTransform.getTransformType() != StreamTransform.TransformTypeEnum.UPDATE_STREAM) {
        continue;
      }

      final boolean anyBreakingFieldTransforms =
          streamTransform.getUpdateStream().getFieldTransforms().stream().anyMatch(FieldTransform::getBreaking);

      final boolean anyBreakingStreamAttributeTransforms =
          streamTransform.getUpdateStream().getStreamAttributeTransforms().stream().anyMatch(StreamAttributeTransform::getBreaking);

      if (anyBreakingFieldTransforms || anyBreakingStreamAttributeTransforms) {
        return true;
      }
    }
    return false;
  }

}
