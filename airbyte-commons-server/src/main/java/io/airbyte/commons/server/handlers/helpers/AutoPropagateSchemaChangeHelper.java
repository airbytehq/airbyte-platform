/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.commons.json.Jsons;
import io.airbyte.featureflag.FeatureFlagClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.NotSupportedException;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper that allows to generate the catalogs to be auto propagated.
 */
@Slf4j
public class AutoPropagateSchemaChangeHelper {

  private enum DefaultSyncModeCase {
    SOURCE_CURSOR_AND_PRIMARY_KEY,
    SOURCE_CURSOR_NO_PRIMARY_KEY_SUPPORTS_FULL_REFRESH,
    SOURCE_CURSOR_NO_PRIMARY_KEY_NO_FULL_REFRESH,
    NO_SOURCE_CURSOR
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
   * @return a list of strings describing the changes that will be applied to the destination.
   */
  @VisibleForTesting
  static String staticFormatDiff(final StreamTransform transform) {
    String namespace = transform.getStreamDescriptor().getNamespace();
    String nsPrefix = namespace != null ? String.format("%s.", namespace) : "";
    String streamName = transform.getStreamDescriptor().getName();
    switch (transform.getTransformType()) {
      case ADD_STREAM -> {
        return String.format("Added new stream '%s%s'", nsPrefix, streamName);
      }
      case REMOVE_STREAM -> {
        return String.format("Removed stream '%s%s'", nsPrefix, streamName);
      }
      case UPDATE_STREAM -> {
        StringBuilder returnValue = new StringBuilder(String.format("Modified stream '%s%s': ", nsPrefix, streamName));
        if (transform.getUpdateStream() == null) {
          return returnValue.toString();
        }
        List<String> addedFields = new ArrayList<>();
        List<String> removedFields = new ArrayList<>();
        List<String> updatedFields = new ArrayList<>();

        for (final FieldTransform fieldTransform : transform.getUpdateStream()) {
          final String fieldName = String.join(".", fieldTransform.getFieldName());
          switch (fieldTransform.getTransformType()) {
            case ADD_FIELD -> addedFields.add(String.format("'%s'", fieldName));
            case REMOVE_FIELD -> removedFields.add(String.format("'%s'", fieldName));
            case UPDATE_FIELD_SCHEMA -> updatedFields.add(String.format("'%s'", fieldName));
            default -> throw new NotSupportedException("Not supported transformation.");
          }
        }
        List<String> detailedUpdates = new ArrayList<>();
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
   * This is auto propagating schema changes, it replaces the stream in the old catalog by using the
   * ones from the new catalog. The list of transformations contains the information of which stream
   * to update.
   *
   * @param oldCatalog the currently saved catalog
   * @param newCatalog the new catalog, which contains all the stream even the unselected ones
   * @param transformations list of transformation per stream
   * @param nonBreakingChangesPreference User preference for the auto propagation
   * @return an Airbyte catalog the changes being auto propagated
   */
  public static UpdateSchemaResult getUpdatedSchema(final AirbyteCatalog oldCatalog,
                                                    final AirbyteCatalog newCatalog,
                                                    final List<StreamTransform> transformations,
                                                    final NonBreakingChangesPreference nonBreakingChangesPreference,
                                                    final List<DestinationSyncMode> supportedDestinationSyncModes,
                                                    final FeatureFlagClient featureFlagClient,
                                                    final UUID workspaceId) {
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
            changes.add(staticFormatDiff(transformation));
            appliedDiff.addTransformsItem(transformation);
          }
        }
        case ADD_STREAM -> {
          if (nonBreakingChangesPreference.equals(NonBreakingChangesPreference.PROPAGATE_FULLY)) {
            final var streamAndConfigurationToAdd = newCatalogPerStream.get(streamDescriptor);
            // If we're propagating it, we want to enable it! Otherwise, it'll just get dropped when we update
            // the catalog.
            streamAndConfigurationToAdd.getConfig()
                .selected(true);
            CatalogConverter.configureDefaultSyncModesForNewStream(streamAndConfigurationToAdd.getStream(),
                streamAndConfigurationToAdd.getConfig());
            CatalogConverter.ensureCompatibleDestinationSyncMode(streamAndConfigurationToAdd, supportedDestinationSyncModes);
            // TODO(mfsiega-airbyte): handle the case where the chosen sync mode isn't actually one of the
            // supported sync modes.
            oldCatalogPerStream.put(streamDescriptor, streamAndConfigurationToAdd);
            changes.add(staticFormatDiff(transformation));
            appliedDiff.addTransformsItem(transformation);
          }
        }
        case REMOVE_STREAM -> {
          if (nonBreakingChangesPreference.equals(NonBreakingChangesPreference.PROPAGATE_FULLY)) {
            oldCatalogPerStream.remove(streamDescriptor);
            changes.add(staticFormatDiff(transformation));
            appliedDiff.addTransformsItem(transformation);
          }
        }
        default -> throw new NotSupportedException("Not supported transformation.");
      }
    });

    return new UpdateSchemaResult(new AirbyteCatalog().streams(List.copyOf(oldCatalogPerStream.values())), appliedDiff, changes);
  }

  @VisibleForTesting
  static Map<StreamDescriptor, AirbyteStreamAndConfiguration> extractStreamAndConfigPerStreamDescriptor(final AirbyteCatalog catalog) {
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
  public static boolean shouldAutoPropagate(final CatalogDiff diff,
                                            final ConnectionRead connectionRead) {
    final boolean hasDiff = !diff.getTransforms().isEmpty();
    final boolean nonBreakingChange = !AutoPropagateSchemaChangeHelper.containsBreakingChange(diff);
    final boolean autoPropagationIsEnabledForConnection =
        connectionRead.getNonBreakingChangesPreference() != null
            && (connectionRead.getNonBreakingChangesPreference().equals(NonBreakingChangesPreference.PROPAGATE_COLUMNS)
                || connectionRead.getNonBreakingChangesPreference().equals(NonBreakingChangesPreference.PROPAGATE_FULLY));
    return hasDiff && nonBreakingChange && autoPropagationIsEnabledForConnection;
  }

  /**
   * Tests whether the provided catalog diff contains a breaking change.
   *
   * @param diff A {@link CatalogDiff}.
   * @return {@code true} if any breaking field transforms are included in the diff, {@code false}
   *         otherwise.
   */
  public static boolean containsBreakingChange(final CatalogDiff diff) {
    for (final StreamTransform streamTransform : diff.getTransforms()) {
      if (streamTransform.getTransformType() != StreamTransform.TransformTypeEnum.UPDATE_STREAM) {
        continue;
      }

      final boolean anyBreakingFieldTransforms = streamTransform.getUpdateStream().stream().anyMatch(FieldTransform::getBreaking);
      if (anyBreakingFieldTransforms) {
        return true;
      }
    }
    return false;
  }

}
