/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.commons.json.Jsons;
import io.airbyte.featureflag.AutoPropagateNewStreams;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Workspace;
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
  public record UpdateSchemaResult(AirbyteCatalog catalog, List<String> changeDescription) {}

  /**
   * Generate a summary of the changes that will be applied to the destination if the schema is
   * updated.
   *
   * @param transform the transformation to be applied to the destination
   * @return a list of strings describing the changes that will be applied to the destination.
   */
  private static String staticFormatDiff(StreamTransform transform) {
    switch (transform.getTransformType()) {
      case ADD_STREAM -> {
        return String.format("Added new stream %s", transform.getStreamDescriptor().getName());
      }
      case REMOVE_STREAM -> {
        return String.format("Removed stream %s", transform.getStreamDescriptor().getName());
      }
      case UPDATE_STREAM -> {
        String returnValue = String.format("Modified stream %s", transform.getStreamDescriptor().getName());
        if (transform.getUpdateStream() == null) {
          return returnValue;
        }
        for (FieldTransform fieldTransform : transform.getUpdateStream()) {
          String fieldName = String.join(".", fieldTransform.getFieldName());
          switch (fieldTransform.getTransformType()) {
            case ADD_FIELD -> returnValue += String.format("Added field: %s,", fieldName);
            case REMOVE_FIELD -> returnValue += String.format("Removed field: %s,", fieldName);
            case UPDATE_FIELD_SCHEMA -> returnValue += String.format("Field type changed: %s,", fieldName);
            default -> throw new NotSupportedException("Not supported transformation.");
          }
        }
        return returnValue;
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

    List<String> changes = new ArrayList<>();

    transformations.forEach(transformation -> {
      final StreamDescriptor streamDescriptor = transformation.getStreamDescriptor();
      switch (transformation.getTransformType()) {
        case UPDATE_STREAM -> {
          if (oldCatalogPerStream.containsKey(streamDescriptor)) {
            oldCatalogPerStream.get(streamDescriptor)
                .stream(newCatalogPerStream.get(streamDescriptor).getStream());
            changes.add(staticFormatDiff(transformation));
          }
        }
        case ADD_STREAM -> {
          if (nonBreakingChangesPreference.equals(NonBreakingChangesPreference.PROPAGATE_FULLY)) {
            final var streamAndConfigurationToAdd = newCatalogPerStream.get(streamDescriptor);
            if (featureFlagClient.boolVariation(AutoPropagateNewStreams.INSTANCE, new Workspace(workspaceId))) {
              // If we're propagating it, we want to enable it! Otherwise, it'll just get dropped when we update
              // the catalog.
              streamAndConfigurationToAdd.getConfig()
                  .selected(true);
              CatalogConverter.configureDefaultSyncModesForNewStream(streamAndConfigurationToAdd.getStream(),
                  streamAndConfigurationToAdd.getConfig());
              CatalogConverter.ensureCompatibleDestinationSyncMode(streamAndConfigurationToAdd, supportedDestinationSyncModes);
            }
            // TODO(mfsiega-airbyte): handle the case where the chosen sync mode isn't actually one of the
            // supported sync modes.
            oldCatalogPerStream.put(streamDescriptor, streamAndConfigurationToAdd);
            changes.add(staticFormatDiff(transformation));
          }
        }
        case REMOVE_STREAM -> {
          if (nonBreakingChangesPreference.equals(NonBreakingChangesPreference.PROPAGATE_FULLY)) {
            oldCatalogPerStream.remove(streamDescriptor);
            changes.add(staticFormatDiff(transformation));
          }
        }
        default -> throw new NotSupportedException("Not supported transformation.");
      }
    });

    return new UpdateSchemaResult(new AirbyteCatalog().streams(List.copyOf(oldCatalogPerStream.values())), changes);
  }

  @VisibleForTesting
  static Map<StreamDescriptor, AirbyteStreamAndConfiguration> extractStreamAndConfigPerStreamDescriptor(final AirbyteCatalog catalog) {
    return catalog.getStreams().stream().collect(Collectors.toMap(
        airbyteStreamAndConfiguration -> new StreamDescriptor().name(airbyteStreamAndConfiguration.getStream().getName())
            .namespace(airbyteStreamAndConfiguration.getStream().getNamespace()),
        airbyteStreamAndConfiguration -> airbyteStreamAndConfiguration));
  }

  @VisibleForTesting
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
