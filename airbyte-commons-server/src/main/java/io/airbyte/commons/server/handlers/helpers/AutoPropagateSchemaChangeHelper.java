/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.commons.json.Jsons;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.NotSupportedException;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper that allows to generate the catalogs to be auto propagated.
 */
@Slf4j
public class AutoPropagateSchemaChangeHelper {

  /**
   * This is auto propagating schema changes, it replaces the stream in the old catalog by using the
   * ones from the new catalog. The list of transformations contains the information of which stream
   * to update.
   *
   * @param oldCatalog the currently saved catalog
   * @param newCatalog the new catalog, which contains all the stream even the unselected ones
   * @param transformations list of transformation per stream
   * @return an Airbyte catalog the changes being auto propagated
   */
  public static AirbyteCatalog getUpdatedSchema(final AirbyteCatalog oldCatalog,
                                                final AirbyteCatalog newCatalog,
                                                final List<StreamTransform> transformations) {
    AirbyteCatalog copiedOldCatalog = Jsons.clone(oldCatalog);
    Map<StreamDescriptor, AirbyteStreamAndConfiguration> oldCatalogPerStream = extractStreamAndConfigPerStreamDescriptor(copiedOldCatalog);
    Map<StreamDescriptor, AirbyteStreamAndConfiguration> newCatalogPerStream = extractStreamAndConfigPerStreamDescriptor(newCatalog);

    transformations.forEach(transformation -> {
      StreamDescriptor streamDescriptor = transformation.getStreamDescriptor();
      switch (transformation.getTransformType()) {
        case UPDATE_STREAM -> oldCatalogPerStream.get(streamDescriptor)
            .stream(newCatalogPerStream.get(streamDescriptor).getStream());
        case ADD_STREAM -> oldCatalogPerStream.put(streamDescriptor, newCatalogPerStream.get(streamDescriptor));
        case REMOVE_STREAM -> oldCatalogPerStream.remove(streamDescriptor);
        default -> throw new NotSupportedException("Not supported transformation.");
      }
    });

    return new AirbyteCatalog().streams(List.copyOf(oldCatalogPerStream.values()));
  }

  @VisibleForTesting
  static Map<StreamDescriptor, AirbyteStreamAndConfiguration> extractStreamAndConfigPerStreamDescriptor(AirbyteCatalog catalog) {
    return catalog.getStreams().stream().collect(Collectors.toMap(
        airbyteStreamAndConfiguration -> new StreamDescriptor().name(airbyteStreamAndConfiguration.getStream().getName())
            .namespace(airbyteStreamAndConfiguration.getStream().getNamespace()),
        airbyteStreamAndConfiguration -> airbyteStreamAndConfiguration));
  }

}
