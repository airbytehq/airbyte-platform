/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.jooq.tools.StringUtils;

public class AirbyteMessageExtractor {

  public static Optional<ConfiguredAirbyteStream> getCatalogStreamFromMessage(final ConfiguredAirbyteCatalog catalog,
                                                                              final String name,
                                                                              final String namespace) {
    return catalog.getStreams()
        .stream()
        .filter(configuredStream -> StringUtils.equals(configuredStream.getStream().getNamespace(), namespace)
            && StringUtils.equals(configuredStream.getStream().getName(), name))
        .findFirst();
  }

  public static Optional<ConfiguredAirbyteStream> getCatalogStreamFromMessage(final ConfiguredAirbyteCatalog catalog,
                                                                              final AirbyteRecordMessage message) {
    return getCatalogStreamFromMessage(catalog, message.getStream(), message.getNamespace());
  }

  public static List<List<String>> getPks(final Optional<ConfiguredAirbyteStream> catalogStream) {
    return catalogStream
        .map(ConfiguredAirbyteStream::getPrimaryKey)
        .orElse(new ArrayList<>());
  }

  public static boolean containsNonNullPK(final List<String> pks, final JsonNode data) {
    return Jsons.navigateTo(data, pks) != null;
  }

}
