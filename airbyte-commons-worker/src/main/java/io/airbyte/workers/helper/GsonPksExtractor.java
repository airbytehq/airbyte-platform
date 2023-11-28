/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * When the line are too big for jackson, we are failine the sync with a too big line error. This
 * error is not helping our users to identify which line is causing an issue. In order to be able to
 * identify which line is faulty, we need to use another library to parse the line and extract the
 * Pks from it. This class is doing that using GSON.
 */
@Singleton
public class GsonPksExtractor {

  private final Gson gson = new Gson();

  public String extractPks(final ConfiguredAirbyteCatalog catalog,
                           final String line) {
    final Map jsonLine = (Map) gson.fromJson(line, Map.class).get("record");

    final String name = (String) jsonLine.get("stream");
    final String namespace = (String) jsonLine.get("namespace");

    final Optional<ConfiguredAirbyteStream> catalogStream = AirbyteMessageExtractor.getCatalogStreamFromMessage(catalog,
        name,
        namespace);
    final List<List<String>> pks = AirbyteMessageExtractor.getPks(catalogStream);

    final Map<String, String> pkValues = new HashMap<>();

    pks.forEach(pk -> {
      final String value = navigateTo(jsonLine.get("data"), pk);
      pkValues.put(String.join(".", pk), value == null ? "[MISSING]" : value);
    });

    return mapToString(pkValues);
  }

  private String mapToString(final Map<String, String> map) {
    return Joiner.on(",").withKeyValueSeparator("=").join(map);
  }

  private String navigateTo(final Object json, final List<String> keys) {
    Object jsonInternal = json;
    for (final String key : keys) {
      if (jsonInternal == null) {
        return null;
      }
      jsonInternal = ((Map<String, Object>) jsonInternal).get(key);
    }
    return jsonInternal == null ? null : jsonInternal.toString();
  }

}
