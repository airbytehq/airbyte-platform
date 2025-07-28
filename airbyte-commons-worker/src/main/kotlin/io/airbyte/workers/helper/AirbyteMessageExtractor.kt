/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import org.jooq.tools.StringUtils
import java.util.Optional

object AirbyteMessageExtractor {
  fun getCatalogStreamFromMessage(
    catalog: ConfiguredAirbyteCatalog,
    name: String?,
    namespace: String?,
  ): Optional<ConfiguredAirbyteStream> =
    catalog.streams
      .stream()
      .filter { configuredStream: ConfiguredAirbyteStream ->
        StringUtils.equals(configuredStream.stream.namespace, namespace) &&
          StringUtils.equals(configuredStream.stream.name, name)
      }.findFirst()

  @JvmStatic
  fun getCatalogStreamFromMessage(
    catalog: ConfiguredAirbyteCatalog,
    message: AirbyteRecordMessage,
  ): Optional<ConfiguredAirbyteStream> = getCatalogStreamFromMessage(catalog, message.stream, message.namespace)

  fun getPks(catalogStream: Optional<ConfiguredAirbyteStream>): List<List<String>> =
    catalogStream
      .map<List<List<String>>>(ConfiguredAirbyteStream::primaryKey)
      .orElse(ArrayList())

  fun containsNonNullPK(
    pks: List<String>,
    data: JsonNode?,
  ): Boolean = Jsons.navigateTo(data, pks) != null
}
