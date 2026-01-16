/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import com.google.gson.Gson
import io.airbyte.config.ConfiguredAirbyteCatalog
import jakarta.inject.Singleton

private val gson = Gson()

/**
 * When the line are too big for jackson, we are failing the sync with a too big line error. This
 * error is not helping our users to identify which line is causing an issue. In order to be able to
 * identify which line is faulty, we need to use another library to parse the line and extract the
 * Pks from it. This class is doing that using GSON.
 */
@Singleton
class GsonPksExtractor {
  fun extractPks(
    catalog: ConfiguredAirbyteCatalog,
    line: String?,
  ): String {
    val jsonLine = gson.fromJson<Map<*, *>>(line, Map::class.java)["record"] as Map<*, *>

    val name = jsonLine["stream"] as String?
    val namespace = jsonLine["namespace"] as String?

    val catalogStream =
      AirbyteMessageExtractor.getCatalogStreamFromMessage(
        catalog,
        name,
        namespace,
      )

    return AirbyteMessageExtractor
      .getPks(catalogStream)
      .joinToString(separator = ",") {
        val key = it.joinToString(separator = ".")
        val value = navigateTo(jsonLine["data"], it) ?: "[MISSING]"
        "$key=$value"
      }
  }

  private fun navigateTo(
    json: Any?,
    keys: List<String?>,
  ): String? {
    var jsonInternal = json
    keys.forEach { key ->
      if (jsonInternal == null) {
        return null
      }
      jsonInternal = (jsonInternal as Map<String?, Any?>).get(key)
    }
    return jsonInternal?.toString()
  }
}
