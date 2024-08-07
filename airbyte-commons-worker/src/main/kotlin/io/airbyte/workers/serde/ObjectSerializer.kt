package io.airbyte.workers.serde

import io.airbyte.commons.json.Jsons
import jakarta.inject.Singleton

@Singleton
class ObjectSerializer {
  fun <T> serialize(config: T): String {
    return Jsons.serialize(config)
  }
}
