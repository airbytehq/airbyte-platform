package io.airbyte.workload.launcher.serde

import io.airbyte.commons.json.Jsons
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.SpecInput
import jakarta.inject.Singleton

@Singleton
class PayloadDeserializer {
  fun toReplicationActivityInput(payload: String): ReplicationActivityInput {
    return Jsons.deserialize(payload, ReplicationActivityInput::class.java)
  }

  fun toCheckConnectionInput(payload: String): CheckConnectionInput {
    return Jsons.deserialize(payload, CheckConnectionInput::class.java)
  }

  fun toDiscoverCatalogInput(payload: String): DiscoverCatalogInput {
    return Jsons.deserialize(payload, DiscoverCatalogInput::class.java)
  }

  fun toSpecInput(payload: String): SpecInput {
    return Jsons.deserialize(payload, SpecInput::class.java)
  }
}
