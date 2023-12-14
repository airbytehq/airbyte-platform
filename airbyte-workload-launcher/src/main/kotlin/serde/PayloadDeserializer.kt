package io.airbyte.workload.launcher.serde

import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.workers.models.ReplicationActivityInput
import jakarta.inject.Singleton

@Singleton
class PayloadDeserializer {
  fun toReplicationActivityInput(payload: String): ReplicationActivityInput {
    return Jsons.deserialize(payload, ReplicationActivityInput::class.java)
  }

  fun toStandardCheckConnectionInput(payload: String): StandardCheckConnectionInput {
    return Jsons.deserialize(payload, StandardCheckConnectionInput::class.java)
  }
}
