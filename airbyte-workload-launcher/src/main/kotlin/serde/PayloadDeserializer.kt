package io.airbyte.workload.launcher.serde

import io.airbyte.commons.json.Jsons
import io.airbyte.persistence.job.models.ReplicationInput
import jakarta.inject.Singleton

@Singleton
class PayloadDeserializer {
  fun toReplicationInput(payload: String): ReplicationInput {
    return Jsons.deserialize(payload, ReplicationInput::class.java)
  }
}
