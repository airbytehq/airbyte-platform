package io.airbyte.workload.launcher.serde

import io.airbyte.commons.json.Jsons
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.ReplicationActivityInput
import jakarta.inject.Singleton

@Singleton
class PayloadDeserializer {
  fun toReplicationActivityInput(payload: String): ReplicationActivityInput {
    return Jsons.deserialize(payload, ReplicationActivityInput::class.java)
  }

  fun toReplicationInput(payload: String): ReplicationInput {
    return Jsons.deserialize(payload, ReplicationInput::class.java)
  }
}
