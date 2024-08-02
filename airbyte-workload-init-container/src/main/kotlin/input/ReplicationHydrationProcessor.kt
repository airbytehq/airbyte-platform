package io.airbyte.initContainer.input

import io.airbyte.initContainer.system.FileClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.serde.ObjectSerializer
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workers.sync.OrchestratorConstants
import jakarta.inject.Singleton

/**
 * Parses, hydrates and writes input files for the replication orchestrator.
 */
@Singleton
class ReplicationHydrationProcessor(
  private val replicationInputHydrator: ReplicationInputHydrator,
  private val deserializer: PayloadDeserializer,
  private val serializer: ObjectSerializer,
  private val fileClient: FileClient,
) {
  fun process(rawPayload: String) {
    val parsed: ReplicationActivityInput = deserializer.toReplicationActivityInput(rawPayload)

    val hydrated: ReplicationInput = replicationInputHydrator.getHydratedReplicationInput(parsed)

    fileClient.writeInputFile(
      OrchestratorConstants.INIT_FILE_INPUT,
      serializer.serialize(hydrated),
    )
  }
}
