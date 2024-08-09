package io.airbyte.initContainer.input

import io.airbyte.initContainer.system.FileClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.input.setDestinationLabels
import io.airbyte.workers.input.setSourceLabels
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.pod.PodLabeler
import io.airbyte.workers.serde.ObjectSerializer
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workload.api.client.model.generated.Workload
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

/**
 * Parses, hydrates and writes input files for the replication orchestrator.
 */
@Requires(property = "airbyte.init.operation", pattern = "sync")
@Singleton
class ReplicationHydrationProcessor(
  private val replicationInputHydrator: ReplicationInputHydrator,
  private val deserializer: PayloadDeserializer,
  private val serializer: ObjectSerializer,
  private val fileClient: FileClient,
  private val labeler: PodLabeler,
) : InputHydrationProcessor {
  override fun process(workload: Workload) {
    val rawPayload = workload.inputPayload
    val parsed: ReplicationActivityInput = deserializer.toReplicationActivityInput(rawPayload)

    val hydrated: ReplicationInput = replicationInputHydrator.getHydratedReplicationInput(parsed)

    val labels =
      labeler.getSharedLabels(
        workloadId = workload.id,
        mutexKey = workload.mutexKey,
        autoId = workload.autoId,
        passThroughLabels = workload.labels.associate { it.key to it.value },
      )

    val inputWithLabels =
      hydrated
        .setSourceLabels(labels)
        .setDestinationLabels(labels)

    fileClient.writeInputFile(
      OrchestratorConstants.INIT_FILE_INPUT,
      serializer.serialize(inputWithLabels),
    )
  }
}
