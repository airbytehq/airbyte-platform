package io.airbyte.initContainer.input

import io.airbyte.initContainer.system.FileClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.input.setDestinationLabels
import io.airbyte.workers.input.setSourceLabels
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.pod.PodLabeler
import io.airbyte.workers.serde.ObjectSerializer
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.api.client.model.generated.Workload
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Parses, hydrates and writes input files for the replication orchestrator.
 */
@Requires(property = "airbyte.init.operation", pattern = "sync")
@Requires(property = "airbyte.init.monopod", pattern = "false")
@Singleton
class OrchestratorHydrationProcessor(
  private val replicationInputHydrator: ReplicationInputHydrator,
  private val deserializer: PayloadDeserializer,
  private val serializer: ObjectSerializer,
  private val fileClient: FileClient,
  private val labeler: PodLabeler,
) : InputHydrationProcessor {
  override fun process(workload: Workload) {
    logger.info { "Deserializing replication input..." }
    val rawPayload = workload.inputPayload
    val parsed: ReplicationActivityInput = deserializer.toReplicationActivityInput(rawPayload)

    logger.info { "Hydrating replication input..." }
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

    // orchestrator input
    logger.info { "Writing orchestrator inputs..." }
    fileClient.writeInputFile(
      FileConstants.INIT_INPUT_FILE,
      serializer.serialize(inputWithLabels),
    )
  }
}
