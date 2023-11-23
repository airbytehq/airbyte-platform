package io.airbyte.workload.launcher.pods

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.process.Metadata.ORCHESTRATOR_REPLICATION_STEP
import io.airbyte.workers.process.Metadata.READ_STEP
import io.airbyte.workers.process.Metadata.SYNC_STEP_KEY
import io.airbyte.workers.process.Metadata.WRITE_STEP
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.MUTEX_KEY
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.WORKLOAD_ID
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream

class PodLabelerTest {
  @ParameterizedTest
  @MethodSource("replInputWorkloadIdMatrix")
  fun getSourceLabels(
    input: ReplicationInput,
    workloadId: String,
    passThroughLabels: Map<String, String>,
  ) {
    val labeler = PodLabeler()
    val result = labeler.getSourceLabels(input, workloadId, passThroughLabels)

    assert(
      result ==
        passThroughLabels +
        mapOf(
          MUTEX_KEY to input.connectionId.toString(),
          WORKLOAD_ID to workloadId,
          SYNC_STEP_KEY to READ_STEP,
        ),
    )
  }

  @ParameterizedTest
  @MethodSource("replInputWorkloadIdMatrix")
  fun getDestinationLabels(
    input: ReplicationInput,
    workloadId: String,
    passThroughLabels: Map<String, String>,
  ) {
    val labeler = PodLabeler()
    val result = labeler.getDestinationLabels(input, workloadId, passThroughLabels)

    assert(
      result ==
        passThroughLabels +
        mapOf(
          MUTEX_KEY to input.connectionId.toString(),
          WORKLOAD_ID to workloadId,
          SYNC_STEP_KEY to WRITE_STEP,
        ),
    )
  }

  @ParameterizedTest
  @MethodSource("replInputWorkloadIdMatrix")
  fun getOrchestratorLabels(
    input: ReplicationInput,
    workloadId: String,
    passThroughLabels: Map<String, String>,
  ) {
    val labeler = PodLabeler()
    val result = labeler.getOrchestratorLabels(input, workloadId, passThroughLabels)

    assert(
      result ==
        passThroughLabels +
        mapOf(
          MUTEX_KEY to input.connectionId.toString(),
          WORKLOAD_ID to workloadId,
          SYNC_STEP_KEY to ORCHESTRATOR_REPLICATION_STEP,
        ),
    )
  }

  @SuppressWarnings("PMD.UnusedFormalParameter")
  @ParameterizedTest
  @MethodSource("replInputWorkloadIdMatrix")
  fun getWorkloadLabels(
    unusedInput: ReplicationInput,
    workloadId: String,
    unusedLabels: Map<String, String>,
  ) {
    val labeler = PodLabeler()
    val result = labeler.getWorkloadLabels(workloadId)

    assert(
      result ==
        mapOf(
          WORKLOAD_ID to workloadId,
        ),
    )
  }

  @SuppressWarnings("PMD.UnusedFormalParameter")
  @ParameterizedTest
  @MethodSource("replInputWorkloadIdMatrix")
  fun getMutexLabels(
    input: ReplicationInput,
    unusedWorkloadId: String,
    unusedLabels: Map<String, String>,
  ) {
    val labeler = PodLabeler()
    val result = labeler.getMutexLabels(input)

    assert(
      result ==
        mapOf(
          MUTEX_KEY to input.connectionId.toString(),
        ),
    )
  }

  companion object {
    @JvmStatic
    private fun replInputWorkloadIdMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(
          ReplicationInput().withConnectionId(UUID.randomUUID()),
          UUID.randomUUID().toString(),
          mapOf("random labels1" to "from input msg1"),
        ),
        Arguments.of(
          ReplicationInput().withConnectionId(UUID.randomUUID()),
          UUID.randomUUID().toString(),
          mapOf("random labels2" to "from input msg2"),
        ),
      )
    }
  }
}
