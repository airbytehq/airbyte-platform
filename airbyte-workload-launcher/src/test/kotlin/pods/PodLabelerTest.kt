package io.airbyte.workload.launcher.pods

import io.airbyte.workers.process.Metadata.ORCHESTRATOR_REPLICATION_STEP
import io.airbyte.workers.process.Metadata.READ_STEP
import io.airbyte.workers.process.Metadata.SYNC_STEP_KEY
import io.airbyte.workers.process.Metadata.WRITE_STEP
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.MUTEX_KEY
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.WORKLOAD_ID
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream

class PodLabelerTest {
  @Test
  fun getSourceLabels() {
    val labeler = PodLabeler()
    val result = labeler.getSourceLabels()

    assert(
      result ==
        mapOf(
          SYNC_STEP_KEY to READ_STEP,
        ),
    )
  }

  @Test
  fun getDestinationLabels() {
    val labeler = PodLabeler()
    val result = labeler.getDestinationLabels()

    assert(
      result ==
        mapOf(
          SYNC_STEP_KEY to WRITE_STEP,
        ),
    )
  }

  @Test
  fun getOrchestratorLabels() {
    val labeler = PodLabeler()
    val result = labeler.getOrchestratorLabels()

    assert(
      result ==
        mapOf(
          SYNC_STEP_KEY to ORCHESTRATOR_REPLICATION_STEP,
        ),
    )
  }

  @ParameterizedTest
  @MethodSource("randomStringMatrix")
  fun getWorkloadLabels(workloadId: String) {
    val labeler = PodLabeler()
    val result = labeler.getWorkloadLabels(workloadId)

    assert(
      result ==
        mapOf(
          WORKLOAD_ID to workloadId,
        ),
    )
  }

  @ParameterizedTest
  @MethodSource("randomStringMatrix")
  fun getMutexLabels(key: String) {
    val labeler = PodLabeler()
    val result = labeler.getMutexLabels(key)

    assert(
      result ==
        mapOf(
          MUTEX_KEY to key,
        ),
    )
  }

  @ParameterizedTest
  @MethodSource("replInputWorkloadIdMatrix")
  fun getSharedLabels(
    workloadId: String,
    mutexKey: String?,
    passThroughLabels: Map<String, String>,
  ) {
    val labeler = PodLabeler()
    val result = labeler.getSharedLabels(workloadId, mutexKey, passThroughLabels)

    assert(
      result ==
        passThroughLabels +
        labeler.getWorkloadLabels(workloadId) +
        labeler.getMutexLabels(mutexKey),
    )
  }

  companion object {
    @JvmStatic
    private fun replInputWorkloadIdMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          mapOf("random labels1" to "from input msg1"),
        ),
        Arguments.of(
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          mapOf("random labels2" to "from input msg2"),
        ),
        Arguments.of(
          UUID.randomUUID().toString(),
          null,
          mapOf("random labels3" to "from input msg3"),
        ),
      )
    }

    @JvmStatic
    private fun randomStringMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of("random string id 1"),
        Arguments.of("RANdoM strIng Id 2"),
        Arguments.of("literally anything"),
        Arguments.of("89127421"),
        Arguments.of("false"),
        Arguments.of("{}"),
      )
    }
  }
}
