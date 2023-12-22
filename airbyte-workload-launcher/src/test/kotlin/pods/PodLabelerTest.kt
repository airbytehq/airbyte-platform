package io.airbyte.workload.launcher.pods

import io.airbyte.workers.process.Metadata.CHECK_JOB
import io.airbyte.workers.process.Metadata.CHECK_STEP_KEY
import io.airbyte.workers.process.Metadata.CONNECTOR_STEP
import io.airbyte.workers.process.Metadata.IMAGE_NAME
import io.airbyte.workers.process.Metadata.IMAGE_VERSION
import io.airbyte.workers.process.Metadata.JOB_TYPE_KEY
import io.airbyte.workers.process.Metadata.ORCHESTRATOR_CHECK_STEP
import io.airbyte.workers.process.Metadata.ORCHESTRATOR_REPLICATION_STEP
import io.airbyte.workers.process.Metadata.READ_STEP
import io.airbyte.workers.process.Metadata.SYNC_JOB
import io.airbyte.workers.process.Metadata.SYNC_STEP_KEY
import io.airbyte.workers.process.Metadata.WRITE_STEP
import io.airbyte.workers.process.ProcessFactory
import io.airbyte.workload.launcher.pods.PodLabeler.LabelKeys.MUTEX_KEY
import io.airbyte.workload.launcher.pods.PodLabeler.LabelKeys.WORKLOAD_ID
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream

class PodLabelerTest {
  @Test
  fun getSourceLabels() {
    val labeler = PodLabeler(ORCHESTRATOR_IMAGE_NAME)
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
    val labeler = PodLabeler(ORCHESTRATOR_IMAGE_NAME)
    val result = labeler.getDestinationLabels()

    assert(
      result ==
        mapOf(
          SYNC_STEP_KEY to WRITE_STEP,
        ),
    )
  }

  @Test
  fun getReplicationOrchestratorLabels() {
    val labeler = PodLabeler(ORCHESTRATOR_IMAGE_NAME)
    val result = labeler.getReplicationOrchestratorLabels()
    val shortImageName = ProcessFactory.getShortImageName(ORCHESTRATOR_IMAGE_NAME)
    val imageVersion = ProcessFactory.getImageVersion(ORCHESTRATOR_IMAGE_NAME)

    assert(
      result ==
        mapOf(
          IMAGE_NAME to shortImageName,
          IMAGE_VERSION to imageVersion,
          JOB_TYPE_KEY to SYNC_JOB,
          SYNC_STEP_KEY to ORCHESTRATOR_REPLICATION_STEP,
        ),
    )
  }

  @Test
  fun getCheckConnectorLabels() {
    val labeler = PodLabeler(ORCHESTRATOR_IMAGE_NAME)
    val result = labeler.getCheckConnectorLabels()

    assert(
      result ==
        mapOf(
          JOB_TYPE_KEY to CHECK_JOB,
          CHECK_STEP_KEY to CONNECTOR_STEP,
        ),
    )
  }

  @Test
  fun getCheckOrchestratorLabels() {
    val labeler = PodLabeler(ORCHESTRATOR_IMAGE_NAME)
    val result = labeler.getCheckOrchestratorLabels()
    val shortImageName = ProcessFactory.getShortImageName(ORCHESTRATOR_IMAGE_NAME)
    val imageVersion = ProcessFactory.getImageVersion(ORCHESTRATOR_IMAGE_NAME)

    assert(
      result ==
        mapOf(
          IMAGE_NAME to shortImageName,
          IMAGE_VERSION to imageVersion,
          JOB_TYPE_KEY to CHECK_JOB,
          CHECK_STEP_KEY to ORCHESTRATOR_CHECK_STEP,
        ),
    )
  }

  @ParameterizedTest
  @MethodSource("randomStringMatrix")
  fun getWorkloadLabels(workloadId: String) {
    val labeler = PodLabeler(ORCHESTRATOR_IMAGE_NAME)
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
    val labeler = PodLabeler(ORCHESTRATOR_IMAGE_NAME)
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
    val labeler = PodLabeler(ORCHESTRATOR_IMAGE_NAME)
    val result = labeler.getSharedLabels(workloadId, mutexKey, passThroughLabels)

    assert(
      result ==
        passThroughLabels +
        labeler.getWorkloadLabels(workloadId) +
        labeler.getMutexLabels(mutexKey),
    )
  }

  companion object {
    const val ORCHESTRATOR_IMAGE_NAME: String = "an image"

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
