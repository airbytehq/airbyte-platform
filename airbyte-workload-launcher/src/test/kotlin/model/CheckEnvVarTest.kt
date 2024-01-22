package model

import io.airbyte.workload.launcher.model.CheckEnvVar
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.Optional

class CheckEnvVarTest {
  @Test
  fun `Test that the map is properly generated`() {
    val annotations = "annotations"
    val labels = "labels"
    val nodeSelectors = "nodeSelectors"
    val cpuLimit = "cpuLimit"
    val cpuRequest = "cpuRequest"
    val memoryLimit = "memoryLimit"
    val memoryRequest = "memoryRequest"

    val checkEnvVar =
      CheckEnvVar(
        Optional.of(annotations),
        Optional.of(labels),
        Optional.of(nodeSelectors),
        Optional.of(cpuLimit),
        Optional.of(cpuRequest),
        Optional.of(memoryLimit),
        Optional.of(memoryRequest),
      )

    assertEquals(
      mapOf(
        "CHECK_JOB_KUBE_ANNOTATIONS" to annotations,
        "CHECK_JOB_KUBE_LABELS" to labels,
        "CHECK_JOB_KUBE_NODE_SELECTORS" to nodeSelectors,
        "CHECK_JOB_MAIN_CONTAINER_CPU_LIMIT" to cpuLimit,
        "CHECK_JOB_MAIN_CONTAINER_CPU_REQUEST" to cpuRequest,
        "CHECK_JOB_MAIN_CONTAINER_MEMORY_LIMIT" to memoryLimit,
        "CHECK_JOB_MAIN_CONTAINER_MEMORY_REQUEST" to memoryRequest,
      ),
      checkEnvVar.getEnvMap(),
    )
  }

  @Test
  fun `Test that missing variable are ignored`() {
    val checkEnvVar =
      CheckEnvVar(
        Optional.of("anno"),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
      )

    assertEquals(1, checkEnvVar.getEnvMap().size)
  }
}
