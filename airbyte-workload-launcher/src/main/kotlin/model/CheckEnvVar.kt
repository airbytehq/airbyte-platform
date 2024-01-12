package io.airbyte.workload.launcher.model

import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.Optional

@Singleton
data class CheckEnvVar(
  @Named("airbyte.worker.check.annotations") val annotations: Optional<String>,
  @Named("airbyte.worker.check.labels") val labels: Optional<String>,
  @Named("airbyte.worker.check.node-selectors") val nodeSelectors: Optional<String>,
  @Named("airbyte.worker.check.cpu-limit") val cpuLimit: Optional<String>,
  @Named("airbyte.worker.check.cpu-request") val cpuRequest: Optional<String>,
  @Named("airbyte.worker.check.memory-limit") val memoryLimit: Optional<String>,
  @Named("airbyte.worker.check.memory-request") val memoryRequest: Optional<String>,
) {
  fun getEnvMap(): Map<String, String> {
    return mapOf(
      "CHECK_JOB_KUBE_ANNOTATIONS" to annotations,
      "CHECK_JOB_KUBE_LABELS" to labels,
      "CHECK_JOB_KUBE_NODE_SELECTORS" to nodeSelectors,
      "CHECK_JOB_MAIN_CONTAINER_CPU_LIMIT" to cpuLimit,
      "CHECK_JOB_MAIN_CONTAINER_CPU_REQUEST" to cpuRequest,
      "CHECK_JOB_MAIN_CONTAINER_MEMORY_LIMIT" to memoryLimit,
      "CHECK_JOB_MAIN_CONTAINER_MEMORY_REQUEST" to memoryRequest,
    )
      .filterValues { it.isPresent }
      .mapValues { it.value.get() }
  }
}
