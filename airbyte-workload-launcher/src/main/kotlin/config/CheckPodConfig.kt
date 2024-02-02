package io.airbyte.workload.launcher.config

import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

@Singleton
data class CheckPodConfig(
  @Value("\${airbyte.worker.check.cpu-limit:2}") val cpuLimit: String,
  @Value("\${airbyte.worker.check.cpu-request:2}") val cpuRequest: String,
  @Value("\${airbyte.worker.check.memory-limit:500Mi}") val memoryLimit: String,
  @Value("\${airbyte.worker.check.memory-request:500Mi}") val memoryRequest: String,
)
