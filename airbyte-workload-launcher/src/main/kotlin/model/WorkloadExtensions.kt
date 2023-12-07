package io.airbyte.workload.launcher.model

import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput

typealias OpenApiWorkload = io.airbyte.workload.api.client.model.generated.Workload
typealias OpenApiWorkloadType = io.airbyte.workload.api.client.model.generated.WorkloadType
typealias InternalApiWorkloadType = io.airbyte.config.WorkloadType

fun OpenApiWorkloadType.toInternalApi(): io.airbyte.config.WorkloadType {
  return when (this) {
    OpenApiWorkloadType.CHECK -> InternalApiWorkloadType.CHECK
    OpenApiWorkloadType.DISCOVER -> InternalApiWorkloadType.DISCOVER
    OpenApiWorkloadType.SPEC -> InternalApiWorkloadType.SPEC
    OpenApiWorkloadType.SYNC -> InternalApiWorkloadType.SYNC
  }
}

fun OpenApiWorkload.toLauncherInput(): LauncherInput {
  return LauncherInput(
    this.id,
    this.inputPayload,
    this.labels.associate { it.key to it.value },
    this.logPath,
    this.mutexKey,
    this.type.toInternalApi(),
  )
}
