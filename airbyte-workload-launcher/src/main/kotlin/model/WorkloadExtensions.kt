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
    workloadId = this.id,
    workloadInput = this.inputPayload,
    labels = this.labels.associate { it.key to it.value },
    logPath = this.logPath,
    mutexKey = this.mutexKey,
    workloadType = this.type.toInternalApi(),
    autoId = this.autoId,
  )
}
