package io.airbyte.workload.launcher.model

import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput

fun Workload.toLauncherInput(): LauncherInput {
  return LauncherInput(
    this.id,
    this.inputPayload,
    this.labels.associate { it.key to it.value },
    this.logPath,
  )
}
