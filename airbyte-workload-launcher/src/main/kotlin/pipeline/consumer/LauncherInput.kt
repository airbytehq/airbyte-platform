package io.airbyte.workload.launcher.pipeline.consumer

import io.airbyte.config.WorkloadType

data class LauncherInput(
  val workloadId: String,
  val workloadInput: String,
  val labels: Map<String, String>,
  val logPath: String,
  val mutexKey: String?,
  val workloadType: WorkloadType,
  val startTimeMs: Long? = null,
)
