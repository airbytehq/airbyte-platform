package io.airbyte.workload.launcher.pipeline

data class LauncherInput(
  val workloadId: String,
  val workloadInput: String,
  val labels: Map<String, String>,
  val logPath: String,
)
