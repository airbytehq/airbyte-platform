package io.airbyte.workload.launcher.pipeline

import java.io.File

data class LauncherInput(
  val workloadId: String,
  val workloadInput: String,
  val jobLogPath: String = File.createTempFile("log-path", ".txt").absolutePath,
)
