package fixtures

import io.airbyte.config.WorkloadType
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput

object RecordFixtures {
  fun launcherInput(
    workloadId: String = "1",
    workloadInput: String = "input-blob",
    labels: Map<String, String> = mapOf(),
    logPath: String = "/log/path",
    mutexKey: String? = null,
    workloadType: WorkloadType = WorkloadType.SYNC,
  ): LauncherInput =
    LauncherInput(
      workloadId,
      workloadInput,
      labels,
      logPath,
      mutexKey,
      workloadType,
    )
}
