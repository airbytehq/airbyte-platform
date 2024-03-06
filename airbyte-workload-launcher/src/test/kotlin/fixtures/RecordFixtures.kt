package fixtures

import io.airbyte.config.WorkloadType
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import java.util.UUID

object RecordFixtures {
  fun launcherInput(
    workloadId: String = "1",
    workloadInput: String = "input-blob",
    labels: Map<String, String> = mapOf(),
    logPath: String = "/log/path",
    mutexKey: String? = null,
    workloadType: WorkloadType = WorkloadType.SYNC,
    autoId: UUID = UUID.randomUUID(),
  ): LauncherInput =
    LauncherInput(
      workloadId,
      workloadInput,
      labels,
      logPath,
      mutexKey,
      workloadType,
      autoId = autoId,
    )
}
