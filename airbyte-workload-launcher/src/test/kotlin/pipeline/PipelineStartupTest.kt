package pipeline

import io.airbyte.workload.api.client2.model.generated.Workload
import io.airbyte.workload.api.client2.model.generated.WorkloadListRequest
import io.airbyte.workload.api.client2.model.generated.WorkloadListResponse
import io.airbyte.workload.api.client2.model.generated.WorkloadStatus
import io.airbyte.workload.launcher.StartupApplicationEventListener
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.airbyte.workload.launcher.pipeline.LauncherInput
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import io.temporal.worker.WorkerFactory
import org.junit.jupiter.api.Test

class PipelineStartupTest {
  @Test
  fun `should process claimed workloads`() {
    val workloadId = "1"
    val dataplaneId = "US"
    val launcherInput = LauncherInput(workloadId, "workload-input")

    val workloadApiClient: WorkloadApiClient = mockk()
    val workerFactory: WorkerFactory = mockk()
    val launchPipeline: LaunchPipeline = mockk()

    every {
      launchPipeline.accept(launcherInput)
    } returns Unit

    val workloadListRequest =
      WorkloadListRequest(
        listOf(dataplaneId),
        listOf(WorkloadStatus.cLAIMED),
      )

    val workload: Workload =
      Workload(workloadId, dataplaneId, WorkloadStatus.cLAIMED)

    val workloadListResponse = WorkloadListResponse(listOf(workload))

    every {
      workloadApiClient.workloadList(
        workloadListRequest,
      )
    } returns workloadListResponse

    val startupApplicationEventListener =
      spyk(
        StartupApplicationEventListener(
          workloadApiClient,
          launchPipeline,
          workerFactory,
          dataplaneId,
        ),
      )

    startupApplicationEventListener.rehydrateAndProcessClaimed()

    verify { workloadApiClient.workloadList(workloadListRequest) }
    verify { startupApplicationEventListener.convertToInputMessage(workload) }
    verify { launchPipeline.accept(launcherInput) }
  }
}
