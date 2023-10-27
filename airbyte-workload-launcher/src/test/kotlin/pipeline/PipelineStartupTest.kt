package pipeline

import io.airbyte.api.client2.model.generated.Workload
import io.airbyte.api.client2.model.generated.WorkloadListRequest
import io.airbyte.api.client2.model.generated.WorkloadListResponse
import io.airbyte.api.client2.model.generated.WorkloadStatus
import io.airbyte.workload.launcher.PipelineRunner
import io.airbyte.workload.launcher.StartupApplicationEventListener
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.mocks.LauncherInputMessage
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Test

class PipelineStartupTest {
  @Test
  fun `should process claimed workloads`() {
    val workloadId = "1"
    val dataplaneId = "US"
    val launcherInputMessage = LauncherInputMessage(workloadId, "workload-input")

    val workloadApiClient: WorkloadApiClient = mockk()
    val pipelineRunner: PipelineRunner = mockk()
    val launchPipeline: LaunchPipeline = mockk()

    every {
      launchPipeline.accept(launcherInputMessage)
    } returns Unit

    val workloadListRequest =
      WorkloadListRequest(
        listOf(dataplaneId),
        listOf(WorkloadStatus.claimed),
      )

    val workload: Workload =
      Workload(workloadId, dataplaneId, WorkloadStatus.claimed)

    val workloadListResponse = WorkloadListResponse(listOf(workload))

    every {
      workloadApiClient.workloadList(
        workloadListRequest,
      )
    } returns workloadListResponse

    val startupApplicationEventListener =
      spyk(
        StartupApplicationEventListener(
          pipelineRunner,
          workloadApiClient,
          launchPipeline,
          dataplaneId,
        ),
      )

    startupApplicationEventListener.rehydrateAndProcessClaimed()

    verify { workloadApiClient.workloadList(workloadListRequest) }
    verify { startupApplicationEventListener.convertToInputMessage(workload) }
    verify { launchPipeline.accept(launcherInputMessage) }
  }
}
