package pipeline.stages

import io.airbyte.workload.api.client2.generated.WorkloadApi
import io.airbyte.workload.api.client2.model.generated.WorkloadStatus
import io.airbyte.workload.api.client2.model.generated.WorkloadStatusUpdateRequest
import io.airbyte.workload.launcher.mocks.LauncherInputMessage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.CheckStatusStage
import io.airbyte.workload.launcher.pods.KubePodClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Test
import javax.ws.rs.ClientErrorException

class CheckStatusStageTest {
  @Test
  fun `sets skip flag to true for running pods`() {
    val workloadId = "1"
    val namespace = "namespace"

    val workloadStatusUpdateRequest =
      WorkloadStatusUpdateRequest(workloadId, WorkloadStatus.rUNNING)

    val workloadApi: WorkloadApi = mockk()
    val kubernetesClient: KubePodClient = mockk()

    every {
      workloadApi.workloadStatusUpdate(
        workloadStatusUpdateRequest,
      )
    } returns Unit

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns true

    val checkStatusStage: CheckStatusStage =
      spyk(CheckStatusStage(workloadApi, kubernetesClient))

    val originalInput = LaunchStageIO(LauncherInputMessage(workloadId, "{}"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    verify { workloadApi.workloadStatusUpdate(workloadStatusUpdateRequest) }

    assert(outputFromCheckStatusStage.skip) { "Skip Launch flag should be true but it's false" }
  }

  @Test
  fun `sets skip flag to false for non-running pods`() {
    val workloadId = "1"
    val namespace = "namespace"

    val workloadStatusUpdateRequest =
      WorkloadStatusUpdateRequest(workloadId, WorkloadStatus.rUNNING)

    val workloadApi: WorkloadApi = mockk()
    val kubernetesClient: KubePodClient = mockk()

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns false

    val checkStatusStage: CheckStatusStage = CheckStatusStage(workloadApi, kubernetesClient)

    val originalInput = LaunchStageIO(LauncherInputMessage(workloadId, "{}"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    verify(exactly = 0) { workloadApi.workloadStatusUpdate(workloadStatusUpdateRequest) }

    assert(!outputFromCheckStatusStage.skip) { "Skip Launch flag should be false but it's true" }
  }

  @Test
  fun `sets skip flag to true in case of workload-api error`() {
    val workloadId = "1"
    val namespace = "namespace"

    val workloadStatusUpdateRequest =
      WorkloadStatusUpdateRequest(workloadId, WorkloadStatus.rUNNING)

    val workloadApi: WorkloadApi = mockk()
    val kubernetesClient: KubePodClient = mockk()

    every {
      workloadApi.workloadStatusUpdate(
        workloadStatusUpdateRequest,
      )
    } throws ClientErrorException(400)

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns true

    val checkStatusStage: CheckStatusStage = CheckStatusStage(workloadApi, kubernetesClient)

    val originalInput = LaunchStageIO(LauncherInputMessage(workloadId, "{}"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    verify { workloadApi.workloadStatusUpdate(workloadStatusUpdateRequest) }

    assert(outputFromCheckStatusStage.skip) { "Skip Launch flag should be true but it's false" }
  }
}
