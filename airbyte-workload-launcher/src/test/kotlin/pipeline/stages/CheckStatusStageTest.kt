package pipeline.stages

import io.airbyte.api.client2.AirbyteApiClient2
import io.airbyte.api.client2.model.generated.WorkloadStatus
import io.airbyte.api.client2.model.generated.WorkloadStatusUpdateRequest
import io.airbyte.workload.launcher.client.KubeClient
import io.airbyte.workload.launcher.mocks.LauncherInputMessage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.CheckStatusStage
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
      WorkloadStatusUpdateRequest(workloadId, WorkloadStatus.running)

    val airbyteApiClient: AirbyteApiClient2 = mockk()
    val kubernetesClient: KubeClient = mockk()

    every {
      airbyteApiClient.workloadApi.workloadStatusUpdate(
        workloadStatusUpdateRequest,
      )
    } returns Unit

    every {
      kubernetesClient.podsExistForWorkload(workloadId, namespace)
    } returns true

    val checkStatusStage: CheckStatusStage =
      spyk(CheckStatusStage(airbyteApiClient, kubernetesClient, namespace))

    val originalInput = LaunchStageIO(LauncherInputMessage(workloadId, "{}"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    verify { airbyteApiClient.workloadApi.workloadStatusUpdate(workloadStatusUpdateRequest) }

    assert(outputFromCheckStatusStage.skip) { "Skip Launch flag should be true but it's false" }
  }

  @Test
  fun `sets skip flag to false for non-running pods`() {
    val workloadId = "1"
    val namespace = "namespace"

    val workloadStatusUpdateRequest =
      WorkloadStatusUpdateRequest(workloadId, WorkloadStatus.running)

    val airbyteApiClient: AirbyteApiClient2 = mockk()
    val kubernetesClient: KubeClient = mockk()

    every {
      kubernetesClient.podsExistForWorkload(workloadId, namespace)
    } returns false

    val checkStatusStage: CheckStatusStage = CheckStatusStage(airbyteApiClient, kubernetesClient, namespace)

    val originalInput = LaunchStageIO(LauncherInputMessage(workloadId, "{}"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    verify(exactly = 0) { airbyteApiClient.workloadApi.workloadStatusUpdate(workloadStatusUpdateRequest) }

    assert(!outputFromCheckStatusStage.skip) { "Skip Launch flag should be false but it's true" }
  }

  @Test
  fun `sets skip flag to true in case of workload-api error`() {
    val workloadId = "1"
    val namespace = "namespace"

    val workloadStatusUpdateRequest =
      WorkloadStatusUpdateRequest(workloadId, WorkloadStatus.running)

    val airbyteApiClient: AirbyteApiClient2 = mockk()
    val kubernetesClient: KubeClient = mockk()

    every {
      airbyteApiClient.workloadApi.workloadStatusUpdate(
        workloadStatusUpdateRequest,
      )
    } throws ClientErrorException(400)

    every {
      kubernetesClient.podsExistForWorkload(workloadId, namespace)
    } returns true

    val checkStatusStage: CheckStatusStage = CheckStatusStage(airbyteApiClient, kubernetesClient, namespace)

    val originalInput = LaunchStageIO(LauncherInputMessage(workloadId, "{}"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    verify { airbyteApiClient.workloadApi.workloadStatusUpdate(workloadStatusUpdateRequest) }

    assert(outputFromCheckStatusStage.skip) { "Skip Launch flag should be true but it's false" }
  }
}
