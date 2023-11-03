package pipeline.stages

import io.airbyte.workload.launcher.client.StatusUpdater
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.LauncherInput
import io.airbyte.workload.launcher.pipeline.stages.CheckStatusStage
import io.airbyte.workload.launcher.pods.KubePodClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import javax.ws.rs.ClientErrorException

class CheckStatusStageTest {
  @Test
  fun `sets skip flag to true for running pods`() {
    val workloadId = "1"

    val statusUpdater: StatusUpdater = mockk()
    val kubernetesClient: KubePodClient = mockk()

    every {
      statusUpdater.updateStatusToRunning(
        workloadId,
      )
    } returns Unit

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns true

    val checkStatusStage = CheckStatusStage(statusUpdater, kubernetesClient)

    val originalInput = LaunchStageIO(LauncherInput(workloadId, "{}"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    verify { statusUpdater.updateStatusToRunning(workloadId) }

    assert(outputFromCheckStatusStage.skip) { "Skip Launch flag should be true but it's false" }
  }

  @Test
  fun `sets skip flag to false for non-running pods`() {
    val workloadId = "1"

    val statusUpdater: StatusUpdater = mockk()
    val kubernetesClient: KubePodClient = mockk()

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns false

    val checkStatusStage = CheckStatusStage(statusUpdater, kubernetesClient)

    val originalInput = LaunchStageIO(LauncherInput(workloadId, "{}"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    verify(exactly = 0) { statusUpdater.updateStatusToRunning(workloadId) }

    assert(!outputFromCheckStatusStage.skip) { "Skip Launch flag should be false but it's true" }
  }

  @Test
  fun `error is propagated in case of workload-api error`() {
    val workloadId = "1"

    val statusUpdater: StatusUpdater = mockk()
    val kubernetesClient: KubePodClient = mockk()

    every {
      statusUpdater.updateStatusToRunning(
        workloadId,
      )
    } throws ClientErrorException(400)

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns true

    val checkStatusStage = CheckStatusStage(statusUpdater, kubernetesClient)

    val originalInput = LaunchStageIO(LauncherInput(workloadId, "{}"))

    assertThrows<ClientErrorException> { checkStatusStage.applyStage(originalInput) }
  }
}
