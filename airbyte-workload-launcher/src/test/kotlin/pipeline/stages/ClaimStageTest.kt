package pipeline.stages

import io.airbyte.workload.api.client2.model.generated.ClaimResponse
import io.airbyte.workload.api.client2.model.generated.WorkloadClaimRequest
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.mocks.LauncherInputMessage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.ClaimStage
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import javax.ws.rs.ClientErrorException

class ClaimStageTest {
  @Test
  fun `sets skip flag to false for successful claim`() {
    val workloadId = "1"
    val dataplaneId = "US"

    val workloadClaimRequest =
      WorkloadClaimRequest(
        workloadId,
        dataplaneId,
      )

    val workloadApiClient: WorkloadApiClient = mockk()
    every {
      workloadApiClient.workloadClaim(
        workloadClaimRequest,
      )
    } returns ClaimResponse(true)

    val claimStage = ClaimStage(workloadApiClient, dataplaneId)
    val originalInput = LaunchStageIO(LauncherInputMessage(workloadId, "{}"))
    val outputFromClaimStage = claimStage.applyStage(originalInput)

    verify { workloadApiClient.workloadClaim(workloadClaimRequest) }

    assert(!outputFromClaimStage.skip) { "Skip Launch flag should be false but it's true" }
  }

  @Test
  fun `sets skip flag to true for un-successful claim`() {
    val workloadId = "1"
    val dataplaneId = "US"

    val workloadClaimRequest =
      WorkloadClaimRequest(
        workloadId,
        dataplaneId,
      )

    val workloadApiClient: WorkloadApiClient = mockk()
    every {
      workloadApiClient.workloadClaim(
        workloadClaimRequest,
      )
    } returns ClaimResponse(false)

    val claimStage = ClaimStage(workloadApiClient, dataplaneId)
    val originalInput = LaunchStageIO(LauncherInputMessage(workloadId, "{}"))
    val outputFromClaimStage = claimStage.applyStage(originalInput)

    verify { workloadApiClient.workloadClaim(workloadClaimRequest) }

    assert(outputFromClaimStage.skip) { "Skip Launch flag should be true but it's false" }
  }

  @Test
  fun `propagates error if claim request error`() {
    val workloadId: String = "1"
    val dataplaneId: String = "US"

    val workloadClaimRequest: WorkloadClaimRequest =
      WorkloadClaimRequest(
        workloadId,
        dataplaneId,
      )

    val workloadApiClient: WorkloadApiClient = mockk()
    every {
      workloadApiClient.workloadClaim(
        workloadClaimRequest,
      )
    } throws ClientErrorException(400)

    val claimStage = ClaimStage(workloadApiClient, dataplaneId)
    val originalInput = LaunchStageIO(LauncherInputMessage(workloadId, "{}"))

    assertThrows<ClientErrorException> { claimStage.applyStage(originalInput) }

    verify { workloadApiClient.workloadClaim(workloadClaimRequest) }
  }
}
