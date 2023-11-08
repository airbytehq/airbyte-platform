/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.workload.api.client2.generated.WorkloadApi
import io.airbyte.workload.api.client2.model.generated.ClaimResponse
import io.airbyte.workload.api.client2.model.generated.WorkloadClaimRequest
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.LauncherInput
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

    val workloadApiClient: WorkloadApi = mockk()
    every {
      workloadApiClient.workloadClaim(
        workloadClaimRequest,
      )
    } returns ClaimResponse(true)

    val claimStage = ClaimStage(workloadApiClient, dataplaneId)
    val originalInput = LaunchStageIO(LauncherInput(workloadId, "{}"))
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

    val workloadApiClient: WorkloadApi = mockk()
    every {
      workloadApiClient.workloadClaim(
        workloadClaimRequest,
      )
    } returns ClaimResponse(false)

    val claimStage = ClaimStage(workloadApiClient, dataplaneId)
    val originalInput = LaunchStageIO(LauncherInput(workloadId, "{}"))
    val outputFromClaimStage = claimStage.applyStage(originalInput)

    verify { workloadApiClient.workloadClaim(workloadClaimRequest) }

    assert(outputFromClaimStage.skip) { "Skip Launch flag should be true but it's false" }
  }

  @Test
  fun `propagates error if claim request error`() {
    val workloadId = "1"
    val dataplaneId = "US"

    val workloadClaimRequest: WorkloadClaimRequest =
      WorkloadClaimRequest(
        workloadId,
        dataplaneId,
      )

    val workloadApiClient: WorkloadApi = mockk()
    every {
      workloadApiClient.workloadClaim(
        workloadClaimRequest,
      )
    } throws ClientErrorException(400)

    val claimStage = ClaimStage(workloadApiClient, dataplaneId)
    val originalInput = LaunchStageIO(LauncherInput(workloadId, "{}"))

    assertThrows<ClientErrorException> { claimStage.applyStage(originalInput) }

    verify { workloadApiClient.workloadClaim(workloadClaimRequest) }
  }
}
