/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.client

import io.airbyte.config.WorkloadType
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.ClaimResponse
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadRunningRequest
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.StageError
import io.airbyte.workload.launcher.pipeline.stages.model.StageIO
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID

internal class WorkloadApiClientTest {
  private lateinit var workloadApiClient: WorkloadApiClient
  private lateinit var workloadApi: WorkloadApi
  private lateinit var internalWorkloadApiClient: io.airbyte.workload.api.client.WorkloadApiClient

  @BeforeEach
  internal fun setup() {
    workloadApi = mockk()
    internalWorkloadApiClient = mockk()
    workloadApiClient = WorkloadApiClient(internalWorkloadApiClient, DATA_PLANE_ID)

    every { internalWorkloadApiClient.workloadApi } returns workloadApi
  }

  @Test
  internal fun `test reporting a failure to the workload API`() {
    val workloadId = "workload-id"
    val launcherInput =
      LauncherInput(
        workloadId = workloadId,
        workloadInput = "",
        labels = mapOf(),
        logPath = "",
        workloadType = WorkloadType.SYNC,
        mutexKey = "",
        autoId = UUID.randomUUID(),
      )
    val stageIo: StageIO = mockk()
    val requestCapture = slot<WorkloadFailureRequest>()

    every { workloadApi.workloadFailure(any()) } returns Unit
    every { stageIo.msg } returns launcherInput
    val failure = StageError(stageIo, StageName.LAUNCH, RuntimeException("Cause"))

    workloadApiClient.reportFailure(failure)

    verify(exactly = 1) { workloadApi.workloadFailure(capture(requestCapture)) }
    assertEquals(workloadId, requestCapture.captured.workloadId)
  }

  @Test
  internal fun `test that a failure is not reported to the Workload API for the claim stage`() {
    val workloadId = "workload-id"
    val launcherInput =
      LauncherInput(
        workloadId = workloadId,
        workloadInput = "",
        labels = mapOf(),
        logPath = "",
        workloadType = WorkloadType.SYNC,
        mutexKey = "",
        autoId = UUID.randomUUID(),
      )
    val stageIo: StageIO = mockk()
    val failure: StageError = mockk()

    every { workloadApi.workloadFailure(any()) } returns Unit
    every { stageIo.msg } returns launcherInput
    every { failure.stageName } returns StageName.CLAIM
    every { failure.io } returns stageIo

    workloadApiClient.reportFailure(failure)

    verify(exactly = 0) { workloadApi.workloadFailure(any()) }
  }

  @Test
  internal fun `test reporting a running status to the workload API`() {
    val workloadId = "workload-id"
    val requestCapture = slot<WorkloadRunningRequest>()

    every { workloadApi.workloadRunning(any()) } returns Unit

    workloadApiClient.updateStatusToRunning(workloadId)

    verify(exactly = 1) { workloadApi.workloadRunning(capture(requestCapture)) }
    assertEquals(workloadId, requestCapture.captured.workloadId)
  }

  @Test
  internal fun `test reporting a failed status to the workload API`() {
    val workloadId = "workload-id"
    val requestCapture = slot<WorkloadFailureRequest>()

    val launcherInput = mockk<LauncherInput>()
    every { launcherInput.workloadId } returns workloadId
    val io = mockk<StageIO>()
    every { io.msg } returns launcherInput
    every { workloadApi.workloadFailure(any()) } returns Unit

    workloadApiClient.updateStatusToFailed(StageError(io, StageName.CLAIM, RuntimeException("Cause")))

    verify(exactly = 1) { workloadApi.workloadFailure(capture(requestCapture)) }
    assertEquals(workloadId, requestCapture.captured.workloadId)
  }

  @Test
  internal fun `test submitting a successful claim to the Workload API for a workload`() {
    val workloadId = "workload-id"
    val response: ClaimResponse = mockk()

    every { response.claimed } returns true
    every { workloadApi.workloadClaim(any()) } returns response

    val claimResult = workloadApiClient.claim(workloadId)

    verify(exactly = 1) { workloadApi.workloadClaim(any()) }
    assertEquals(response.claimed, claimResult)
  }

  @Test
  internal fun `test if an exception occurs during a claim request to the Workload API, the workload is not claimed`() {
    val workloadId = "workload-id"

    every { workloadApi.workloadClaim(any()) } throws ClientException(message = "test", statusCode = HttpStatus.NOT_FOUND.code)

    val claimResult = workloadApiClient.claim(workloadId)

    verify(exactly = 1) { workloadApi.workloadClaim(any()) }
    assertEquals(false, claimResult)
  }

  companion object {
    const val DATA_PLANE_ID = "data-plane-id"
  }
}
