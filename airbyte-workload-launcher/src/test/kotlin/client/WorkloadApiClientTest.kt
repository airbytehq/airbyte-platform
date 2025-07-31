/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.client

import io.airbyte.api.client.ApiException
import io.airbyte.workload.api.domain.ClaimResponse
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val APPLICATION_NAME = "airbyte-workload-launcher"

internal class WorkloadApiClientTest {
  private lateinit var identifyService: DataplaneIdentityService
  private lateinit var workloadApiClient: WorkloadApiClient
  private lateinit var internalWorkloadApiClient: io.airbyte.workload.api.client.WorkloadApiClient

  @BeforeEach
  internal fun setup() {
    internalWorkloadApiClient = mockk()
    identifyService = mockk(relaxed = true)
    workloadApiClient = WorkloadApiClient(internalWorkloadApiClient, identifyService, APPLICATION_NAME)
  }

  @Test
  internal fun `test reporting a failure to the workload API`() {
    val workloadId = "workload-id"
    val requestCapture = slot<WorkloadFailureRequest>()

    every { internalWorkloadApiClient.workloadFailure(any()) } returns Unit
    val failure = RuntimeException("Cause")

    workloadApiClient.reportFailure(workloadId, failure)

    verify(exactly = 1) { internalWorkloadApiClient.workloadFailure(capture(requestCapture)) }
    assertEquals(workloadId, requestCapture.captured.workloadId)
  }

  @Test
  internal fun `test reporting a failed status to the workload API`() {
    val workloadId = "workload-id"
    val requestCapture = slot<WorkloadFailureRequest>()

    every { internalWorkloadApiClient.workloadFailure(any()) } returns Unit

    workloadApiClient.updateStatusToFailed(workloadId, "Cause")

    verify(exactly = 1) { internalWorkloadApiClient.workloadFailure(capture(requestCapture)) }
    assertEquals(workloadId, requestCapture.captured.workloadId)
  }

  @Test
  internal fun `test submitting a successful claim to the Workload API for a workload`() {
    val workloadId = "workload-id"
    val response: ClaimResponse = mockk()

    every { response.claimed } returns true
    every { internalWorkloadApiClient.workloadClaim(any()) } returns response

    val claimResult = workloadApiClient.claim(workloadId)

    verify(exactly = 1) { internalWorkloadApiClient.workloadClaim(any()) }
    assertEquals(response.claimed, claimResult)
  }

  @Test
  internal fun `test if an exception occurs during a claim request to the Workload API, the workload is not claimed`() {
    val workloadId = "workload-id"

    every { internalWorkloadApiClient.workloadClaim(any()) } throws
      ApiException(message = "test", statusCode = HttpStatus.NOT_FOUND.code, url = "http://localhost/test")

    val claimResult = workloadApiClient.claim(workloadId)

    verify(exactly = 1) { internalWorkloadApiClient.workloadClaim(any()) }
    assertEquals(false, claimResult)
  }
}
