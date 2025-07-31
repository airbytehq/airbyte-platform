/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client

import io.airbyte.api.client.ApiException
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.workload.api.domain.ClaimResponse
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadDepthResponse
import io.airbyte.workload.api.domain.WorkloadListResponse
import io.airbyte.workload.api.domain.WorkloadQueueStatsResponse
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import retrofit2.Response
import retrofit2.mock.Calls

class WorkloadApiClientTest {
  @Test
  fun `verify correct workload-api methods are called`() {
    val workloadApi =
      mockk<WorkloadApi> {
        every { workloadCreate(any()) } returns Calls.response(Unit)
        every { workloadFailure(any()) } returns Calls.response(Unit)
        every { workloadSuccess(any()) } returns Calls.response(Unit)
        every { workloadRunning(any()) } returns Calls.response(Unit)
        every { workloadCancel(any()) } returns Calls.response(Unit)
        every { workloadLaunched(any()) } returns Calls.response(Unit)
        every { workloadHeartbeat(any()) } returns Calls.response(Unit)
        every { workloadQueueClean(any()) } returns Calls.response(Unit)

        every { workloadClaim(any()) } returns Calls.response(mockk<ClaimResponse>())
        every { workloadGet(any()) } returns Calls.response(mockk<Workload>())
        every { workloadList(any()) } returns Calls.response(mockk<WorkloadListResponse>())
        every { workloadListWithExpiredDeadline(any()) } returns Calls.response(mockk<WorkloadListResponse>())
        every { workloadListOldNonSync(any()) } returns Calls.response(mockk<WorkloadListResponse>())
        every { workloadListOldSync(any()) } returns Calls.response(mockk<WorkloadListResponse>())
        every { pollWorkloadQueue(any()) } returns Calls.response(mockk<WorkloadListResponse>())
        every { countWorkloadQueueDepth(any()) } returns Calls.response(mockk<WorkloadDepthResponse>())
        every { getWorkloadQueueStats() } returns Calls.response(mockk<WorkloadQueueStatsResponse>())
      }

    val metricClient = mockk<MetricClient>()

    val workloadApiClient =
      WorkloadApiClient(
        metricClient = metricClient,
        api = workloadApi,
        retryConfig = RetryPolicyConfig(),
      )

    with(workloadApiClient) {
      workloadCreate(mockk())
      workloadFailure(mockk())
      workloadSuccess(mockk())
      workloadRunning(mockk())
      workloadCancel(mockk())
      workloadLaunched(mockk())
      workloadHeartbeat(mockk())
      workloadQueueClean(mockk())

      workloadClaim(mockk())
      workloadGet("dummy")
      workloadList(mockk())
      workloadListWithExpiredDeadline(mockk())
      workloadListOldNonSync(mockk())
      workloadListOldSync(mockk())
      pollWorkloadQueue(mockk())
      countWorkloadQueueDepth(mockk())
      getWorkloadQueueStats()
    }

    verify {
      with(workloadApi) {
        workloadCreate(any())
        workloadFailure(any())
        workloadSuccess(any())
        workloadRunning(any())
        workloadCancel(any())
        workloadLaunched(any())
        workloadHeartbeat(any())
        workloadQueueClean(any())

        workloadClaim(any())
        workloadGet(any())
        workloadList(any())
        workloadListWithExpiredDeadline(any())
        workloadListOldNonSync(any())
        workloadListOldSync(any())
        pollWorkloadQueue(any())
        countWorkloadQueueDepth(any())
        getWorkloadQueueStats()
      }
    }
  }

  @Test
  fun `verify ApiException is thrown for non-success`() {
    val workloadApi =
      mockk<WorkloadApi> {
        // test something that returns a Unit
        every { workloadCreate(any()) } returns Calls.response(Response<Unit>.error(400, "".toResponseBody()))
        // test something that returns a non-unit
        every { workloadClaim(any()) } returns Calls.response(Response<Unit>.error(404, "".toResponseBody()))
      }

    val metricClient = mockk<MetricClient>()

    val workloadApiClient =
      WorkloadApiClient(
        metricClient = metricClient,
        api = workloadApi,
        retryConfig = RetryPolicyConfig(),
      )

    assertThrows<ApiException> { workloadApiClient.workloadCreate(mockk()) }.let {
      assertEquals(400, it.statusCode)
      assertEquals("Response.error()", it.message)
      assertEquals("http://localhost/", it.url)
    }
    assertThrows<ApiException> { workloadApiClient.workloadClaim(mockk()) }.let {
      assertEquals(404, it.statusCode)
      assertEquals("Response.error()", it.message)
      assertEquals("http://localhost/", it.url)
    }
  }

  @Test
  fun `verify heartbeat with GONE response is returned`() {
    val workloadApi =
      mockk<WorkloadApi> {
        every { workloadHeartbeat(any()) } returns Calls.response(Response<Unit>.error(HttpStatus.GONE.code, "".toResponseBody()))
      }

    val metricClient = mockk<MetricClient>()

    val workloadApiClient =
      WorkloadApiClient(
        metricClient = metricClient,
        api = workloadApi,
        retryConfig = RetryPolicyConfig(),
      )

    assertThrows<ApiException> { workloadApiClient.workloadHeartbeat(mockk()) }.let {
      assertEquals(HttpStatus.GONE.code, it.statusCode)
      assertEquals("Response.error()", it.message)
      assertEquals("http://localhost/", it.url)
    }
  }

  // I attempted to also create tests for all the failure/retry scenarios.  I can see that the proper `onRetry`, `onFailure` handlers
  // are being called but I cannot get mockk to register those calls.  It does work for the success case however.
  @Test
  fun `verify success metric client`() {
    val workloadApi =
      mockk<WorkloadApi> {
        every { workloadHeartbeat(any()) } returns Calls.response(Unit)
      }

    val metricClient = mockk<MetricClient>()

    val workloadApiClient =
      WorkloadApiClient(
        metricClient = metricClient,
        api = workloadApi,
        retryConfig = RetryPolicyConfig(),
      )

    workloadApiClient.workloadHeartbeat(mockk())
    verify(exactly = 1) {
      metricClient.count(metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_SUCCESS, attributes = anyVararg<MetricAttribute>())
    }

    verify(exactly = 0) {
      metricClient.count(metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_ABORT, attributes = anyVararg<MetricAttribute>())
      metricClient.count(metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_FAILURE, attributes = anyVararg<MetricAttribute>())
      metricClient.count(metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_RETRY, attributes = anyVararg<MetricAttribute>())
      metricClient.count(metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_RETRIES_EXCEEDED, attributes = anyVararg<MetricAttribute>())
    }
  }
}
