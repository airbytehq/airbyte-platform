/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client

import dev.failsafe.RetryPolicy
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.generated.infrastructure.ClientError
import io.airbyte.workload.api.client.generated.infrastructure.ClientException
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import okhttp3.OkHttpClient
import okhttp3.Response
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * [WorkloadApi] is a generated class using OpenAPI Generator. We are making some changes to the generated class
 * in a gradle task. This test is meant to test these changes.
 */
class WorkloadApiTest {
  companion object {
    const val MESSAGE = "message"
    const val BODY = "body"
    const val STATUS_CODE = 400
    const val BASE_PATH = "http://basepath"
  }

  @Test
  fun `test client exception includes http response body`() {
    val client: OkHttpClient = mockk()
    val policy: RetryPolicy<Response> = mockk(relaxed = true)
    val workloadApi = spyk(WorkloadApi(BASE_PATH, client, policy))

    every {
      workloadApi.workloadCancelWithHttpInfo(any())
    } returns ClientError(MESSAGE, BODY, STATUS_CODE, mapOf())

    val exception = assertThrows<ClientException> { workloadApi.workloadCancel(WorkloadCancelRequest("workloadId", "reason", "source")) }
    assertTrue(exception.message!!.contains(MESSAGE))
    assertTrue(exception.message!!.contains(BODY))
    assertTrue(exception.message!!.contains(STATUS_CODE.toString()))
  }

  @Test
  fun `test client exception null http response body`() {
    val client: OkHttpClient = mockk()
    val policy: RetryPolicy<Response> = mockk(relaxed = true)
    val workloadApi = spyk(WorkloadApi(BASE_PATH, client, policy))

    every {
      workloadApi.workloadCancelWithHttpInfo(any())
    } returns ClientError(MESSAGE, null, STATUS_CODE, mapOf())

    val exception = assertThrows<ClientException> { workloadApi.workloadCancel(WorkloadCancelRequest("workloadId", "reason", "source")) }
    assertTrue(exception.message!!.contains(MESSAGE))
    assertTrue(exception.message!!.contains(STATUS_CODE.toString()))
  }
}
