/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import dev.failsafe.RetryPolicy
import io.mockk.mockk
import okhttp3.OkHttpClient
import okhttp3.Response
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

class WorkloadApiClientTest {
  @Test
  fun `test that the workload API client creates the underlying Workload API object with the provided configuration`() {
    val basePath = "base-path"
    val client: OkHttpClient = mockk()
    val policy: RetryPolicy<Response> = mockk()

    val workloadApiClient = WorkloadApiClient(basePath, policy, client)
    assertNotNull(workloadApiClient.workloadApi)
    assertEquals(client, workloadApiClient.workloadApi.client)
    assertEquals(policy, workloadApiClient.workloadApi.policy)
    assertEquals(basePath, workloadApiClient.workloadApi.baseUrl)
  }
}
