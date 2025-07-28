/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class WorkloadApiClientTest {
  @Test
  fun `test that the workload API client creates the underlying Workload API object with the provided configuration`() {
    val operations = mockk<io.airbyte.workload.api.WorkloadApiClient>()
    val workloadApiClient = WorkloadApiClient(operations)
    assertEquals(operations, workloadApiClient.workloadApi)
  }
}
