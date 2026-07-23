/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.model.generated.CheckDataWorkerCapacityRead
import io.airbyte.api.client.model.generated.CheckDataWorkerCapacityRequest
import io.airbyte.workers.temporal.scheduling.activities.CapacityCheckActivity.CapacityCheckInput
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class CapacityCheckActivityImplTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var jobsApi: JobsApi
  private lateinit var activity: CapacityCheckActivityImpl

  @BeforeEach
  fun setUp() {
    airbyteApiClient = mockk()
    jobsApi = mockk()
    every { airbyteApiClient.jobsApi } returns jobsApi
    activity = CapacityCheckActivityImpl(airbyteApiClient)
  }

  @Test
  fun `checkCapacity skips api call when enforcement disabled`() {
    val output = activity.checkCapacity(CapacityCheckInput(42L, UUID.randomUUID(), UUID.randomUUID(), false))

    assertTrue(output.capacityAvailable)
    assertFalse(output.useOnDemandCapacity)
    assertFalse(output.enforcementEnabled)
    verify(exactly = 0) { jobsApi.checkDataWorkerCapacity(any<CheckDataWorkerCapacityRequest>()) }
  }

  @Test
  fun `checkCapacity calls jobs api when enforcement enabled`() {
    val jobId = 42L
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    every {
      jobsApi.checkDataWorkerCapacity(
        CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId),
      )
    } returns CheckDataWorkerCapacityRead(false, false)

    val output = activity.checkCapacity(CapacityCheckInput(jobId, connectionId, organizationId, true))

    assertFalse(output.capacityAvailable)
    assertFalse(output.useOnDemandCapacity)
    assertTrue(output.enforcementEnabled)
    verify(exactly = 1) {
      jobsApi.checkDataWorkerCapacity(
        CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId),
      )
    }
  }

  @Test
  fun `checkCapacity maps on demand response`() {
    val jobId = 99L
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    every {
      jobsApi.checkDataWorkerCapacity(
        CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId),
      )
    } returns CheckDataWorkerCapacityRead(true, true)

    val output = activity.checkCapacity(CapacityCheckInput(jobId, connectionId, organizationId, true))

    assertEquals(true, output.capacityAvailable)
    assertEquals(true, output.useOnDemandCapacity)
    assertEquals(true, output.enforcementEnabled)
  }
}
