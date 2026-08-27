/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.model.generated.CheckDataWorkerCapacityRead
import io.airbyte.api.client.model.generated.CheckDataWorkerCapacityRequest
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
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
  private lateinit var metricClient: MetricClient
  private lateinit var activity: CapacityCheckActivityImpl

  @BeforeEach
  fun setUp() {
    airbyteApiClient = mockk()
    jobsApi = mockk()
    metricClient = mockk(relaxed = true)
    every { airbyteApiClient.jobsApi } returns jobsApi
    activity = CapacityCheckActivityImpl(airbyteApiClient, metricClient)
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

  @Test
  fun `checkCapacity emits committed immediate admission with supplied identity tags`() {
    val jobId = 42L
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val metricAttributes =
      arrayOf(
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
        MetricAttribute(MetricTags.WORKSPACE_ID, "workspace-id"),
        MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
        MetricAttribute(MetricTags.JOB_ID, jobId.toString()),
      )
    every {
      jobsApi.checkDataWorkerCapacity(CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId))
    } returns CheckDataWorkerCapacityRead(true, false)

    activity.checkCapacity(CapacityCheckInput(jobId, connectionId, organizationId, true, metricAttributes, null))

    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.DATA_WORKER_CAPACITY_ADMISSION,
        1L,
        *metricAttributes,
        MetricAttribute(MetricTags.DATA_WORKER_CAPACITY_ADMISSION_RESULT, "committed_immediate"),
      )
    }
  }

  @Test
  fun `checkCapacity emits on demand admission`() {
    val jobId = 43L
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val metricAttributes =
      arrayOf(
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
        MetricAttribute(MetricTags.WORKSPACE_ID, "workspace-id"),
        MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
        MetricAttribute(MetricTags.JOB_ID, jobId.toString()),
      )
    every {
      jobsApi.checkDataWorkerCapacity(CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId))
    } returns CheckDataWorkerCapacityRead(true, true)

    activity.checkCapacity(CapacityCheckInput(jobId, connectionId, organizationId, true, metricAttributes, null))

    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.DATA_WORKER_CAPACITY_ADMISSION,
        1L,
        *metricAttributes,
        MetricAttribute(MetricTags.DATA_WORKER_CAPACITY_ADMISSION_RESULT, "on_demand"),
      )
    }
  }

  @Test
  fun `checkCapacity emits on demand admission when queued capacity is available`() {
    val jobId = 49L
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val metricAttributes =
      arrayOf(
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
        MetricAttribute(MetricTags.WORKSPACE_ID, "workspace-id"),
        MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
        MetricAttribute(MetricTags.JOB_ID, jobId.toString()),
      )
    every {
      jobsApi.checkDataWorkerCapacity(CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId))
    } returns CheckDataWorkerCapacityRead(true, true)

    activity.checkCapacity(CapacityCheckInput(jobId, connectionId, organizationId, true, metricAttributes, 6.5))

    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.DATA_WORKER_CAPACITY_ADMISSION,
        1L,
        *metricAttributes,
        MetricAttribute(MetricTags.DATA_WORKER_CAPACITY_ADMISSION_RESULT, "on_demand"),
      )
    }
  }

  @Test
  fun `checkCapacity emits queued admission and age after capacity is available`() {
    val jobId = 44L
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val metricAttributes =
      arrayOf(
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
        MetricAttribute(MetricTags.WORKSPACE_ID, "workspace-id"),
        MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
        MetricAttribute(MetricTags.JOB_ID, jobId.toString()),
      )
    every {
      jobsApi.checkDataWorkerCapacity(CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId))
    } returns CheckDataWorkerCapacityRead(true, false)

    activity.checkCapacity(CapacityCheckInput(jobId, connectionId, organizationId, true, metricAttributes, 12.5))

    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.DATA_WORKER_CAPACITY_ADMISSION,
        1L,
        *metricAttributes,
        MetricAttribute(MetricTags.DATA_WORKER_CAPACITY_ADMISSION_RESULT, "committed_after_queue"),
      )
    }
    verify(exactly = 1) {
      metricClient.distribution(OssMetricsRegistry.DATA_WORKER_CAPACITY_QUEUE_AGE_SECONDS, 12.5, *metricAttributes)
    }
  }

  @Test
  fun `checkCapacity emits queued age but no admission while capacity remains unavailable`() {
    val jobId = 45L
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val metricAttributes =
      arrayOf(
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
        MetricAttribute(MetricTags.WORKSPACE_ID, "workspace-id"),
        MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
        MetricAttribute(MetricTags.JOB_ID, jobId.toString()),
      )
    every {
      jobsApi.checkDataWorkerCapacity(CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId))
    } returns CheckDataWorkerCapacityRead(false, false)

    activity.checkCapacity(CapacityCheckInput(jobId, connectionId, organizationId, true, metricAttributes, 8.0))

    verify(exactly = 0) { metricClient.count(OssMetricsRegistry.DATA_WORKER_CAPACITY_ADMISSION, any(), *anyVararg()) }
    verify(exactly = 1) {
      metricClient.distribution(OssMetricsRegistry.DATA_WORKER_CAPACITY_QUEUE_AGE_SECONDS, 8.0, *metricAttributes)
    }
  }

  @Test
  fun `checkCapacity emits no metric when enforcement is disabled or the capacity api fails`() {
    activity.checkCapacity(CapacityCheckInput(46L, UUID.randomUUID(), UUID.randomUUID(), false, emptyArray(), 1.0))
    verify(exactly = 0) { metricClient.count(any(), any(), *anyVararg()) }
    verify(exactly = 0) { metricClient.distribution(any(), any(), *anyVararg()) }

    val jobId = 47L
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    every {
      jobsApi.checkDataWorkerCapacity(CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId))
    } throws java.io.IOException("capacity unavailable")

    org.junit.jupiter.api.Assertions.assertThrows(io.airbyte.commons.temporal.exception.RetryableException::class.java) {
      activity.checkCapacity(CapacityCheckInput(jobId, connectionId, organizationId, true, emptyArray(), 1.0))
    }
    verify(exactly = 0) { metricClient.count(any(), any(), *anyVararg()) }
    verify(exactly = 0) { metricClient.distribution(any(), any(), *anyVararg()) }
  }

  @Test
  fun `checkCapacity emits no lifecycle metrics for empty or partial identity tags`() {
    val jobId = 48L
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    every {
      jobsApi.checkDataWorkerCapacity(CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId))
    } returns CheckDataWorkerCapacityRead(true, false)

    activity.checkCapacity(CapacityCheckInput(jobId, connectionId, organizationId, true, emptyArray(), 1.0))
    activity.checkCapacity(
      CapacityCheckInput(
        jobId,
        connectionId,
        organizationId,
        true,
        arrayOf(
          MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
          MetricAttribute(MetricTags.WORKSPACE_ID, "workspace-id"),
          MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
        ),
        1.0,
      ),
    )

    verify(exactly = 0) { metricClient.count(any(), any(), *anyVararg()) }
    verify(exactly = 0) { metricClient.distribution(any(), any(), *anyVararg()) }
  }

  @Test
  fun `checkCapacity isolates metric failures from the capacity response`() {
    val jobId = 48L
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    every {
      jobsApi.checkDataWorkerCapacity(CheckDataWorkerCapacityRequest(jobId, connectionId, organizationId))
    } returns CheckDataWorkerCapacityRead(true, false)
    every { metricClient.count(any(), any(), *anyVararg()) } throws RuntimeException("metrics unavailable")

    val metricAttributes =
      arrayOf(
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
        MetricAttribute(MetricTags.WORKSPACE_ID, "workspace-id"),
        MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
        MetricAttribute(MetricTags.JOB_ID, jobId.toString()),
      )

    val output = activity.checkCapacity(CapacityCheckInput(jobId, connectionId, organizationId, true, metricAttributes, null))

    assertTrue(output.capacityAvailable)
  }
}
