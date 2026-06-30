/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.domain.services.dsr.DsrDeletionRequestTimeoutService
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

internal class DsrDeletionTimeoutWorkflowTest {
  private lateinit var deletionRequestTimeoutService: DsrDeletionRequestTimeoutService
  private lateinit var metricClient: MetricClient
  private lateinit var workflow: DsrDeletionTimeoutWorkflow

  @BeforeEach
  fun setup() {
    deletionRequestTimeoutService = mockk()
    metricClient = mockk(relaxed = true)
    workflow = DsrDeletionTimeoutWorkflow(deletionRequestTimeoutService, metricClient, Duration.ofHours(2))
  }

  @Test
  fun `recoverTimedOutRequests recovers stale running DSR requests`() {
    every { deletionRequestTimeoutService.recoverTimedOutRunningRequestsWithSummary(Duration.ofHours(2)) } returns
      DsrDeletionRequestTimeoutService.TimeoutRecoveryResult(
        activeTimedOutCount = 2,
        activeFailedCount = 1,
        queuedTimedOutCount = 3,
        queuedRecoveredCount = 2,
      )

    workflow.recoverTimedOutRequests()

    verify(exactly = 1) { deletionRequestTimeoutService.recoverTimedOutRunningRequestsWithSummary(Duration.ofHours(2)) }
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
        attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "dsr_deletion_timeout")),
      )
    }
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.DSR_DELETION_TIMEOUT_SWEEP,
        attributes = arrayOf(MetricAttribute(MetricTags.STATUS, MetricTags.SUCCESS)),
      )
    }
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.DSR_DELETION_TIMEOUT_RECOVERED,
        value = 1,
        attributes = arrayOf(MetricAttribute("execution_state", "active")),
      )
    }
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.DSR_DELETION_TIMEOUT_RECOVERED,
        value = 2,
        attributes = arrayOf(MetricAttribute("execution_state", "queued")),
      )
    }
  }

  @Test
  fun `recoverTimedOutRequests does not throw when timeout sweep fails`() {
    every { deletionRequestTimeoutService.recoverTimedOutRunningRequestsWithSummary(Duration.ofHours(2)) } throws
      RuntimeException("database unavailable")

    workflow.recoverTimedOutRequests()

    verify(exactly = 1) { deletionRequestTimeoutService.recoverTimedOutRunningRequestsWithSummary(Duration.ofHours(2)) }
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.DSR_DELETION_TIMEOUT_SWEEP,
        attributes = arrayOf(MetricAttribute(MetricTags.STATUS, MetricTags.FAILURE)),
      )
    }
  }
}
