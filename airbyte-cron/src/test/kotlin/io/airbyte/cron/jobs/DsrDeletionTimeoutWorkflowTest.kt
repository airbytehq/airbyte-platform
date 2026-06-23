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
    every { deletionRequestTimeoutService.recoverTimedOutRunningRequests(Duration.ofHours(2)) } returns 3

    workflow.recoverTimedOutRequests()

    verify(exactly = 1) { deletionRequestTimeoutService.recoverTimedOutRunningRequests(Duration.ofHours(2)) }
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
        attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "dsr_deletion_timeout")),
      )
    }
  }

  @Test
  fun `recoverTimedOutRequests does not throw when timeout sweep fails`() {
    every { deletionRequestTimeoutService.recoverTimedOutRunningRequests(Duration.ofHours(2)) } throws RuntimeException("database unavailable")

    workflow.recoverTimedOutRequests()

    verify(exactly = 1) { deletionRequestTimeoutService.recoverTimedOutRunningRequests(Duration.ofHours(2)) }
  }
}
