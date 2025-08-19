/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.DbPrune
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class DbPruneWorkflowTest {
  private lateinit var dbPrune: DbPrune
  private lateinit var metricClient: MetricClient
  private lateinit var workflow: DbPruneWorkflow

  @BeforeEach
  fun setup() {
    dbPrune = mockk()
    metricClient = mockk(relaxed = true)
    workflow = DbPruneWorkflow(dbPrune, metricClient)
  }

  @Test
  fun `should skip pruning when no jobs are eligible for deletion`() {
    // Given: no jobs to prune
    every { dbPrune.getEligibleJobCount(any()) } returns 0L

    // When: pruning is executed
    workflow.pruneOldJobs()

    // Then: should check for eligible jobs but not attempt any deletions
    verify(exactly = 1) { dbPrune.getEligibleJobCount(any()) }
    verify(exactly = 0) { dbPrune.pruneJobs(any()) }

    // Should record success metrics
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.DATABASE_PRUNING_JOBS_DELETED,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
      )
    }
  }

  @Test
  fun `should prune jobs in batches until none remain`() {
    // Given: jobs to delete (DbPrune handles all batching internally)
    every { dbPrune.getEligibleJobCount(any()) } returns 1500L // Initial check
    every { dbPrune.pruneJobs(any()) } returns 1500 // Total deleted across all internal batches

    // When: pruning is executed
    workflow.pruneOldJobs()

    // Then: should check initial count and call pruneJobs once (which handles internal batching)
    verify(exactly = 1) { dbPrune.getEligibleJobCount(any()) }
    verify(exactly = 1) { dbPrune.pruneJobs(any()) }

    // Should record success metrics
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.DATABASE_PRUNING_JOBS_DELETED,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
      )
    }

    verify {
      metricClient.gauge(
        metric = OssMetricsRegistry.DATABASE_PRUNING_DURATION,
        value = any<Double>(),
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
      )
    }
  }

  @Test
  fun `should handle when no jobs are actually deleted`() {
    // Given: jobs are eligible but DbPrune returns 0 (no actual deletion - handled internally by DbPrune)
    every { dbPrune.getEligibleJobCount(any()) } returns 100L
    every { dbPrune.pruneJobs(any()) } returns 0 // No jobs actually deleted (DbPrune handles infinite loop prevention internally)

    // When: pruning is executed
    workflow.pruneOldJobs()

    // Then: should call DbPrune once (DbPrune handles edge cases internally)
    verify(exactly = 1) { dbPrune.getEligibleJobCount(any()) }
    verify(exactly = 1) { dbPrune.pruneJobs(any()) }

    // Should still record success metrics
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.DATABASE_PRUNING_JOBS_DELETED,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
      )
    }
  }

  @Test
  fun `should record failure metrics when pruning throws exception`() {
    // Given: pruning will fail
    every { dbPrune.getEligibleJobCount(any()) } returns 100L
    every { dbPrune.pruneJobs(any()) } throws RuntimeException("Database error")

    // When/Then: should propagate exception
    assertThrows<RuntimeException> {
      workflow.pruneOldJobs()
    }

    // Should record failure metrics
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.DATABASE_PRUNING_JOBS_DELETED,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "false")),
      )
    }

    val gaugeSlot = slot<Double>()
    verify {
      metricClient.gauge(
        metric = OssMetricsRegistry.DATABASE_PRUNING_DURATION,
        value = capture(gaugeSlot),
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "false")),
      )
    }

    // Duration should be a positive number even for failures
    assert(gaugeSlot.captured > 0.0)
  }

  @Test
  fun `should record cron job execution metrics`() {
    // Given: normal execution
    every { dbPrune.getEligibleJobCount(any()) } returns 0L

    // When: pruning is executed
    workflow.pruneOldJobs()

    // Then: should record cron job execution
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
        attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "db_prune")),
      )
    }
  }

  @Test
  fun `should handle single batch completion correctly`() {
    // Given: jobs to delete
    every { dbPrune.getEligibleJobCount(any()) } returns 500L // Initial count
    every { dbPrune.pruneJobs(any()) } returns 500 // Delete all jobs (DbPrune handles batching internally)

    // When: pruning is executed
    workflow.pruneOldJobs()

    // Then: should check count and call pruneJobs once
    verify(exactly = 1) { dbPrune.getEligibleJobCount(any()) } // Only initial check
    verify(exactly = 1) { dbPrune.pruneJobs(any()) }

    // Should record success metrics
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.DATABASE_PRUNING_JOBS_DELETED,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
      )
    }
  }
}
