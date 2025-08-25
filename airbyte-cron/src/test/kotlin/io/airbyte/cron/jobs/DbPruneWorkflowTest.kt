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
  fun `should handle when no jobs are eligible for deletion`() {
    // Given: no jobs to prune (pruneJobs and pruneEvents will return 0)
    every { dbPrune.pruneJobs(any()) } returns 0
    every { dbPrune.pruneEvents(any()) } returns 0

    // When: pruning is executed
    workflow.pruneRecords()

    // Then: should call both prune methods (they handle empty case internally)
    verify(exactly = 1) { dbPrune.pruneJobs(any()) }
    verify(exactly = 1) { dbPrune.pruneEvents(any()) }

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
    // Given: jobs and events to delete (DbPrune handles all batching internally)
    every { dbPrune.pruneJobs(any()) } returns 1500 // Total deleted across all internal batches
    every { dbPrune.pruneEvents(any()) } returns 200 // Events deleted

    // When: pruning is executed
    workflow.pruneRecords()

    // Then: should call prune methods once each (which handle internal batching)
    verify(exactly = 1) { dbPrune.pruneJobs(any()) }
    verify(exactly = 1) { dbPrune.pruneEvents(any()) }

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
    // Given: DbPrune returns 0 (no actual deletion - handled internally by DbPrune)
    every { dbPrune.pruneJobs(any()) } returns 0 // No jobs actually deleted (DbPrune handles all logic internally)
    every { dbPrune.pruneEvents(any()) } returns 0 // No events actually deleted

    // When: pruning is executed
    workflow.pruneRecords()

    // Then: should call DbPrune once for each type (DbPrune handles edge cases internally)
    verify(exactly = 1) { dbPrune.pruneJobs(any()) }
    verify(exactly = 1) { dbPrune.pruneEvents(any()) }

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
    every { dbPrune.pruneJobs(any()) } throws RuntimeException("Database error")

    // When/Then: should propagate exception
    assertThrows<RuntimeException> {
      workflow.pruneRecords()
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
    every { dbPrune.pruneJobs(any()) } returns 0
    every { dbPrune.pruneEvents(any()) } returns 0

    // When: pruning is executed
    workflow.pruneRecords()

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
    // Given: jobs and events to delete
    every { dbPrune.pruneJobs(any()) } returns 500 // Delete all jobs (DbPrune handles batching internally)
    every { dbPrune.pruneEvents(any()) } returns 100 // Delete all events

    // When: pruning is executed
    workflow.pruneRecords()

    // Then: should call prune methods once each
    verify(exactly = 1) { dbPrune.pruneJobs(any()) }
    verify(exactly = 1) { dbPrune.pruneEvents(any()) }

    // Should record success metrics
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.DATABASE_PRUNING_JOBS_DELETED,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
      )
    }
  }
}
