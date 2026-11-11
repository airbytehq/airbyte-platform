/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.config.persistence.WorkloadPurge
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.EnableWorkloadPurge
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.WorkloadPurgeRunRequest
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class WorkloadPurgeWorkflowTest {
  private val purge = mockk<WorkloadPurge>()
  private val metrics = mockk<MetricClient>(relaxed = true)
  private val featureFlags = mockk<FeatureFlagClient>()
  private lateinit var workflow: WorkloadPurgeWorkflow

  @BeforeEach
  fun setup() {
    every { featureFlags.boolVariation(EnableWorkloadPurge, Empty) } returns true
    every { featureFlags.stringVariation(WorkloadPurgeRunRequest, Empty) } returns ""
    workflow = WorkloadPurgeWorkflow(purge, metrics, featureFlags)
  }

  @Test
  fun `disabled LaunchDarkly flag skips workload purge`() {
    every { featureFlags.boolVariation(EnableWorkloadPurge, Empty) } returns false

    workflow.pruneRecords()

    verify(exactly = 0) { purge.prune(any()) }
    verify(exactly = 1) { featureFlags.boolVariation(EnableWorkloadPurge, Empty) }
  }

  @Test
  fun `records deleted workload count and duration`() {
    every { purge.prune(any()) } returns 501

    workflow.pruneRecords()

    verify(exactly = 1) { purge.prune(any()) }
    verify {
      metrics.count(OssMetricsRegistry.WORKLOAD_PRUNING_DELETED, 501, *arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")))
      metrics.gauge(OssMetricsRegistry.WORKLOAD_PRUNING_DURATION, any<Double>(), *arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")))
    }
  }

  @Test
  fun `empty purge reports zero deleted`() {
    every { purge.prune(any()) } returns 0

    workflow.pruneRecords()

    verify { metrics.count(OssMetricsRegistry.WORKLOAD_PRUNING_DELETED, 0, *arrayOf(MetricAttribute(MetricTags.SUCCESS, "true"))) }
  }

  @Test
  fun `reports failure and duration when persistence fails`() {
    every { purge.prune(any()) } throws IllegalStateException("database unavailable")

    assertThrows<IllegalStateException> { workflow.pruneRecords() }

    verify {
      metrics.count(OssMetricsRegistry.WORKLOAD_PRUNING_FAILURE)
      metrics.gauge(OssMetricsRegistry.WORKLOAD_PRUNING_DURATION, any<Double>(), *arrayOf(MetricAttribute(MetricTags.SUCCESS, "false")))
    }
  }
}
