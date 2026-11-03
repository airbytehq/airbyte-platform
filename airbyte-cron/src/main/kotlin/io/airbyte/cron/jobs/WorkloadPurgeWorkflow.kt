/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.config.persistence.WorkloadPurge
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.EnableWorkloadPurge
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.scheduling.annotation.Scheduled
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.time.ZoneOffset

private val log = KotlinLogging.logger {}

@Singleton
class WorkloadPurgeWorkflow(
  private val workloadPurge: WorkloadPurge,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
) {
  @WithSpan
  @Scheduled(cron = "0 0 20 * * *", zoneId = "America/Los_Angeles")
  @Synchronized
  fun pruneRecords() {
    if (!featureFlagClient.boolVariation(EnableWorkloadPurge, Empty)) {
      log.info { "Workload pruning is disabled by feature flag. Skipping pruning workflow." }
      return
    }

    val started = System.nanoTime()
    var success = false
    metricClient.count(
      metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "workload_purge")),
    )
    try {
      val deleted = workloadPurge.prune(OffsetDateTime.now(ZoneOffset.UTC))
      success = true
      log.info { "Workload pruning completed: $deleted workloads deleted" }
      metricClient.count(
        metric = OssMetricsRegistry.WORKLOAD_PRUNING_DELETED,
        value = deleted.toLong(),
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
      )
    } catch (e: Exception) {
      log.error(e) { "Workload pruning failed; committed batches remain deleted" }
      metricClient.count(OssMetricsRegistry.WORKLOAD_PRUNING_FAILURE)
      throw e
    } finally {
      val durationMs = (System.nanoTime() - started) / 1_000_000.0
      log.info { "Workload pruning finished in $durationMs ms; success=$success" }
      metricClient.gauge(
        metric = OssMetricsRegistry.WORKLOAD_PRUNING_DURATION,
        value = durationMs,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, success.toString())),
      )
    }
  }
}
