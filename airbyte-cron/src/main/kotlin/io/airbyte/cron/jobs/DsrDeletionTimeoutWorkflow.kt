/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.domain.services.dsr.DsrDeletionRequestTimeoutService
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.scheduling.annotation.Scheduled
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Singleton
import java.time.Duration

private val log = KotlinLogging.logger {}

private const val DSR_DELETION_TIMEOUT_CRON_TYPE = "dsr_deletion_timeout"
private const val DSR_EXECUTION_STATE_TAG = "execution_state"
private const val DSR_EXECUTION_STATE_ACTIVE = "active"
private const val DSR_EXECUTION_STATE_QUEUED = "queued"

/**
 * Recovers DSR deletion requests when an in-process async executor is lost and the row remains
 * RUNNING past the configured timeout. Active requests are failed; accepted-but-unclaimed requests
 * are returned to PREVIEWED so Support can retry execution.
 */
@Singleton
@Requires(property = "airbyte.cron.dsr-deletion-timeout.enabled", value = "true", defaultValue = "true")
class DsrDeletionTimeoutWorkflow(
  private val deletionRequestTimeoutService: DsrDeletionRequestTimeoutService,
  private val metricClient: MetricClient,
  @param:Value("\${airbyte.dsr-deletion.execution-timeout:PT2H}") private val executionTimeout: Duration,
) {
  init {
    log.info { "Creating DSR deletion timeout workflow" }
  }

  @WithSpan
  @Scheduled(fixedRate = "\${airbyte.cron.dsr-deletion-timeout.fixed-rate:1h}")
  @Synchronized
  fun recoverTimedOutRequests() {
    log.info { "Starting DSR deletion timeout recovery sweep for RUNNING requests older than $executionTimeout" }

    metricClient.count(
      metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, DSR_DELETION_TIMEOUT_CRON_TYPE)),
    )

    try {
      val result = deletionRequestTimeoutService.recoverTimedOutRunningRequestsWithSummary(executionTimeout)
      metricClient.count(
        metric = OssMetricsRegistry.DSR_DELETION_TIMEOUT_SWEEP,
        attributes = arrayOf(MetricAttribute(MetricTags.STATUS, MetricTags.SUCCESS)),
      )
      if (result.activeFailedCount > 0) {
        metricClient.count(
          metric = OssMetricsRegistry.DSR_DELETION_TIMEOUT_RECOVERED,
          value = result.activeFailedCount.toLong(),
          attributes = arrayOf(MetricAttribute(DSR_EXECUTION_STATE_TAG, DSR_EXECUTION_STATE_ACTIVE)),
        )
      }
      if (result.queuedRecoveredCount > 0) {
        metricClient.count(
          metric = OssMetricsRegistry.DSR_DELETION_TIMEOUT_RECOVERED,
          value = result.queuedRecoveredCount.toLong(),
          attributes = arrayOf(MetricAttribute(DSR_EXECUTION_STATE_TAG, DSR_EXECUTION_STATE_QUEUED)),
        )
      }
      log.info {
        "Recovered ${result.recoveredCount} timed-out DSR deletion requests " +
          "(activeTimedOut=${result.activeTimedOutCount}, activeFailed=${result.activeFailedCount}, " +
          "queuedTimedOut=${result.queuedTimedOutCount}, queuedRecovered=${result.queuedRecoveredCount})"
      }
    } catch (e: Exception) {
      metricClient.count(
        metric = OssMetricsRegistry.DSR_DELETION_TIMEOUT_SWEEP,
        attributes = arrayOf(MetricAttribute(MetricTags.STATUS, MetricTags.FAILURE)),
      )
      log.error(e) { "Failed to recover timed-out DSR deletion requests" }
    }
  }
}
