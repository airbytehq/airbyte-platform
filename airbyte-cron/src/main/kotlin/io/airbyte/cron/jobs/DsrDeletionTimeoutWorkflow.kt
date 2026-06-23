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
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "dsr_deletion_timeout")),
    )

    try {
      val recoveredCount = deletionRequestTimeoutService.recoverTimedOutRunningRequests(executionTimeout)
      log.info { "Recovered $recoveredCount timed-out DSR deletion requests" }
    } catch (e: Exception) {
      log.error(e) { "Failed to recover timed-out DSR deletion requests" }
    }
  }
}
