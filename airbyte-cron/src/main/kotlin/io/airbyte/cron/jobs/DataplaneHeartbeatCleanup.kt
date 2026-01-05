/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.data.services.DataplaneHealthService
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.scheduling.annotation.Scheduled
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

/**
 * Cron job for cleaning up old dataplane heartbeat logs.
 * Runs every hour to maintain a 24-hour retention period.
 */
@Singleton
class DataplaneHeartbeatCleanup(
  private val dataplaneHealthService: DataplaneHealthService,
  private val metricClient: MetricClient,
) {
  init {
    log.info { "Creating dataplane heartbeat cleanup job" }
  }

  @WithSpan
  @Scheduled(fixedRate = "1h")
  fun cleanupOldHeartbeats() {
    log.info { "Starting dataplane heartbeat cleanup" }

    metricClient.count(
      metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "dataplane_heartbeat_cleanup")),
    )

    try {
      val deletedCount = dataplaneHealthService.cleanupOldHeartbeats()
      log.info { "Successfully cleaned up $deletedCount old heartbeat logs" }
    } catch (e: Exception) {
      log.error(e) { "Failed to cleanup old heartbeat logs" }
    }
  }
}
