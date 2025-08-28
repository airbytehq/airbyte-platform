/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import datadog.trace.api.Trace
import io.airbyte.cron.SCHEDULED_TRACE_OPERATION_NAME
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.DbPrune
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.time.ZoneOffset

private val log = KotlinLogging.logger {}

/**
 * Workflow that runs daily at 10pm to prune old records from the database.
 * Deletes jobs older than 6 months that are not the last job for their scope,
 * along with all related records via foreign key relationships.
 * Also deletes connection timeline events older than 18 months.
 */
@Singleton
@Requires(property = "airbyte.cron.db-prune.enabled", value = "true")
class DbPruneWorkflow(
  @Named("dbPrune") private val dbPrune: DbPrune,
  private val metricClient: MetricClient,
) {
  init {
    log.info { "Creating database pruning workflow" }
  }

  /*
   * Runs daily at 10pm PST (2am UTC) to reduce the chance of a deploy stopping it.
   * Wanted a consistent time so we can see patterns if any arise.
   */
  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(cron = "0 0 22 * * *", zoneId = "America/Los_Angeles")
  @Synchronized
  fun pruneRecords() {
    val startTime = OffsetDateTime.now(ZoneOffset.UTC)
    log.info { "Starting database pruning workflow at $startTime" }

    metricClient.count(
      metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "db_prune")),
    )

    try {
      // Use the same timestamp for both count and pruning to ensure consistency
      val now = OffsetDateTime.now(ZoneOffset.UTC)

      log.info { "Pruning jobs older than 6 months" }
      val jobsDeleted = dbPrune.pruneJobs(now)

      log.info { "Pruning jobs older than 18 months" }
      val eventsDeleted = dbPrune.pruneEvents(now)

      val endTime = OffsetDateTime.now(ZoneOffset.UTC)
      val duration = java.time.Duration.between(startTime, endTime)

      log.info {
        "Database pruning completed successfully. " +
          "Total jobs deleted: $jobsDeleted, " +
          "Total events deleted: $eventsDeleted, " +
          "Duration: ${duration.toMinutes()} minutes"
      }

      // Record success metrics
      metricClient.count(
        metric = OssMetricsRegistry.DATABASE_PRUNING_JOBS_DELETED,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
      )

      metricClient.gauge(
        metric = OssMetricsRegistry.DATABASE_PRUNING_DURATION,
        value = duration.toMillis().toDouble(),
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
      )
    } catch (e: Exception) {
      val endTime = OffsetDateTime.now(ZoneOffset.UTC)
      val duration = java.time.Duration.between(startTime, endTime)

      log.error(e) { "Database pruning failed after ${duration.toMinutes()} minutes" }

      // Record failure metrics
      metricClient.count(
        metric = OssMetricsRegistry.DATABASE_PRUNING_JOBS_DELETED,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "false")),
      )

      metricClient.gauge(
        metric = OssMetricsRegistry.DATABASE_PRUNING_DURATION,
        value = duration.toMillis().toDouble(),
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "false")),
      )

      throw e
    }
  }
}
