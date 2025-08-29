/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.commons.json.Jsons
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.statistics.OutlierEvaluation
import io.github.oshai.kotlinlogging.KotlinLogging
import io.sentry.IHub
import io.sentry.SentryEvent
import io.sentry.protocol.Contexts
import io.sentry.protocol.Message
import io.sentry.protocol.User

private val logger = KotlinLogging.logger {}

class JobObservabilityReportingService(
  private val sentryHub: IHub?,
) {
  fun reportJobOutlierStatus(event: OutlierOutcome) {
    if (sentryHub == null) {
      return
    }

    try {
      val sentryEvent = buildEvent(event)
      sentryHub.captureEvent(sentryEvent)
    } catch (e: Exception) {
      logger.error(e) { "Failed to report event to sentry: $event" }
    }
  }

  private fun buildEvent(event: OutlierOutcome): SentryEvent =
    SentryEvent().apply {
      user = buildSentryUser(event)
      tags = buildSentryTags(event.job)
      message =
        Message().apply {
          message = "Outlier detected workspace:${event.job.workspaceId} connection:${event.job.connectionId}"
        }
      this
        .contexts
        .enrichSentryContext(event)
    }

  private fun buildSentryTags(tags: JobInfo): Map<String, String> {
    val workspaceUrl = "https://cloud.airbyte.com/workspaces/${tags.workspaceId}"
    val connectionUrl = "$workspaceUrl/connections/${tags.connectionId}"
    val jobUrl = "$connectionUrl/job-history#${tags.jobId}::0"
    return mapOf(
      MetricTags.CONNECTION_ID to tags.connectionId.toString(),
      MetricTags.WORKSPACE_ID to tags.workspaceId.toString(),
      MetricTags.ORGANIZATION_ID to tags.organizationId.toString(),
      MetricTags.JOB_ID to tags.jobId.toString(),
      MetricTags.SOURCE_ID to tags.sourceId.toString(),
      MetricTags.SOURCE_DEFINITION_ID to tags.sourceDefinitionId.toString(),
      MetricTags.SOURCE_IMAGE to tags.sourceImageName,
      MetricTags.SOURCE_IMAGE_TAG to tags.sourceImageTag,
      MetricTags.DESTINATION_ID to tags.destinationId.toString(),
      MetricTags.DESTINATION_DEFINITION_ID to tags.destinationDefinitionId.toString(),
      MetricTags.DESTINATION_IMAGE to tags.destinationImageName,
      MetricTags.DESTINATION_IMAGE_TAG to tags.destinationImageTag,
      // Convenience URLs
      "connection_url" to connectionUrl,
      "job_url" to jobUrl,
      "workspace_url" to workspaceUrl,
    )
  }

  /**
   * Using Workspace as User in sentry for automatic workspace grouping.
   */
  private fun buildSentryUser(event: OutlierOutcome): User =
    User().apply {
      id = event.job.workspaceId.toString()
    }

  /**
   *
   */
  private fun Contexts.enrichSentryContext(event: OutlierOutcome) {
    this["Job"] = buildJobInfo(event.job)
    this["Outlier Summary"] = buildOutlierSummary(event)
    event.streams.forEach {
      if (it.isOutlier) {
        this["Stream ${it.namespace}:${it.name}"] = buildStreamSummary(it)
      }
    }
  }

  private fun List<OutlierEvaluation>.addToContextMap(map: MutableMap<String, Any>) {
    forEach { it.addToContextMap(map) }
  }

  private fun OutlierEvaluation.addToContextMap(map: MutableMap<String, Any>) {
    map["_score_$name"] =
      Jsons.serialize(
        mapOf(
          "value" to value,
          "threshold" to threshold,
          "is_outlier" to isOutlier,
          "scores" to scores.toString(),
        ),
      )
  }

  private fun buildJobInfo(job: JobInfo): Map<String, Any> {
    val context =
      mutableMapOf<String, Any>(
        "attempt_count" to job.metrics.attemptCount,
        "duration_seconds" to job.metrics.durationSeconds,
        "job_type" to job.jobType,
        "job_outcome" to job.jobOutcome,
      )
    job.evaluations.addToContextMap(context)
    return context
  }

  private fun buildOutlierSummary(event: OutlierOutcome): Map<String, Any> =
    mapOf(
      "is_outlier" to event.isOutlier,
      "is_freshness_outlier" to event.job.isOutlier,
      "is_correctness_outlier" to (event.numberOfOutlierStreams > 0),
      "number_of_historical_job_considered" to event.numberOfHistoricalJobsConsidered,
      "number_of_outlier_streams" to event.numberOfOutlierStreams,
      "number_of_streams" to event.streams.size,
    )

  private fun buildStreamSummary(stream: StreamInfo): Map<String, Any> {
    val context =
      mutableMapOf<String, Any>(
        "bytes_loaded" to stream.metrics.bytesLoaded,
        "records_loaded" to stream.metrics.recordsLoaded,
        "records_rejected" to stream.metrics.recordsRejected,
        "was_backfilled" to stream.wasBackfilled,
        "was_resumed" to stream.wasResumed,
      )
    stream.evaluations.addToContextMap(context)
    return context
  }
}
