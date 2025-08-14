/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.api.model.generated.AttemptRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.config.JobConfig
import io.airbyte.data.repositories.entities.ObsJobsStats
import io.airbyte.data.repositories.entities.ObsStreamStats
import io.airbyte.data.repositories.entities.ObsStreamStatsId
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ObsStatsService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.server.services.JobObservabilityService.JobMetrics.Companion.toJobMetrics
import io.airbyte.server.services.JobObservabilityService.StreamMetrics.Companion.toStreamMetrics
import io.airbyte.statistics.Outliers
import io.airbyte.statistics.Scores
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.math.absoluteValue

@Singleton
class JobObservabilityService(
  private val actorDefinitionService: ActorDefinitionService,
  private val connectionService: ConnectionService,
  private val jobHistoryHandler: JobHistoryHandler,
  private val jobPersistence: JobPersistence,
  private val obsStatsService: ObsStatsService,
  private val workspaceService: WorkspaceService,
  private val metricClient: MetricClient,
) {
  private val logger = KotlinLogging.logger {}
  private val outliers = Outliers()

  private data class JobDetails(
    val jobId: Long,
    val connectionId: UUID,
    val workspaceId: UUID,
    val organizationId: UUID,
    val sourceId: UUID,
    val sourceDefinitionId: UUID,
    val sourceImageTag: String,
    val destinationId: UUID,
    val destinationDefinitionId: UUID,
    val destinationImageTag: String,
    val job: JobRead,
    val attempts: List<AttemptRead>,
  )

  private data class OutlierOutcome(
    val jobStats: ObsJobsStats,
    val streamsStats: List<ObsStreamStats>,
    val isOutlier: Boolean,
    val jobScore: Scores,
    val streamScores: Map<Pair<String?, String>, Scores>,
  )

  /**
   * The Job metrics to consider for outlier detection.
   */
  private data class JobMetrics(
    val attemptCount: Int,
    val durationSeconds: Long,
  ) {
    companion object {
      fun ObsJobsStats.toJobMetrics(): JobMetrics =
        JobMetrics(
          attemptCount = attemptCount,
          durationSeconds = durationSeconds,
        )

      fun isOutlier(scores: Scores): Boolean {
        scores.scores["durationSeconds"]?.let { if (it > 2.5) return true }
        scores.scores["attemptCount"]?.let { if (it > 2.5) return true }
        return false
      }
    }
  }

  /**
   * The Stream metrics to consider for outlier detection.
   */
  private data class StreamMetrics(
    var bytesLoaded: Long,
    var recordsLoaded: Long,
    var recordsRejected: Long,
  ) {
    companion object {
      fun ObsStreamStats.toStreamMetrics(): StreamMetrics =
        StreamMetrics(
          bytesLoaded = bytesLoaded,
          recordsLoaded = recordsLoaded,
          recordsRejected = recordsRejected,
        )

      fun isOutlier(scores: Scores): Boolean {
        scores.scores["bytesLoaded"]?.let { if (it.absoluteValue > 2.5) return true }
        scores.scores["recordsLoaded"]?.let { if (it.absoluteValue > 2.5) return true }
        scores.scores["recordsRejected"]?.let { if (it.absoluteValue > 2.5) return true }
        return false
      }
    }
  }

  fun finalizeStats(jobId: Long) {
    val details = fetchJobDetails(jobId)

    val obsJobStats =
      ObsJobsStats(
        jobId = jobId,
        connectionId = details.connectionId,
        workspaceId = details.workspaceId,
        organizationId = details.organizationId,
        sourceId = details.sourceId,
        sourceDefinitionId = details.sourceDefinitionId,
        sourceImageTag = details.sourceImageTag,
        destinationId = details.destinationId,
        destinationDefinitionId = details.destinationDefinitionId,
        destinationImageTag = details.destinationImageTag,
        createdAt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(details.job.createdAt), ZoneOffset.UTC),
        jobType = details.job.configType.toString(),
        status = details.job.status.toString(),
        attemptCount = details.attempts.size,
        durationSeconds =
          (
            details.attempts
              .lastOrNull()
              ?.endedAt ?: details.job.updatedAt
          ) - details.job.createdAt,
      )
    obsStatsService.saveJobsStats(obsJobStats)

    val streamStatsList =
      details.job.streamAggregatedStats.map {
        ObsStreamStats(
          id =
            ObsStreamStatsId(
              jobId = jobId,
              streamNamespace = it.streamNamespace,
              streamName = it.streamName,
            ),
          bytesLoaded = it.bytesCommitted,
          recordsLoaded = it.recordsCommitted,
          recordsRejected = it.recordsRejected,
          wasBackfilled = it.wasBackfilled,
          wasResumed = it.wasResumed,
        )
      }
    obsStatsService.saveStreamStats(streamStatsList)
  }

  fun evaluateOutlier(jobId: Long) {
    // We only consider syncs for outlier detection.
    // When passing the job type restriction, it will automatically exclude the current job if it isn't as sync as well.
    val (job, jobHistory) =
      obsStatsService.findJobStatsAndPrevious(jobId, DEFAULT_INTERVAL, MAX_JOBS_COUNT, JOB_TYPES_TO_CONSIDER).partition {
        it.jobId ==
          jobId
      }
    if (job.isEmpty()) {
      logger.info { "Unable to find jobId:$jobId for outlier detection" }
      return
    }

    val jobScore = outliers.evaluate(jobHistory.map { it.toJobMetrics() }, job.first().toJobMetrics())

    val allStreamStats =
      obsStatsService
        .findJobStreamStatsAndPrevious(jobId, DEFAULT_INTERVAL, MAX_JOBS_COUNT, JOB_TYPES_TO_CONSIDER)
    val scoresByStream =
      allStreamStats
        .groupBy { it.id.streamNamespace to it.id.streamName }
        .mapValues { streamStats ->
          val (currentList, history) = streamStats.value.partition { it.id.jobId == jobId }
          val current =
            currentList.firstOrNull()
              ?: defaultEmptyStatsForStream(jobId = jobId, streamNamespace = streamStats.key.first, streamName = streamStats.key.second)
          val streamScore = outliers.evaluate(history.map { it.toStreamMetrics() }, current.toStreamMetrics())
          return@mapValues streamScore
        }

    val outlierOutcome =
      OutlierOutcome(
        jobStats = job.first(),
        streamsStats = allStreamStats.filter { it.id.jobId == jobId },
        isOutlier = JobMetrics.isOutlier(jobScore) || scoresByStream.values.any { StreamMetrics.isOutlier(it) },
        jobScore = jobScore,
        streamScores = scoresByStream,
      )

    if (outlierOutcome.isOutlier) {
      logger.info {
        "jobId:$jobId has been marked as an outlier. (details:${outlierOutcome.jobStats}, ${outlierOutcome.jobScore}, ${outlierOutcome.streamScores})"
      }
    }
    reportOutlierMetric(outlierOutcome)
  }

  private fun reportOutlierMetric(outlierOutcome: OutlierOutcome) {
    metricClient.count(
      OssMetricsRegistry.DATA_OBS_OUTLIER_CHECK,
      1,
      MetricAttribute(MetricTags.STATUS_TAG, outlierOutcome.isOutlier.toString()),
      MetricAttribute(MetricTags.CONNECTION_ID, outlierOutcome.jobStats.connectionId.toString()),
      MetricAttribute(MetricTags.WORKSPACE_ID, outlierOutcome.jobStats.workspaceId.toString()),
      MetricAttribute(MetricTags.SOURCE_DEFINITION_ID, outlierOutcome.jobStats.sourceDefinitionId.toString()),
      MetricAttribute(MetricTags.DESTINATION_DEFINITION_ID, outlierOutcome.jobStats.destinationDefinitionId.toString()),
    )
  }

  private fun defaultEmptyStatsForStream(
    jobId: Long,
    streamNamespace: String?,
    streamName: String,
  ): ObsStreamStats =
    ObsStreamStats(
      id =
        ObsStreamStatsId(
          jobId = jobId,
          streamNamespace = streamNamespace,
          streamName = streamName,
        ),
      bytesLoaded = 0,
      recordsLoaded = 0,
      recordsRejected = 0,
      wasBackfilled = false,
      wasResumed = false,
    )

  private fun fetchJobDetails(jobId: Long): JobDetails {
    // We look up Job from this because it has the stats aggregation logic we use for billing.
    val jobRead = jobHistoryHandler.getJobInfoWithoutLogs(jobId)
    val connectionId = UUID.fromString(jobRead.job.configId)

    // Because that's how we get sourceId/destId currently
    val standardSync = connectionService.getStandardSync(connectionId)

    // We look up jobs from this because we need the actorVersion to get details about the actual connectors we ran
    val job = jobPersistence.getJob(jobId)
    val destVersion = actorDefinitionService.getActorDefinitionVersion(job.config.getDestinationVersionId())
    val sourceVersion = job.config.getSourceVersionId()?.let { actorDefinitionService.getActorDefinitionVersion(it) }

    val workspaceId = job.config.getWorkspaceId()
    val orgId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).orElse(UUID_ZERO)

    return JobDetails(
      jobId = jobId,
      connectionId = connectionId,
      workspaceId = workspaceId,
      organizationId = orgId,
      sourceId = standardSync.sourceId,
      sourceDefinitionId = sourceVersion?.actorDefinitionId ?: UUID_ZERO,
      sourceImageTag = sourceVersion?.dockerImageTag ?: "",
      destinationId = standardSync.destinationId,
      destinationDefinitionId = destVersion.actorDefinitionId,
      destinationImageTag = destVersion.dockerImageTag,
      job = jobRead.job,
      attempts = jobRead.attempts.map { it.attempt },
    )
  }

  private fun JobConfig.getDestinationVersionId(): UUID =
    sync?.destinationDefinitionVersionId ?: resetConnection?.destinationDefinitionVersionId ?: UUID_ZERO

  private fun JobConfig.getSourceVersionId(): UUID? = sync?.sourceDefinitionVersionId

  private fun JobConfig.getWorkspaceId(): UUID = sync?.workspaceId ?: resetConnection?.workspaceId ?: UUID_ZERO

  companion object {
    // Define the limits for the history to consider
    // The idea is to get at most 28Days of data, however to avoid hourly and more frequent jobs to generate too much data
    // cap it at 7 days of hourly syncs.
    private val DEFAULT_INTERVAL = Duration.ofDays(28)
    private val MAX_JOBS_COUNT = 7 * 24
    private val JOB_TYPES_TO_CONSIDER = listOf(JobConfigType.sync)

    private val UUID_ZERO: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }
}
