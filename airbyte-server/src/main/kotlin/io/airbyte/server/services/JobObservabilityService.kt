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
import kotlin.math.sqrt

data class OutlierOutcome(
  val job: JobInfo,
  val streams: List<StreamInfo>,
  val isOutlier: Boolean,
  val numberOfHistoricalJobsConsidered: Int,
  val numberOfOutlierStreams: Int,
)

data class JobInfo(
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
  val jobType: String,
  val jobOutcome: String,
  val metrics: JobMetrics,
  val evaluations: List<OutlierEvaluation>,
  val isOutlier: Boolean,
)

/**
 * The Job metrics to consider for outlier detection.
 */
data class JobMetrics(
  val attemptCount: Int,
  val durationSeconds: Long,
)

data class StreamInfo(
  val namespace: String?,
  val name: String,
  val wasBackfilled: Boolean,
  val wasResumed: Boolean,
  val metrics: StreamMetrics,
  val evaluations: List<OutlierEvaluation>,
  val isOutlier: Boolean,
)

/**
 * The Stream metrics to consider for outlier detection.
 */
data class StreamMetrics(
  var bytesLoaded: Long,
  var recordsLoaded: Long,
  var recordsRejected: Long,
)

data class OutlierEvaluation(
  val dimension: String,
  val score: Double,
  val threshold: Double,
  val isOutlier: Boolean,
)

@Singleton
class JobObservabilityService(
  private val actorDefinitionService: ActorDefinitionService,
  private val connectionService: ConnectionService,
  private val jobHistoryHandler: JobHistoryHandler,
  private val jobPersistence: JobPersistence,
  private val obsStatsService: ObsStatsService,
  private val workspaceService: WorkspaceService,
  private val metricClient: MetricClient,
  private val jobObservabilityReportingService: JobObservabilityReportingService?,
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

    val jobInfo = evaluateJob(job.first(), jobHistory)

    val allStreamStats =
      obsStatsService
        .findJobStreamStatsAndPrevious(jobId, DEFAULT_INTERVAL, MAX_JOBS_COUNT, JOB_TYPES_TO_CONSIDER)
    val streams =
      allStreamStats
        .groupBy { it.id.streamNamespace to it.id.streamName }
        .map { streamStats ->
          val (currentList, history) = streamStats.value.partition { it.id.jobId == jobId }
          val current =
            currentList.firstOrNull()
              ?: defaultEmptyStatsForStream(jobId = jobId, streamNamespace = streamStats.key.first, streamName = streamStats.key.second)
          evaluateStream(
            namespace = streamStats.key.first,
            name = streamStats.key.second,
            currentStream = current,
            streamHistory = history,
          )
        }

    val numberOfOutlierStreams = streams.count { it.isOutlier }
    val outlierOutcome =
      OutlierOutcome(
        job = jobInfo,
        isOutlier = jobInfo.isOutlier || numberOfOutlierStreams > 0,
        numberOfHistoricalJobsConsidered = jobHistory.size,
        numberOfOutlierStreams = numberOfOutlierStreams,
        streams = streams,
      )

    if (outlierOutcome.isOutlier) {
      jobObservabilityReportingService?.reportJobOutlierStatus(outlierOutcome)
      logger.info {
        "jobId:$jobId has been marked as an outlier. (" +
          "jobOutlier:${outlierOutcome.job.isOutlier}, " +
          "outlierStreams:${outlierOutcome.numberOfOutlierStreams}, " +
          "details:$outlierOutcome)"
      }
    }
    reportOutlierMetric(outlierOutcome)
  }

  private fun evaluateJob(
    currentJob: ObsJobsStats,
    jobHistory: List<ObsJobsStats>,
  ): JobInfo {
    val currentJobMetrics = currentJob.toJobMetrics()
    val jobScore = outliers.evaluate(jobHistory.map { it.toJobMetrics() }, currentJobMetrics)
    val jobEvaluations = evaluateJobOutliers(jobScore)
    return JobInfo(
      jobId = currentJob.jobId,
      connectionId = currentJob.connectionId,
      workspaceId = currentJob.workspaceId,
      organizationId = currentJob.organizationId,
      sourceId = currentJob.sourceId,
      sourceDefinitionId = currentJob.sourceDefinitionId,
      sourceImageTag = currentJob.sourceImageTag,
      destinationId = currentJob.destinationId,
      destinationDefinitionId = currentJob.destinationDefinitionId,
      destinationImageTag = currentJob.destinationImageTag,
      jobType = currentJob.jobType,
      jobOutcome = currentJob.status,
      metrics = currentJobMetrics,
      evaluations = jobEvaluations,
      isOutlier = jobEvaluations.any { it.isOutlier },
    )
  }

  private fun evaluateStream(
    namespace: String?,
    name: String,
    currentStream: ObsStreamStats,
    streamHistory: List<ObsStreamStats>,
  ): StreamInfo {
    val currentStreamMetrics = currentStream.toStreamMetrics()
    val streamScore = outliers.evaluate(streamHistory.map { it.toStreamMetrics() }, currentStreamMetrics)
    val streamEvaluations = evaluateStreamOutliers(streamScore)
    return StreamInfo(
      namespace = namespace,
      name = name,
      wasBackfilled = currentStream.wasBackfilled,
      wasResumed = currentStream.wasResumed,
      metrics = currentStreamMetrics,
      evaluations = streamEvaluations,
      isOutlier = streamEvaluations.any { streamFeatureEval -> streamFeatureEval.isOutlier },
    )
  }

  private fun reportOutlierMetric(outlierOutcome: OutlierOutcome) {
    metricClient.count(
      OssMetricsRegistry.DATA_OBS_OUTLIER_CHECK,
      1,
      MetricAttribute(MetricTags.STATUS_TAG, outlierOutcome.isOutlier.toString()),
      MetricAttribute(MetricTags.CONNECTION_ID, outlierOutcome.job.connectionId.toString()),
      MetricAttribute(MetricTags.WORKSPACE_ID, outlierOutcome.job.workspaceId.toString()),
      MetricAttribute(MetricTags.SOURCE_DEFINITION_ID, outlierOutcome.job.sourceDefinitionId.toString()),
      MetricAttribute(MetricTags.DESTINATION_DEFINITION_ID, outlierOutcome.job.destinationDefinitionId.toString()),
      MetricAttribute(MetricTags.IS_CORRECTNESS_OUTLIER, (outlierOutcome.numberOfOutlierStreams > 0).toString()),
      MetricAttribute(MetricTags.IS_FRESHNESS_OUTLIER, outlierOutcome.job.isOutlier.toString()),
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

    fun ObsJobsStats.toJobMetrics(): JobMetrics =
      JobMetrics(
        attemptCount = attemptCount,
        durationSeconds = durationSeconds,
      )

    fun ObsStreamStats.toStreamMetrics(): StreamMetrics =
      StreamMetrics(
        bytesLoaded = bytesLoaded,
        recordsLoaded = recordsLoaded,
        recordsRejected = recordsRejected,
      )

    private fun evaluateJobOutliers(scores: Scores): List<OutlierEvaluation> {
      val evaluations = mutableListOf<OutlierEvaluation>()
      scores.scores["durationSeconds"]?.let {
        evaluations.add(
          OutlierEvaluation(
            dimension = "duration",
            score = it,
            threshold = 3.0,
            isOutlier = it > 3.0,
          ),
        )
      }
      scores.scores["attemptCount"]?.let {
        evaluations.add(
          OutlierEvaluation(
            dimension = "attempts",
            score = it,
            threshold = 3.0,
            isOutlier = it > 3.0,
          ),
        )
      }
      return evaluations
    }
  }

  /**
   * StreamMetrics outlier evaluation.
   *
   * We are adjusting the threshold for loaded data based on the average record count as a proxy for volume. Rationale being that a stream moving
   * little amount of data will be more susceptible to variations.
   *
   * RejectedRecords have their own deviation, mostly because they are more isolated events and shouldn't be tied to the same trend as the
   * "positive" amount of data loaded.
   */
  private fun evaluateStreamOutliers(scores: Scores): List<OutlierEvaluation> {
    val evals = mutableListOf<OutlierEvaluation>()
    val loadedCoefficient = volumeCoefficient(scores.mean["recordsLoaded"])
    val loadedThreshold = outlierThreshold(threshold = 3.0, coefficient = loadedCoefficient)
    scores.scores["bytesLoaded"]?.let {
      evals.add(
        OutlierEvaluation(
          dimension = "bytesLoaded",
          score = it,
          threshold = loadedThreshold,
          isOutlier = it.absoluteValue > loadedThreshold,
        ),
      )
    }
    scores.scores["recordsLoaded"]?.let {
      evals.add(
        OutlierEvaluation(
          dimension = "recordsLoaded",
          score = it,
          threshold = loadedThreshold,
          isOutlier = it.absoluteValue > loadedThreshold,
        ),
      )
    }

    val rejectedCoefficient = volumeCoefficient(scores.mean["recordsRejected"])
    val rejectedThreshold = outlierThreshold(threshold = 3.0, coefficient = rejectedCoefficient)
    scores.scores["recordsRejected"]?.let {
      evals.add(
        OutlierEvaluation(
          dimension = "recordsRejected",
          score = it,
          threshold = rejectedThreshold,
          isOutlier = it.absoluteValue > rejectedThreshold,
        ),
      )
    }
    return evals
  }

  /**
   * Returns a coefficient to adjust the outlier threshold. The intent is to increase the thresholds for connections with
   * lower volume of data.
   * Desired function has a high enough value for f(0) and should converge towards 1 as x goes towards infinity
   */
  private fun volumeCoefficient(x: Double?): Double = 1.0 + (1.0 / sqrt(x ?: 0.01))

  private fun outlierThreshold(
    threshold: Double,
    coefficient: Double,
  ): Double = threshold * coefficient
}
