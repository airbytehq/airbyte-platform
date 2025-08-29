/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.api.model.generated.AttemptRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.config.Attempt
import io.airbyte.config.JobConfig
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StreamDescriptor
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
import io.airbyte.statistics.OutlierEvaluation
import io.airbyte.statistics.OutlierRule
import io.airbyte.statistics.Outliers
import io.airbyte.statistics.Scores
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

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
  val sourceImageName: String,
  val sourceImageTag: String,
  val destinationId: UUID,
  val destinationDefinitionId: UUID,
  val destinationImageName: String,
  val destinationImageTag: String,
  val jobType: String,
  val jobOutcome: String,
  val metrics: JobMetrics,
  val evaluations: List<OutlierEvaluation>,
  val isOutlier: Boolean,
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
  private val jobObservabilityRulesService: JobObservabilityRulesService,
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
    val attemptsDurationSeconds: Long,
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
        durationSeconds = details.attemptsDurationSeconds,
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

    val jobConfig = jobPersistence.getJob(jobId)
    val configuredStreams =
      jobConfig.config.sync.configuredAirbyteCatalog.streams
        .map { it.streamDescriptor }
        .toSet()
    val jobInfo = evaluateJob(job.first(), jobHistory)

    val allStreamStats =
      obsStatsService
        .findJobStreamStatsAndPrevious(jobId, DEFAULT_INTERVAL, MAX_JOBS_COUNT, JOB_TYPES_TO_CONSIDER)
    val streams =
      allStreamStats
        .groupBy { it.id.streamNamespace to it.id.streamName }
        // Make sure we only evaluate the streams from the configured catalog
        .filter { configuredStreams.contains(StreamDescriptor().withNamespace(it.key.first).withName(it.key.second)) }
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
    val jobScore = outliers.getScores(jobHistory.map { it.toJobMetrics() }, currentJobMetrics)
    val jobEvaluations = evaluateJobOutliers(jobScore)

    val (sourceImage, destImage) = getImageNames(currentJob.jobId)
    return JobInfo(
      jobId = currentJob.jobId,
      connectionId = currentJob.connectionId,
      workspaceId = currentJob.workspaceId,
      organizationId = currentJob.organizationId,
      sourceId = currentJob.sourceId,
      sourceDefinitionId = currentJob.sourceDefinitionId,
      sourceImageName = sourceImage,
      sourceImageTag = currentJob.sourceImageTag,
      destinationId = currentJob.destinationId,
      destinationDefinitionId = currentJob.destinationDefinitionId,
      destinationImageName = destImage,
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
    val streamScore = outliers.getScores(streamHistory.map { it.toStreamMetrics() }, currentStreamMetrics)
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
      MetricAttribute(MetricTags.SOURCE_IMAGE, outlierOutcome.job.sourceImageName),
      MetricAttribute(MetricTags.SOURCE_IMAGE_TAG, outlierOutcome.job.sourceImageTag),
      MetricAttribute(MetricTags.DESTINATION_DEFINITION_ID, outlierOutcome.job.destinationDefinitionId.toString()),
      MetricAttribute(MetricTags.DESTINATION_IMAGE, outlierOutcome.job.destinationImageName),
      MetricAttribute(MetricTags.DESTINATION_IMAGE_TAG, outlierOutcome.job.destinationImageTag),
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
    val durationSeconds = getDurationSecondsFromAttempts(job.attempts)

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
      attemptsDurationSeconds = durationSeconds,
    )
  }

  private fun getDurationSecondsFromAttempts(attempts: List<Attempt>): Long =
    attempts.sumOf { attempt ->
      attempt.output
        ?.sync
        ?.standardSyncSummary
        ?.getDurationSecondsFromSummary() ?: attempt.getDurationsSecondsWhenNoJobOutput()
    }

  private fun StandardSyncSummary.getDurationSecondsFromSummary(): Long? =
    // The SyncSummary of a failed attempt may not have start/end times
    if (endTime != null && startTime != null) {
      (endTime - startTime) / 1000
    } else {
      null
    }

  private fun Attempt.getDurationsSecondsWhenNoJobOutput(): Long = (endedAtInSecond ?: updatedAtInSecond) - createdAtInSecond

  /**
   * returns (souceImageName, destinationImageName) for a jobId
   */
  private fun getImageNames(jobId: Long): Pair<String, String> {
    val job = jobPersistence.getJob(jobId)
    val sourceVersion = job.config.getSourceVersionId()?.let { actorDefinitionService.getActorDefinitionVersion(it) }
    val destVersion = actorDefinitionService.getActorDefinitionVersion(job.config.getDestinationVersionId())
    return Pair(
      sourceVersion?.dockerRepository ?: "empty-source",
      destVersion.dockerRepository,
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

  private fun ObsJobsStats.toJobMetrics(): JobMetrics =
    JobMetrics(
      attemptCount = attemptCount,
      durationSeconds = durationSeconds,
    )

  private fun ObsStreamStats.toStreamMetrics(): StreamMetrics =
    StreamMetrics(
      bytesLoaded = bytesLoaded,
      recordsLoaded = recordsLoaded,
      recordsRejected = recordsRejected,
    )

  private fun evaluateJobOutliers(scores: Map<String, Scores>): List<OutlierEvaluation> =
    jobObservabilityRulesService
      .getJobOutlierRules()
      .mapNotNull { evaluateRule(scores, rule = it) }

  private fun evaluateStreamOutliers(scores: Map<String, Scores>): List<OutlierEvaluation> =
    jobObservabilityRulesService
      .getStreamOutlierRules()
      .mapNotNull { evaluateRule(scores, rule = it) }

  private fun evaluateRule(
    scores: Map<String, Scores>,
    rule: OutlierRule,
  ): OutlierEvaluation? {
    val eval = rule.evaluate(scores)
    if (eval == null) {
      logger.warn { "Failed to evaluate outlier rule ${rule.name}" }
      metricClient.count(OssMetricsRegistry.DATA_OBS_OUTLIER_CHECK_ERRORS, value = 1)
    }
    return eval
  }
}
