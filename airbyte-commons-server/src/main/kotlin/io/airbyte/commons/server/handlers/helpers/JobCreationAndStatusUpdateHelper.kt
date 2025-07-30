/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import io.airbyte.api.model.generated.JobFailureRequest
import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.server.JobStatus
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Attempt
import io.airbyte.config.Attempt.Companion.isAttemptInTerminalState
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.Metadata
import io.airbyte.config.ReleaseStage
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.FAILURE_ORIGINS_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.FAILURE_TYPES_KEY
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.MetricTags.getFailureOrigin
import io.airbyte.metrics.lib.MetricTags.getFailureType
import io.airbyte.metrics.lib.MetricTags.getReleaseStage
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.tracker.JobTracker
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.util.CollectionUtils
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Locale
import java.util.Objects
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream

/**
 * Helper class to handle and track job creation and status updates.
 */
@Singleton
class JobCreationAndStatusUpdateHelper(
  private val jobPersistence: JobPersistence,
  private val actorDefinitionService: ActorDefinitionService,
  private val connectionService: ConnectionService,
  private val jobNotifier: JobNotifier,
  private val jobTracker: JobTracker,
  private val connectionTimelineEventHelper: ConnectionTimelineEventHelper,
  private val metricClient: MetricClient,
) {
  fun findPreviousJob(
    jobs: List<Job>,
    targetJobId: Long,
  ): Optional<Job> {
    val targetJob =
      jobs
        .stream()
        .filter { j: Job -> j.id == targetJobId }
        .findFirst()

    // Target job not found or list is empty.
    if (targetJob.isEmpty) {
      return Optional.empty()
    }

    return jobs
      .stream()
      .filter { job: Job -> job.id != targetJobId } // Not our target job.
      .filter { job: Job -> job.createdAtInSecond < targetJob.get().createdAtInSecond } // Precedes target job.
      .reduce { a: Job, b: Job -> if (a.createdAtInSecond > b.createdAtInSecond) a else b } // Get latest.
  }

  fun didJobSucceed(job: Job): Boolean = job.status == io.airbyte.config.JobStatus.SUCCEEDED

  @Throws(IOException::class)
  fun failNonTerminalJobs(connectionId: UUID) {
    val jobs =
      jobPersistence.listJobsForConnectionWithStatuses(
        connectionId,
        Job.REPLICATION_TYPES,
        io.airbyte.config.JobStatus.NON_TERMINAL_STATUSES,
      )

    for ((jobId, _, _, _, attempts) in jobs) {
      // fail all non-terminal attempts
      for (attempt in attempts) {
        if (isAttemptInTerminalState(attempt)) {
          continue
        }

        val attemptNumber = attempt.getAttemptNumber()
        log.info("Failing non-terminal attempt {} for non-terminal job {}", attemptNumber, jobId)
        jobPersistence.failAttempt(jobId, attemptNumber)
        jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummaryForTemporalCleaningJobState(jobId, attemptNumber))
      }

      log.info("Failing non-terminal job {}", jobId)
      jobPersistence.failJob(jobId)

      val attemptStats: MutableList<JobPersistence.AttemptStats> = ArrayList()
      for (attempt in attempts) {
        attemptStats.add(jobPersistence.getAttemptStats(jobId, attempt.getAttemptNumber()))
      }
      val failedJob = jobPersistence.getJob(jobId)
      jobNotifier.failJob(failedJob, attemptStats)
      // log failure events in connection timeline
      connectionTimelineEventHelper.logJobFailureEventInConnectionTimeline(failedJob, connectionId, attemptStats)
      trackCompletion(failedJob, JobStatus.FAILED)
    }
  }

  private fun parseIsJobRunningOnCustomConnectorForMetrics(job: Job): String {
    if (job.config == null || job.config.sync == null) {
      return "null"
    }
    if (job.config.sync.isSourceCustomConnector == null ||
      job.config.sync.isDestinationCustomConnector == null
    ) {
      return "null"
    }
    return (job.config.sync.isSourceCustomConnector || job.config.sync.isDestinationCustomConnector).toString()
  }

  @Throws(IOException::class)
  private fun emitAttemptEvent(
    metric: OssMetricsRegistry,
    job: Job,
    attemptNumber: Int,
    additionalAttributes: List<MetricAttribute>,
  ) {
    val releaseStages = getJobToReleaseStages(job)
    val releaseStagesOrdered = orderByReleaseStageAsc(releaseStages)
    val connectionId: UUID = UUID.fromString(job.scope)
    val dataplaneGroupName = connectionService.getDataplaneGroupNameForConnection(connectionId)
    val parsedAttemptNumber = parseAttemptNumberOrNull(attemptNumber)

    val baseMetricAttributes: MutableList<MetricAttribute> = ArrayList()
    if (dataplaneGroupName != null) {
      baseMetricAttributes.add(MetricAttribute(MetricTags.GEOGRAPHY, dataplaneGroupName))
    }
    if (parsedAttemptNumber != null) {
      baseMetricAttributes.add(MetricAttribute(MetricTags.ATTEMPT_NUMBER, parsedAttemptNumber))
    }
    baseMetricAttributes
      .add(MetricAttribute(MetricTags.MIN_CONNECTOR_RELEASE_STATE, getReleaseStage(getOrNull(releaseStagesOrdered, 0))))
    baseMetricAttributes
      .add(MetricAttribute(MetricTags.MAX_CONNECTOR_RELEASE_STATE, getReleaseStage(getOrNull(releaseStagesOrdered, 1))))
    baseMetricAttributes.add(MetricAttribute(MetricTags.IS_CUSTOM_CONNECTOR_SYNC, parseIsJobRunningOnCustomConnectorForMetrics(job)))
    baseMetricAttributes.addAll(imageAttrsFromJob(job))
    baseMetricAttributes.addAll(linkAttrsFromJob(job))

    val allMetricAttributes =
      Stream
        .concat(baseMetricAttributes.stream(), additionalAttributes.stream())
        .toList()
        .toTypedArray<MetricAttribute>()
    metricClient.count(metric, 1L, *allMetricAttributes)
  }

  @Throws(IOException::class)
  fun emitAttemptCreatedEvent(
    job: Job,
    attemptNumber: Int,
  ) {
    emitAttemptEvent(OssMetricsRegistry.ATTEMPTS_CREATED, job, attemptNumber, listOf())
  }

  private fun emitAttemptCompletedEvent(
    job: Job,
    attempt: Attempt,
  ) {
    val failureOrigin =
      attempt.getFailureSummary().flatMap { summary: AttemptFailureSummary ->
        summary.failures
          .stream()
          .map { obj: FailureReason -> obj.failureOrigin }
          .filter { obj: FailureReason.FailureOrigin? -> Objects.nonNull(obj) }
          .map { obj: FailureReason.FailureOrigin -> obj.name }
          .findFirst()
      }

    val failureType =
      attempt.getFailureSummary().flatMap<String> { summary: AttemptFailureSummary ->
        summary.failures
          .stream()
          .map<FailureReason.FailureType> { obj: FailureReason -> obj.failureType }
          .filter { obj: FailureReason.FailureType? -> Objects.nonNull(obj) }
          .map<String> { obj: FailureReason.FailureType -> getFailureType(obj) }
          .findFirst()
      }

    val externalMsg =
      attempt.getFailureSummary().flatMap { summary: AttemptFailureSummary ->
        summary.failures
          .stream()
          .map { obj: FailureReason -> obj.externalMessage }
          // For DD, we get 200 characters between the key and value, so we keep it relatively short here.
          .filter { obj: String? -> Objects.nonNull(obj) }
          .map { s: String? -> this.abbreviate(s) }
          .findFirst()
      }

    val internalMsg =
      attempt.getFailureSummary().flatMap { summary: AttemptFailureSummary ->
        summary.failures
          .stream()
          .map { obj: FailureReason -> obj.internalMessage }
          // For DD, we get 200 characters between the key and value, so we keep it relatively short here.
          .filter { obj: String? -> Objects.nonNull(obj) }
          .map { s: String? -> this.abbreviate(s) }
          .findFirst()
      }

    val additionalAttributes: MutableList<MetricAttribute> = ArrayList()
    additionalAttributes.add(MetricAttribute(MetricTags.ATTEMPT_OUTCOME, attempt.status.toString()))
    failureOrigin.ifPresent { o: String? ->
      additionalAttributes.add(
        MetricAttribute(
          MetricTags.FAILURE_ORIGIN,
          o!!,
        ),
      )
    }
    failureType.ifPresent { t: String? ->
      additionalAttributes.add(
        MetricAttribute(
          MetricTags.FAILURE_TYPE,
          t!!,
        ),
      )
    }
    if (attempt.processingTaskQueue != null) {
      additionalAttributes.add(MetricAttribute(MetricTags.ATTEMPT_QUEUE, attempt.processingTaskQueue!!))
    }
    externalMsg.ifPresent { e: String? ->
      additionalAttributes.add(
        MetricAttribute(
          MetricTags.EXTERNAL_MESSAGE,
          e!!,
        ),
      )
    }
    internalMsg.ifPresent { i: String? ->
      additionalAttributes.add(
        MetricAttribute(
          MetricTags.INTERNAL_MESSAGE,
          i!!,
        ),
      )
    }

    try {
      emitAttemptEvent(OssMetricsRegistry.ATTEMPTS_COMPLETED, job, attempt.getAttemptNumber(), additionalAttributes)
    } catch (e: IOException) {
      log.info("Failed to record attempt completed metric for attempt {} of job {}", attempt.getAttemptNumber(), job.id)
    }
  }

  private fun abbreviate(s: String?): String? {
    val length = 50

    if (s == null || s.length <= length) {
      return s
    }

    return s.substring(0, length - 3) + "..."
  }

  /**
   * Adds attributes necessary to link back to the connection from DD or otherwise.
   */
  private fun linkAttrsFromJob(job: Job): List<MetricAttribute> {
    val attrs: MutableList<MetricAttribute> = ArrayList()
    if (job.configType == ConfigType.SYNC) {
      val config = job.config.sync
      attrs.add(MetricAttribute(MetricTags.WORKSPACE_ID, config.workspaceId.toString()))
    } else if (job.configType == ConfigType.REFRESH) {
      val config = job.config.refresh
      attrs.add(MetricAttribute(MetricTags.WORKSPACE_ID, config.workspaceId.toString()))
    } else if (job.configType == ConfigType.RESET_CONNECTION) {
      val config = job.config.resetConnection
      attrs.add(MetricAttribute(MetricTags.WORKSPACE_ID, config.workspaceId.toString()))
    }

    if (job.scope != null) {
      attrs.add(MetricAttribute(MetricTags.CONNECTION_ID, job.scope))
    }

    return attrs
  }

  /**
   * Adds image attributes necessary to filter by via DD or otherwise.
   */
  private fun imageAttrsFromJob(job: Job): List<MetricAttribute> {
    val attrs: MutableList<MetricAttribute> = ArrayList()
    if (job.configType == ConfigType.SYNC) {
      val config = job.config.sync
      attrs.add(MetricAttribute(MetricTags.SOURCE_IMAGE, config.sourceDockerImage))
      attrs.add(MetricAttribute(MetricTags.DESTINATION_IMAGE, config.destinationDockerImage))
      attrs.add(MetricAttribute(MetricTags.WORKSPACE_ID, config.workspaceId.toString()))
    } else if (job.configType == ConfigType.REFRESH) {
      val config = job.config.refresh
      attrs.add(MetricAttribute(MetricTags.SOURCE_IMAGE, config.sourceDockerImage))
      attrs.add(MetricAttribute(MetricTags.DESTINATION_IMAGE, config.destinationDockerImage))
      attrs.add(MetricAttribute(MetricTags.WORKSPACE_ID, config.workspaceId.toString()))
    } else if (job.configType == ConfigType.RESET_CONNECTION) {
      val config = job.config.resetConnection
      attrs.add(MetricAttribute(MetricTags.DESTINATION_IMAGE, config.destinationDockerImage))
      attrs.add(MetricAttribute(MetricTags.WORKSPACE_ID, config.workspaceId.toString()))
    }

    return attrs
  }

  @VisibleForTesting
  @Throws(IOException::class)
  fun getJobToReleaseStages(job: Job?): List<ReleaseStage> {
    if (job?.config == null || job.config.configType == null) {
      return emptyList()
    }

    val actorDefVersionIds =
      when (job.config.configType) {
        ConfigType.SYNC -> listOf(job.config.sync.destinationDefinitionVersionId, job.config.sync.sourceDefinitionVersionId)
        ConfigType.RESET_CONNECTION -> listOf(job.config.resetConnection.destinationDefinitionVersionId)
        ConfigType.REFRESH ->
          listOf(
            job.config.refresh.sourceDefinitionVersionId,
            job.config.refresh.destinationDefinitionVersionId,
          )

        else -> throw IllegalArgumentException("Unexpected config type: " + job.configType)
      }

    return actorDefinitionService
      .getActorDefinitionVersions(actorDefVersionIds)
      .stream()
      .map { obj: ActorDefinitionVersion -> obj.releaseStage }
      .toList()
  }

  @Throws(IOException::class)
  fun emitJobToReleaseStagesMetric(
    metric: OssMetricsRegistry,
    job: Job,
  ) {
    emitToReleaseStagesMetricHelper(metric, job, emptyList())
  }

  @Throws(IOException::class)
  fun emitJobToReleaseStagesMetric(
    metric: OssMetricsRegistry,
    job: Job,
    input: JobSuccessWithAttemptNumberRequest?,
  ) {
    val additionalAttributes: MutableList<MetricAttribute> = ArrayList()
    if (job.configType == ConfigType.SYNC) {
      val sync = job.config.sync
      additionalAttributes.add(MetricAttribute(MetricTags.SOURCE_IMAGE_IS_DEFAULT, sync.sourceDockerImageIsDefault.toString()))
      additionalAttributes
        .add(MetricAttribute(MetricTags.DESTINATION_IMAGE_IS_DEFAULT, sync.destinationDockerImageIsDefault.toString()))
      additionalAttributes.add(MetricAttribute(MetricTags.WORKSPACE_ID, sync.workspaceId.toString()))
    } else if (job.configType == ConfigType.REFRESH) {
      val refresh = job.config.refresh
      additionalAttributes.add(MetricAttribute(MetricTags.WORKSPACE_ID, refresh.workspaceId.toString()))
    }
    additionalAttributes.addAll(imageAttrsFromJob(job))
    emitToReleaseStagesMetricHelper(metric, job, additionalAttributes)
  }

  @Throws(IOException::class)
  fun emitJobToReleaseStagesMetric(
    metric: OssMetricsRegistry,
    job: Job,
    input: JobFailureRequest?,
  ) {
    val additionalAttributes: MutableList<MetricAttribute> = ArrayList()
    if (job.configType == ConfigType.SYNC) {
      val sync = job.config.sync
      additionalAttributes.add(MetricAttribute(MetricTags.SOURCE_IMAGE_IS_DEFAULT, sync.sourceDockerImageIsDefault.toString()))
      additionalAttributes
        .add(MetricAttribute(MetricTags.DESTINATION_IMAGE_IS_DEFAULT, sync.destinationDockerImageIsDefault.toString()))
      additionalAttributes.add(MetricAttribute(MetricTags.WORKSPACE_ID, sync.workspaceId.toString()))
      additionalAttributes.addAll(imageAttrsFromJob(job))
      job
        .getLastAttempt()
        .flatMap { obj: Attempt -> obj.getFailureSummary() }
        .ifPresent { attemptFailureSummary: AttemptFailureSummary ->
          for (failureReason in attemptFailureSummary.failures) {
            val metricTag = getFailureType(failureReason.failureType)
            val failureOriginName = failureReason.failureOrigin.name
            additionalAttributes.add(
              MetricAttribute(
                java.lang.String
                  .join("-", failureOriginName, metricTag)
                  .lowercase(Locale.getDefault()),
                "true",
              ),
            )
          }
        }
    }
    emitToReleaseStagesMetricHelper(metric, job, additionalAttributes)
  }

  @Throws(IOException::class)
  private fun emitToReleaseStagesMetricHelper(
    metric: OssMetricsRegistry,
    job: Job,
    additionalAttributes: List<MetricAttribute>,
  ) {
    val releaseStages = getJobToReleaseStages(job)
    if (releaseStages.isEmpty()) {
      return
    }

    for (stage in releaseStages) {
      if (stage != null) {
        val attributes: MutableList<MetricAttribute> = ArrayList()
        attributes.add(MetricAttribute(MetricTags.RELEASE_STAGE, getReleaseStage(stage)))
        attributes.addAll(additionalAttributes)

        metricClient.count(metric, 1L, *attributes.toTypedArray<MetricAttribute>())
      }
    }
  }

  @Throws(IOException::class)
  fun trackCompletion(
    job: Job,
    status: JobStatus,
  ) {
    emitAttemptCompletedEventIfAttemptPresent(job)
    jobTracker.trackSync(
      job,
      status.convertTo<JobTracker.JobState>(),
    )
  }

  fun emitAttemptCompletedEventIfAttemptPresent(job: Job?) {
    if (job == null) {
      return
    }

    val lastAttempt = job.getLastAttempt()
    if (lastAttempt.isPresent) {
      emitAttemptCompletedEvent(job, lastAttempt.get())
    }
  }

  fun trackCompletionForInternalFailure(
    jobId: Long,
    connectionId: UUID,
    attemptId: Int,
    status: JobStatus,
    e: Exception,
  ) {
    jobTracker.trackSyncForInternalFailure(
      jobId,
      connectionId,
      attemptId,
      status.convertTo<JobTracker.JobState>(),
      e,
    )
  }

  /**
   * Adds the failure origins to the APM trace.
   *
   * @param failureSummary The [AttemptFailureSummary] containing the failure reason(s).
   */
  fun traceFailures(failureSummary: AttemptFailureSummary?) {
    if (failureSummary != null) {
      if (CollectionUtils.isNotEmpty(failureSummary.failures)) {
        addTagsToTrace(
          java.util.Map.of<String?, String>(
            FAILURE_TYPES_KEY,
            failureSummary.failures.joinToString(",") { getFailureType(it.failureType) },
            FAILURE_ORIGINS_KEY,
            failureSummary.failures.joinToString(",") { it.failureOrigin.name },
          ),
        )
      }
    } else {
      addTagsToTrace(
        java.util.Map.of<String?, String?>(
          FAILURE_TYPES_KEY,
          getFailureType(null),
          FAILURE_ORIGINS_KEY,
          FailureReason.FailureOrigin.UNKNOWN.value(),
        ),
      )
    }
  }

  /**
   * Records a metric for each failure reason.
   *
   * @param failureSummary The [AttemptFailureSummary] containing the failure reason(s).
   */
  fun trackFailures(failureSummary: AttemptFailureSummary?) {
    if (failureSummary != null) {
      for (reason in failureSummary.failures) {
        metricClient.count(
          OssMetricsRegistry.ATTEMPT_FAILED_BY_FAILURE_ORIGIN,
          1L,
          MetricAttribute(MetricTags.FAILURE_ORIGIN, getFailureOrigin(reason.failureOrigin)),
          MetricAttribute(MetricTags.FAILURE_TYPE, getFailureType(reason.failureType)),
        )
      }
    } else {
      metricClient.count(
        OssMetricsRegistry.ATTEMPT_FAILED_BY_FAILURE_ORIGIN,
        1L,
        MetricAttribute(MetricTags.FAILURE_ORIGIN, FailureReason.FailureOrigin.UNKNOWN.value()),
        MetricAttribute(MetricTags.FAILURE_TYPE, getFailureType(null)),
      )
    }
  }

  /**
   * Create attempt failure summary.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return attempt failure summary
   */
  private fun failureSummaryForTemporalCleaningJobState(
    jobId: Long,
    attemptNumber: Int,
  ): AttemptFailureSummary {
    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureReason.FailureType.TRANSIENT_ERROR)
        .withInternalMessage(
          "Setting attempt to FAILED because the workflow for this connection was restarted, and existing job state was cleaned.",
        ).withExternalMessage("An internal transient Airbyte error has occurred. The sync should work fine on the next retry.")
        .withTimestamp(System.currentTimeMillis())
        .withMetadata(jobAndAttemptMetadata(jobId, attemptNumber))
    return AttemptFailureSummary().withFailures(listOf(failureReason))
  }

  /**
   * Report a job as started.
   */
  @Throws(IOException::class)
  fun reportJobStart(jobId: Long) {
    val job = jobPersistence.getJob(jobId)
    jobTracker.trackSync(job, JobTracker.JobState.STARTED)
  }

  companion object {
    private val log = KotlinLogging.logger {}

    private const val JOB_ID_METADATA_KEY = "jobId"
    private const val ATTEMPT_NUMBER_METADATA_KEY = "attemptNumber"

    private const val MAX_ATTEMPTS = 3
    private val RELEASE_STAGE_ORDER: Map<ReleaseStage, Int> =
      java.util.Map.of(
        ReleaseStage.CUSTOM,
        1,
        ReleaseStage.ALPHA,
        2,
        ReleaseStage.BETA,
        3,
        ReleaseStage.GENERALLY_AVAILABLE,
        4,
      )
    private val RELEASE_STAGE_COMPARATOR: Comparator<ReleaseStage> = Comparator.comparingInt { key: ReleaseStage -> RELEASE_STAGE_ORDER[key]!! }

    @JvmField
    val SYNC_CONFIG_SET: Set<ConfigType> = java.util.Set.of(ConfigType.SYNC, ConfigType.REFRESH)

    @JvmStatic
    @VisibleForTesting
    fun orderByReleaseStageAsc(releaseStages: List<ReleaseStage>): List<ReleaseStage> =
      releaseStages
        .stream()
        .filter { stage: ReleaseStage? -> stage != null }
        .sorted(RELEASE_STAGE_COMPARATOR)
        .toList()

    /**
     * Extract the attempt number from an attempt. If the number is anonymous (not 0,1,2,3) for some
     * reason return null. We don't want to accidentally have high cardinality here because of a bug.
     *
     * @param attemptNumber - attemptNumber to parse
     * @return extract attempt number or null
     */
    private fun parseAttemptNumberOrNull(attemptNumber: Int): String? =
      if (attemptNumber > MAX_ATTEMPTS) {
        null
      } else {
        attemptNumber.toString()
      }

    /**
     * Get the value at an index or null if the index is out of bounds.
     *
     * @param list list to extract from
     * @param index index to extra
     * @param <T> type of the value in the list
     * @return extract value at index or null if index is out of bounds.
     </T> */
    private fun <T> getOrNull(
      list: List<T>,
      index: Int,
    ): T? {
      Preconditions.checkNotNull(list)
      return if (list.size > index) {
        list[index]
      } else {
        null
      }
    }

    private fun jobAndAttemptMetadata(
      jobId: Long,
      attemptNumber: Int,
    ): Metadata =
      Metadata()
        .withAdditionalProperty(JOB_ID_METADATA_KEY, jobId)
        .withAdditionalProperty(ATTEMPT_NUMBER_METADATA_KEY, attemptNumber)
  }
}
