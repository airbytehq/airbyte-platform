/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.BooleanRead
import io.airbyte.api.model.generated.InternalOperationResult
import io.airbyte.api.model.generated.JobFailureRequest
import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.JobStatus
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobOutput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.MergeStreamStatWithMetadata
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.errorreporter.AttemptConfigReportingContext
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
import io.airbyte.persistence.job.errorreporter.SyncJobReportingContext
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

/**
 * AttemptHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class JobsHandler(
  private val jobPersistence: JobPersistence,
  private val jobCreationAndStatusUpdateHelper: JobCreationAndStatusUpdateHelper,
  private val jobNotifier: JobNotifier,
  private val jobErrorReporter: JobErrorReporter,
  private val connectionTimelineEventHelper: ConnectionTimelineEventHelper,
  private val featureFlagClient: FeatureFlagClient,
  private val metricClient: MetricClient,
) {
  /**
   * Mark job as failure.
   *
   * @param input - the request object.
   * @return - the result of the operation.
   */
  fun jobFailure(input: JobFailureRequest): InternalOperationResult {
    try {
      val jobId = input.jobId
      jobPersistence.failJob(jobId)
      val job = jobPersistence.getJob(jobId)

      val attemptStats: MutableList<JobPersistence.AttemptStats> = ArrayList()
      for (attempt in job.attempts) {
        val mergeStatsWithStreamMetadata =
          featureFlagClient.boolVariation(
            MergeStreamStatWithMetadata,
            Connection(input.connectionId),
          )
        if (mergeStatsWithStreamMetadata) {
          attemptStats.add(jobPersistence.getAttemptStatsWithStreamMetadata(jobId, attempt.getAttemptNumber()))
        } else {
          attemptStats.add(jobPersistence.getAttemptStats(jobId, attempt.getAttemptNumber()))
        }
      }
      if (job.configType == JobConfig.ConfigType.SYNC) {
        jobNotifier.failJob(job, attemptStats)
      }
      connectionTimelineEventHelper.logJobFailureEventInConnectionTimeline(job, input.connectionId, attemptStats)

      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_FAILED_BY_RELEASE_STAGE, job, input)

      val connectionId = UUID.fromString(job.scope)
      if (connectionId != input.connectionId) {
        log.warn("inconsistent connectionId for jobId '{}' (input:'{}', db:'{}')", jobId, input.connectionId, connectionId)
        metricClient.count(OssMetricsRegistry.INCONSISTENT_ACTIVITY_INPUT)
      }

      val jobSyncConfig = job.config.sync
      val destinationDefinitionVersionId: UUID?
      val sourceDefinitionVersionId: UUID?
      if (jobSyncConfig == null) {
        val resetConfig = job.config.resetConnection
        // In a reset, we run a fake source
        sourceDefinitionVersionId = null
        destinationDefinitionVersionId = resetConfig?.destinationDefinitionVersionId
      } else {
        sourceDefinitionVersionId = jobSyncConfig.sourceDefinitionVersionId
        destinationDefinitionVersionId = jobSyncConfig.destinationDefinitionVersionId
      }
      val jobContext = SyncJobReportingContext(jobId, sourceDefinitionVersionId, destinationDefinitionVersionId)
      reportIfLastFailedAttempt(job, connectionId, jobContext)
      jobCreationAndStatusUpdateHelper.trackCompletion(job, JobStatus.FAILED)
      return InternalOperationResult().succeeded(true)
    } catch (e: IOException) {
      jobCreationAndStatusUpdateHelper.trackCompletionForInternalFailure(
        input.jobId,
        input.connectionId,
        input.attemptNumber,
        JobStatus.FAILED,
        e,
      )
      throw RuntimeException(e)
    }
  }

  private fun reportIfLastFailedAttempt(
    job: Job,
    connectionId: UUID,
    jobContext: SyncJobReportingContext,
  ) {
    val lastFailedAttempt = job.getLastFailedAttempt()
    if (lastFailedAttempt.isPresent) {
      val attempt = lastFailedAttempt.get()
      val failureSummaryOpt = attempt.getFailureSummary()

      if (failureSummaryOpt.isPresent) {
        val failureSummary = failureSummaryOpt.get()
        var attemptConfig: AttemptConfigReportingContext? = null

        val syncConfigOpt = attempt.getSyncConfig()
        if (syncConfigOpt.isPresent) {
          val syncConfig = syncConfigOpt.get()
          attemptConfig =
            AttemptConfigReportingContext(
              syncConfig.sourceConfiguration,
              syncConfig.destinationConfiguration,
              syncConfig.state,
            )
        }

        jobErrorReporter.reportSyncJobFailure(connectionId, failureSummary, jobContext, attemptConfig)
        log.info("Successfully reported failure for job id '{}' connectionId: '{}'", job.id, connectionId)
      } else {
        log.info("Failure summary is missing, skipping reporting for jobId '{}', connectionId '{}'", job.id, connectionId)
      }
    } else {
      log.info("Last failed attempt is missing, skipping reporting for jobId '{}', connectionId '{}'", job.id, connectionId)
    }
  }

  /**
   * Report a job and a given attempt as successful.
   */
  fun jobSuccessWithAttemptNumber(input: JobSuccessWithAttemptNumberRequest): InternalOperationResult {
    try {
      val jobId = input.jobId
      val attemptNumber = input.attemptNumber

      if (input.standardSyncOutput != null) {
        val jobOutput =
          JobOutput().withSync(
            Jsons.convertValue(
              input.standardSyncOutput,
              StandardSyncOutput::class.java,
            ),
          )
        jobPersistence.writeOutput(jobId, attemptNumber, jobOutput)
      } else {
        log.warn("The job {} doesn't have any output for the attempt {}", jobId, attemptNumber)
      }
      jobPersistence.succeedAttempt(jobId, attemptNumber)
      val job = jobPersistence.getJob(jobId)
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.ATTEMPT_SUCCEEDED_BY_RELEASE_STAGE, job)

      val attemptStats: MutableList<JobPersistence.AttemptStats> = ArrayList()
      for (attempt in job.attempts) {
        val mergeStatsWithStreamMetadata =
          featureFlagClient.boolVariation(
            MergeStreamStatWithMetadata,
            Connection(input.connectionId),
          )
        if (mergeStatsWithStreamMetadata) {
          attemptStats.add(jobPersistence.getAttemptStatsWithStreamMetadata(jobId, attempt.getAttemptNumber()))
        } else {
          attemptStats.add(jobPersistence.getAttemptStats(jobId, attempt.getAttemptNumber()))
        }
      }
      if (job.configType == JobConfig.ConfigType.SYNC) {
        jobNotifier.successJob(job, attemptStats)
      }
      connectionTimelineEventHelper.logJobSuccessEventInConnectionTimeline(job, input.connectionId, attemptStats)
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_SUCCEEDED_BY_RELEASE_STAGE, job, input)
      jobCreationAndStatusUpdateHelper.trackCompletion(job, JobStatus.SUCCEEDED)

      return InternalOperationResult().succeeded(true)
    } catch (e: IOException) {
      jobCreationAndStatusUpdateHelper.trackCompletionForInternalFailure(
        input.jobId,
        input.connectionId,
        input.attemptNumber,
        JobStatus.SUCCEEDED,
        e,
      )
      throw RuntimeException(e)
    }
  }

  /**
   * Fail non terminal jobs.
   *
   * @param connectionId - the connection id.
   * @throws IOException - exception.
   */
  @Throws(IOException::class)
  fun failNonTerminalJobs(connectionId: UUID) {
    jobCreationAndStatusUpdateHelper.failNonTerminalJobs(connectionId)
  }

  /**
   * Report a job as started.
   */
  @Throws(IOException::class)
  fun reportJobStart(jobId: Long): InternalOperationResult {
    jobCreationAndStatusUpdateHelper.reportJobStart(jobId)
    return InternalOperationResult().succeeded(true)
  }

  /**
   * Did previous job succeed.
   *
   * @param connectionId - the connection id.
   * @param jobId - the job id.
   * @return - the result of the operation.
   * @throws IOException - exception.
   */
  @Throws(IOException::class)
  fun didPreviousJobSucceed(
    connectionId: UUID,
    jobId: Long,
  ): BooleanRead {
    // This DB call is a lift-n-shift from activity code to move database access out of the worker. It
    // is knowingly brittle and awkward. By setting pageSize to 2 this should just fetch the latest and
    // preceding job, but technically can fetch a much longer list.
    val jobs = jobPersistence.listJobsIncludingId(JobCreationAndStatusUpdateHelper.SYNC_CONFIG_SET, connectionId.toString(), jobId, 2)

    val previousJobSucceeded =
      jobCreationAndStatusUpdateHelper
        .findPreviousJob(jobs, jobId)
        .map { job: Job? ->
          jobCreationAndStatusUpdateHelper.didJobSucceed(
            job!!,
          )
        }.orElse(false)

    return BooleanRead().value(previousJobSucceeded)
  }

  /**
   * Records the cancellation of a job and its ongoing attempt. Kicks off notification and other
   * post-processing.
   */
  fun persistJobCancellation(
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Int,
    rawFailureSummary: Any?,
  ) {
    var failureSummary: AttemptFailureSummary? = null
    if (rawFailureSummary != null) {
      try {
        failureSummary = Jsons.convertValue(rawFailureSummary, AttemptFailureSummary::class.java)
      } catch (e: Exception) {
        throw BadRequestException("Unable to parse failureSummary.", e)
      }
    }

    try {
      // fail attempt
      jobPersistence.failAttempt(jobId, attemptNumber)
      jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummary)
      // persist cancellation
      jobPersistence.cancelJob(jobId)
      // post process
      val job = jobPersistence.getJob(jobId)
      val attemptStats: MutableList<JobPersistence.AttemptStats> = ArrayList()
      for (attempt in job.attempts) {
        attemptStats.add(jobPersistence.getAttemptStats(jobId, attempt.getAttemptNumber()))
      }
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_CANCELLED_BY_RELEASE_STAGE, job)
      jobCreationAndStatusUpdateHelper.trackCompletion(job, JobStatus.FAILED)
    } catch (e: IOException) {
      jobCreationAndStatusUpdateHelper.trackCompletionForInternalFailure(
        jobId,
        connectionId,
        attemptNumber,
        JobStatus.FAILED,
        e,
      )
      throw RuntimeException(e)
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
