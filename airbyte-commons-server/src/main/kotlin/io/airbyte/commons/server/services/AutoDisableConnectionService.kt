package io.airbyte.commons.server.services

import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.config.Job
import io.airbyte.config.JobStatus
import io.airbyte.config.StandardSync
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.shared.ConnectionAutoDisabledReason
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.jvm.optionals.getOrNull

private val logger = KotlinLogging.logger {}

@Singleton
open class AutoDisableConnectionService(
  private val connectionService: ConnectionService,
  private val jobService: JobService,
  private val jobPersistence: JobPersistence,
  private val jobNotifier: JobNotifier,
  @Value("\${airbyte.server.connection.limits.max-days}") private val maxDaysOfOnlyFailedJobsBeforeConnectionDisable: Int,
  @Value("\${airbyte.server.connection.limits.max-jobs}") private val maxFailedJobsInARowBeforeConnectionDisable: Int,
  @Value("\${airbyte.server.connection.limits.max-days-warning}") private val maxDaysOfOnlyFailedJobsBeforeConnectionWarning: Int,
  @Value("\${airbyte.server.connection.limits.max-jobs-warning}") private val maxFailedJobsInARowBeforeConnectionWarning: Int,
  private val connectionTimelineEventHelper: ConnectionTimelineEventHelper,
) {
  /**
   * Send a warning and/or disable a connection if it has too many failed jobs in a row and no
   * successful jobs within the configured time frame.
   *
   * @return true if the connection was disabled, false otherwise
   */
  @JvmOverloads
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun autoDisableConnection(
    connectionId: UUID,
    timestamp: Instant = Instant.now(),
  ): Boolean {
    val firstJob = jobPersistence.getFirstReplicationJob(connectionId).getOrNull()
    val mostRecentJob = jobPersistence.getLastReplicationJob(connectionId).getOrNull()

    if (firstJob == null || mostRecentJob == null) {
      logger.error { "No replication job has been run." }
      return false
    }

    if (mostRecentJob.status != JobStatus.FAILED) {
      logger.error { "Most recent job with ID ${mostRecentJob.id} is not failed." }
      return false
    }

    val priorFailedJob = jobService.getPriorJobWithStatusForScopeAndJobId(connectionId.toString(), mostRecentJob.id, JobStatus.FAILED)
    val standardSync = connectionService.getStandardSync(connectionId)
    val lastSuccessfulJob = jobService.lastSuccessfulJobForScope(connectionId.toString())

    if (standardSync.status == StandardSync.Status.INACTIVE) {
      logger.info { "Connection with ID $connectionId is already disabled." }
      return false
    }

    val daysWithoutSuccessWindowStart = Instant.ofEpochSecond(lastSuccessfulJob?.createdAtInSecond ?: firstJob.createdAtInSecond)
    val numConsecutiveFailedJobs = jobService.countFailedJobsSinceLastSuccessForScope(connectionId.toString())
    val daysWithoutSuccess = getDaysBetweenTimestamps(daysWithoutSuccessWindowStart, timestamp)

    if (shouldDisableConnection(numConsecutiveFailedJobs, daysWithoutSuccess)) {
      disableConnection(standardSync, mostRecentJob)
      return true
    }

    if (priorFailedJob != null &&
      shouldWarnAboutConnection(priorFailedJob, numConsecutiveFailedJobs, daysWithoutSuccess, daysWithoutSuccessWindowStart)
    ) {
      warnAboutConnection(mostRecentJob)
    }
    return false
  }

  private fun shouldDisableConnection(
    numConsecutiveFailedJobs: Int,
    daysWithoutSuccess: Int,
  ) = numConsecutiveFailedJobs >= maxFailedJobsInARowBeforeConnectionDisable &&
    daysWithoutSuccess >= maxDaysOfOnlyFailedJobsBeforeConnectionDisable

  private fun shouldWarnAboutConnection(
    priorFailedJob: Job,
    numConsecutiveFailedJobs: Int,
    daysWithoutSuccess: Int,
    daysWithoutSuccessWindowStart: Instant,
  ): Boolean {
    val priorDaysWithoutSuccess = getDaysBetweenTimestamps(daysWithoutSuccessWindowStart, Instant.ofEpochSecond(priorFailedJob.createdAtInSecond))
    val wasPriorWarningSent =
      priorDaysWithoutSuccess >= maxDaysOfOnlyFailedJobsBeforeConnectionWarning &&
        numConsecutiveFailedJobs - 1 >= maxFailedJobsInARowBeforeConnectionWarning

    return !wasPriorWarningSent &&
      daysWithoutSuccess >= maxDaysOfOnlyFailedJobsBeforeConnectionWarning &&
      numConsecutiveFailedJobs >= maxFailedJobsInARowBeforeConnectionWarning
  }

  private fun disableConnection(
    sync: StandardSync,
    mostRecentJob: Job,
  ) {
    sync.status = StandardSync.Status.INACTIVE
    connectionService.writeStandardSync(sync)
    connectionTimelineEventHelper.logStatusChangedEventInConnectionTimeline(
      sync.connectionId,
      ConnectionStatus.INACTIVE,
      ConnectionAutoDisabledReason.TOO_MANY_FAILED_JOBS_WITH_NO_RECENT_SUCCESS.name,
      true,
    )
    jobNotifier.autoDisableConnection(mostRecentJob, getAttemptStatsForJob(mostRecentJob))
  }

  private fun warnAboutConnection(mostRecentJob: Job) {
    jobNotifier.autoDisableConnectionWarning(mostRecentJob, getAttemptStatsForJob(mostRecentJob))
  }

  private fun getAttemptStatsForJob(job: Job): List<JobPersistence.AttemptStats> =
    job.attempts.map { jobPersistence.getAttemptStats(job.id, it.attemptNumber) }

  private fun getDaysBetweenTimestamps(
    firstInstant: Instant,
    secondInstant: Instant,
  ): Int = Duration.between(firstInstant, secondInstant).toDays().toInt()
}
