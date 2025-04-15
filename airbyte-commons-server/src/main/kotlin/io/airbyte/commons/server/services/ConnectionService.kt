/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services

import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.config.Job
import io.airbyte.config.JobStatus
import io.airbyte.config.StandardSync
import io.airbyte.data.services.shared.ConnectionAutoDisabledReason
import io.airbyte.domain.models.ConnectionId
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.jvm.optionals.getOrNull
import io.airbyte.data.services.ConnectionService as ConnectionRepository
import io.airbyte.data.services.JobService as JobRepository

private val logger = KotlinLogging.logger {}

/**
 * Application service for performing business logic related to connections.
 */
interface ConnectionService {
  /**
   * Disable connections and record a timeline event for each.
   * If connections are disabled by an automatic process, the auto-disabled reason should be
   * provided so that an appropriate timeline event can be recorded.
   *
   * @return the set of connection IDs that were disabled
   */
  fun disableConnections(
    connectionIds: Set<ConnectionId>,
    autoDisabledReason: ConnectionAutoDisabledReason?,
  ): Set<ConnectionId>

  /**
   * Send a warning and/or disable a connection if it has too many failed jobs in a row and no
   * successful jobs within the configured time frame.
   *
   * @return true if the connection was disabled, false otherwise
   */
  fun warnOrDisableForConsecutiveFailures(
    connectionId: ConnectionId,
    timestamp: Instant,
  ): Boolean

  @Deprecated("Use ConnectionId version instead, removable once Java callers are converted to Kotlin")
  fun warnOrDisableForConsecutiveFailures(
    connectionId: UUID,
    timestamp: Instant,
  ): Boolean
}

@Singleton
open class ConnectionServiceImpl(
  private val connectionRepository: ConnectionRepository,
  private val connectionTimelineEventHelper: ConnectionTimelineEventHelper,
  private val warnOrDisableHelper: WarnOrDisableConnectionHelper,
  private val eventRunner: EventRunner,
) : ConnectionService {
  @Transactional("config")
  override fun disableConnections(
    connectionIds: Set<ConnectionId>,
    autoDisabledReason: ConnectionAutoDisabledReason?,
  ): Set<ConnectionId> {
    val disabledConnectionIds = connectionRepository.disableConnectionsById(connectionIds.toList().map(ConnectionId::value))
    disabledConnectionIds.forEach { connectionId ->
      connectionTimelineEventHelper.logStatusChangedEventInConnectionTimeline(
        connectionId,
        ConnectionStatus.INACTIVE,
        autoDisabledReason?.name,
        autoDisabledReason != null,
      )
      eventRunner.update(connectionId)
    }
    return disabledConnectionIds.map(::ConnectionId).toSet()
  }

  override fun warnOrDisableForConsecutiveFailures(
    connectionId: ConnectionId,
    timestamp: Instant,
  ): Boolean = warnOrDisableHelper.warnOrDisable(this, connectionId, timestamp)

  @Deprecated("Use ConnectionId version instead, removable once Java callers are converted to Kotlin")
  override fun warnOrDisableForConsecutiveFailures(
    connectionId: UUID,
    timestamp: Instant,
  ): Boolean = warnOrDisableForConsecutiveFailures(ConnectionId(connectionId), timestamp)
}

/**
 * Helper class for warning or disabling a connection based on the number of consecutive failed jobs.
 * Extracted into its own class to avoid overcrowding the ConnectionServiceImpl with dependencies
 * and configurations that are only needed for this specific use case.
 */
@Singleton
class WarnOrDisableConnectionHelper(
  private val connectionRepository: ConnectionRepository,
  private val jobService: JobRepository,
  private val jobPersistence: JobPersistence,
  private val jobNotifier: JobNotifier,
  @Value("\${airbyte.server.connection.limits.max-days}") private val maxDaysOfOnlyFailedJobsBeforeConnectionDisable: Int,
  @Value("\${airbyte.server.connection.limits.max-jobs}") private val maxFailedJobsInARowBeforeConnectionDisable: Int,
  @Value("\${airbyte.server.connection.limits.max-days-warning}") private val maxDaysOfOnlyFailedJobsBeforeConnectionWarning: Int,
  @Value("\${airbyte.server.connection.limits.max-jobs-warning}") private val maxFailedJobsInARowBeforeConnectionWarning: Int,
) {
  fun warnOrDisable(
    connectionService: ConnectionService,
    connectionId: ConnectionId,
    timestamp: Instant,
  ): Boolean {
    val firstJob = jobPersistence.getFirstReplicationJob(connectionId.value).getOrNull()
    val mostRecentJob = jobPersistence.getLastReplicationJob(connectionId.value).getOrNull()

    if (firstJob == null || mostRecentJob == null) {
      logger.error { "No replication job has been run." }
      return false
    }

    if (mostRecentJob.status != JobStatus.FAILED) {
      logger.error { "Most recent job with ID ${mostRecentJob.id} is not failed." }
      return false
    }

    val priorFailedJob = jobService.getPriorJobWithStatusForScopeAndJobId(connectionId.value.toString(), mostRecentJob.id, JobStatus.FAILED)
    val standardSync = connectionRepository.getStandardSync(connectionId.value)
    val lastSuccessfulJob = jobService.lastSuccessfulJobForScope(connectionId.value.toString())

    if (standardSync.status == StandardSync.Status.INACTIVE) {
      logger.info { "Connection with ID $connectionId is already disabled." }
      return false
    }

    val daysWithoutSuccessWindowStart = Instant.ofEpochSecond(lastSuccessfulJob?.createdAtInSecond ?: firstJob.createdAtInSecond)
    val numConsecutiveFailedJobs = jobService.countFailedJobsSinceLastSuccessForScope(connectionId.value.toString())
    val daysWithoutSuccess = getDaysBetweenTimestamps(daysWithoutSuccessWindowStart, timestamp)

    if (shouldDisableConnection(numConsecutiveFailedJobs, daysWithoutSuccess)) {
      connectionService.disableConnections(
        setOf(connectionId),
        ConnectionAutoDisabledReason.TOO_MANY_FAILED_JOBS_WITH_NO_RECENT_SUCCESS,
      )
      jobNotifier.autoDisableConnection(mostRecentJob, getAttemptStatsForJob(mostRecentJob))
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
