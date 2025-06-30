/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services

import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.StandardSync
import io.airbyte.data.services.JobService
import io.airbyte.data.services.shared.ConnectionAutoDisabledReason
import io.airbyte.domain.models.ConnectionId
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.UUID
import io.airbyte.data.services.ConnectionService as ConnectionRepository

class ConnectionServiceTest {
  private val connectionRepository: ConnectionRepository = mockk()
  private val connectionTimelineEventHelper: ConnectionTimelineEventHelper = mockk()
  private val warnOrDisableHelper: WarnOrDisableConnectionHelper = mockk()
  private val eventRunner: EventRunner = mockk()

  private val service =
    ConnectionServiceImpl(
      connectionRepository,
      connectionTimelineEventHelper,
      warnOrDisableHelper,
      eventRunner,
    )

  private val connectionId1 = ConnectionId(UUID.randomUUID())
  private val connectionId2 = ConnectionId(UUID.randomUUID())
  private val connectionId3 = ConnectionId(UUID.randomUUID())

  @Nested
  inner class DisableConnections {
    @Test
    fun `should call connectionRepository to disable connections`() {
      every { connectionRepository.disableConnectionsById(any()) } returns setOf(connectionId1.value, connectionId2.value, connectionId3.value)
      every { connectionTimelineEventHelper.logStatusChangedEventInConnectionTimeline(any(), any(), any(), any()) } returns mockk()
      every { eventRunner.update(any()) } returns mockk()

      service.disableConnections(setOf(connectionId1, connectionId2, connectionId3), null)

      verify { connectionRepository.disableConnectionsById(listOf(connectionId1.value, connectionId2.value, connectionId3.value)) }
    }

    @Test
    fun `should add connection timeline events for disabled connections`() {
      every { connectionRepository.disableConnectionsById(any()) } returns setOf(connectionId2.value, connectionId3.value) // just 2 and 3
      every { connectionTimelineEventHelper.logStatusChangedEventInConnectionTimeline(any(), any(), any(), any()) } returns mockk()
      every { eventRunner.update(any()) } returns mockk()

      service.disableConnections(setOf(connectionId1, connectionId2, connectionId3), ConnectionAutoDisabledReason.INVALID_PAYMENT_METHOD)

      // connectionId1 was already disabled according to the connectionRepository, so no event should be added
      verify(exactly = 0) {
        connectionTimelineEventHelper.logStatusChangedEventInConnectionTimeline(
          connectionId1.value,
          any(),
          any(),
          any(),
        )
      }
      verify {
        connectionTimelineEventHelper.logStatusChangedEventInConnectionTimeline(
          connectionId2.value,
          ConnectionStatus.INACTIVE,
          ConnectionAutoDisabledReason.INVALID_PAYMENT_METHOD.name,
          true,
        )
      }
      verify {
        connectionTimelineEventHelper.logStatusChangedEventInConnectionTimeline(
          connectionId3.value,
          ConnectionStatus.INACTIVE,
          ConnectionAutoDisabledReason.INVALID_PAYMENT_METHOD.name,
          true,
        )
      }
    }

    @Test
    fun `should call event runner to update disabled connections`() {
      every { connectionRepository.disableConnectionsById(any()) } returns setOf(connectionId2.value, connectionId3.value) // just 2 and 3
      every { connectionTimelineEventHelper.logStatusChangedEventInConnectionTimeline(any(), any(), any(), any()) } returns mockk()
      every { eventRunner.update(any()) } returns mockk()

      service.disableConnections(setOf(connectionId1, connectionId2, connectionId3), null)

      // connectionId1 was already disabled according to the connectionRepository, so the event runner should not be called for it
      verify(exactly = 0) {
        eventRunner.update(connectionId1.value)
      }
      verify {
        eventRunner.update(connectionId2.value)
      }
      verify {
        eventRunner.update(connectionId3.value)
      }
    }
  }

  @Nested
  inner class WarnOrDisableForConsecutiveFailures {
    @Test
    fun `should call warnOrDisableHelper`() {
      val timestamp = mockk<Instant>()
      every { warnOrDisableHelper.warnOrDisable(any(), any(), any()) } returns true

      service.warnOrDisableForConsecutiveFailures(connectionId1, timestamp)

      verify { warnOrDisableHelper.warnOrDisable(service, connectionId1, timestamp) }
    }
  }

  @Nested
  inner class WarnOrDisableConnectionHelperTest {
    private val connectionService: ConnectionService = mockk()
    private val connectionRepository: ConnectionRepository = mockk()
    private val jobService: JobService = mockk()
    private val jobPersistence: JobPersistence = mockk()
    private val jobNotifier: JobNotifier = mockk()

    private val maxDaysBeforeDisable = 30
    private val maxJobsBeforeDisable = 5
    private val maxDaysBeforeWarning = 15
    private val maxJobsBeforeWarning = 3
    private val connectionId = UUID.randomUUID()
    private val connectionIdWrapped = ConnectionId(connectionId)
    private val timestamp = Instant.now()
    private val jobId1 = 1L
    private val jobId2 = 2L
    private val jobId3 = 3L

    private val helper =
      WarnOrDisableConnectionHelper(
        connectionRepository,
        jobService,
        jobPersistence,
        jobNotifier,
        maxDaysBeforeDisable,
        maxJobsBeforeDisable,
        maxDaysBeforeWarning,
        maxJobsBeforeWarning,
      )

    private fun mockJob(
      id: Long,
      status: JobStatus,
      createdAt: Long = Instant.now().epochSecond,
    ): Job =
      Job(
        id = id,
        configType = JobConfig.ConfigType.SYNC,
        scope = connectionId.toString(),
        config = JobConfig(),
        attempts = emptyList(),
        status = status,
        startedAtInSecond = null,
        createdAtInSecond = createdAt,
        updatedAtInSecond = createdAt,
        isScheduled = false,
      )

    @Nested
    inner class AutoDisableConnection {
      @Test
      fun `should return false if no replication jobs found`() {
        every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.empty()
        every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.empty()

        helper.warnOrDisable(connectionService, connectionIdWrapped, timestamp).shouldBeFalse()

        verify(exactly = 0) { connectionRepository.getStandardSync(any()) }
        verify(exactly = 0) { connectionRepository.writeStandardSync(any()) }
        verify(exactly = 0) { jobNotifier.autoDisableConnectionWarning(any(), any()) }
        verify(exactly = 0) { jobNotifier.autoDisableConnection(any(), any()) }
      }

      @Test
      fun `should return false if most recent job is not failed`() {
        val firstJob = mockJob(id = jobId1, status = JobStatus.SUCCEEDED)
        val lastJob = mockJob(id = jobId2, status = JobStatus.SUCCEEDED)

        every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.of(firstJob)
        every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.of(lastJob)

        helper.warnOrDisable(connectionService, connectionIdWrapped, timestamp).shouldBeFalse()

        verify(exactly = 0) { connectionRepository.getStandardSync(any()) }
        verify(exactly = 0) { connectionRepository.writeStandardSync(any()) }
        verify(exactly = 0) { jobNotifier.autoDisableConnectionWarning(any(), any()) }
        verify(exactly = 0) { jobNotifier.autoDisableConnection(any(), any()) }
      }

      @Test
      fun `should disable connection if disable thresholds are met`() {
        val failedJob =
          mockJob(
            id = jobId1,
            status = JobStatus.FAILED,
            createdAt = timestamp.minus(Duration.ofDays(31)).epochSecond,
          )
        val sync =
          mockk<StandardSync>(relaxed = true).also {
            every { it.status } returns StandardSync.Status.ACTIVE
            every { it.connectionId } returns connectionId
          }

        every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.of(failedJob)
        every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.of(failedJob)
        every { jobService.getPriorJobWithStatusForScopeAndJobId(connectionId.toString(), jobId1, JobStatus.FAILED) } returns null
        every { jobService.countFailedJobsSinceLastSuccessForScope(connectionId.toString()) } returns maxJobsBeforeDisable
        every { jobService.lastSuccessfulJobForScope(connectionId.toString()) } returns null
        every { connectionRepository.getStandardSync(connectionId) } returns sync
        every { connectionRepository.writeStandardSync(sync) } just Runs
        every { jobNotifier.autoDisableConnection(any(), any()) } just Runs
        every { jobPersistence.getAttemptStats(any(), any()) } returns mockk()
        every { connectionService.disableConnections(any(), any()) } returns mockk()

        helper.warnOrDisable(connectionService, connectionIdWrapped, timestamp).shouldBeTrue()

        verify {
          connectionService.disableConnections(
            setOf(connectionIdWrapped),
            ConnectionAutoDisabledReason.TOO_MANY_FAILED_JOBS_WITH_NO_RECENT_SUCCESS,
          )
        }
        verify { jobNotifier.autoDisableConnection(failedJob, any()) }
      }

      @Test
      fun `should send warning if warning thresholds are met`() {
        val priorFailedJob =
          mockJob(
            id = jobId1,
            status = JobStatus.FAILED,
            createdAt = timestamp.minus(Duration.ofDays(20)).epochSecond,
          )
        val failedJob =
          mockJob(
            id = jobId2,
            status = JobStatus.FAILED,
            createdAt = timestamp.epochSecond,
          )
        val sync =
          mockk<StandardSync>(relaxed = true).also {
            every { it.status } returns StandardSync.Status.ACTIVE
          }

        every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.of(priorFailedJob)
        every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.of(failedJob)
        every { jobService.getPriorJobWithStatusForScopeAndJobId(connectionId.toString(), jobId2, JobStatus.FAILED) } returns priorFailedJob
        every { jobService.countFailedJobsSinceLastSuccessForScope(connectionId.toString()) } returns maxJobsBeforeWarning
        every { jobService.lastSuccessfulJobForScope(connectionId.toString()) } returns null
        every { connectionRepository.getStandardSync(connectionId) } returns sync
        every { jobNotifier.autoDisableConnectionWarning(any(), any()) } just Runs
        every { jobPersistence.getAttemptStats(any(), any()) } returns mockk()

        helper.warnOrDisable(connectionService, connectionIdWrapped, timestamp).shouldBeFalse()

        verify { jobNotifier.autoDisableConnectionWarning(failedJob, any()) }
      }

      @Test
      fun `should not send a warning if days threshold is not met`() {
        val priorFailedJob =
          mockJob(
            id = jobId1,
            status = JobStatus.FAILED,
            // not long enough ago
            createdAt = timestamp.minus(Duration.ofDays(14)).epochSecond,
          )
        val failedJob =
          mockJob(
            id = jobId2,
            status = JobStatus.FAILED,
            createdAt = timestamp.epochSecond,
          )
        val sync =
          mockk<StandardSync>(relaxed = true).also {
            every { it.status } returns StandardSync.Status.ACTIVE
          }

        // meets failure count warning threshold
        every { jobService.countFailedJobsSinceLastSuccessForScope(connectionId.toString()) } returns maxJobsBeforeWarning

        every { jobService.getPriorJobWithStatusForScopeAndJobId(connectionId.toString(), jobId2, JobStatus.FAILED) } returns priorFailedJob
        every { jobService.lastSuccessfulJobForScope(connectionId.toString()) } returns null
        every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.of(priorFailedJob)
        every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.of(failedJob)
        every { connectionRepository.getStandardSync(connectionId) } returns sync
        every { jobNotifier.autoDisableConnectionWarning(any(), any()) } just Runs
        every { jobPersistence.getAttemptStats(any(), any()) } returns mockk()

        helper.warnOrDisable(connectionService, connectionIdWrapped, timestamp).shouldBeFalse()

        verify(exactly = 0) { jobNotifier.autoDisableConnectionWarning(failedJob, any()) }
      }

      @Test
      fun `should not disable connection if days threshold is not met`() {
        val failedJob =
          mockJob(
            id = jobId1,
            status = JobStatus.FAILED,
            // not long enough ago
            createdAt = timestamp.minus(Duration.ofDays(29)).epochSecond,
          )
        val sync =
          mockk<StandardSync>(relaxed = true).also {
            every { it.status } returns StandardSync.Status.ACTIVE
            every { it.connectionId } returns connectionId
          }

        // meets failure count disable threshold
        every { jobService.countFailedJobsSinceLastSuccessForScope(connectionId.toString()) } returns maxJobsBeforeDisable

        every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.of(failedJob)
        every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.of(failedJob)
        every { jobService.getPriorJobWithStatusForScopeAndJobId(connectionId.toString(), jobId1, JobStatus.FAILED) } returns null
        every { jobService.lastSuccessfulJobForScope(connectionId.toString()) } returns null
        every { connectionRepository.getStandardSync(connectionId) } returns sync

        helper.warnOrDisable(connectionService, connectionIdWrapped, timestamp).shouldBeFalse()

        verify(exactly = 0) { connectionRepository.writeStandardSync(any()) }
        verify(exactly = 0) { jobNotifier.autoDisableConnection(any(), any()) }
      }

      @Test
      fun `should not disable connection if failure count threshold is not met`() {
        val failedJob =
          mockJob(
            id = jobId1,
            status = JobStatus.FAILED,
            // meets days threshold
            createdAt = timestamp.minus(Duration.ofDays(30)).epochSecond,
          )
        val sync =
          mockk<StandardSync>(relaxed = true).also {
            every { it.status } returns StandardSync.Status.ACTIVE
            every { it.connectionId } returns connectionId
          }

        // does not meet failure count disable threshold
        every { jobService.countFailedJobsSinceLastSuccessForScope(connectionId.toString()) } returns maxJobsBeforeDisable - 1

        every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.of(failedJob)
        every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.of(failedJob)
        every { jobService.getPriorJobWithStatusForScopeAndJobId(connectionId.toString(), jobId1, JobStatus.FAILED) } returns null
        every { jobService.lastSuccessfulJobForScope(connectionId.toString()) } returns null
        every { connectionRepository.getStandardSync(connectionId) } returns sync

        helper.warnOrDisable(connectionService, connectionIdWrapped, timestamp).shouldBeFalse()

        verify(exactly = 0) { connectionRepository.writeStandardSync(any()) }
        verify(exactly = 0) { jobNotifier.autoDisableConnection(any(), any()) }
      }

      @Test
      fun `should only send warning if no prior warning`() {
        val firstJob =
          mockJob(
            id = jobId1,
            status = JobStatus.FAILED,
            createdAt = timestamp.minus(Duration.ofDays(20)).epochSecond,
          )

        // 16 days after first job, should have triggered warning
        val priorJob =
          mockJob(
            id = jobId2,
            status = JobStatus.FAILED,
            createdAt = timestamp.minus(Duration.ofDays(4)).epochSecond,
          )

        // 20 days after first failure, but prior job already triggered warning
        val mostRecentJob =
          mockJob(
            id = jobId3,
            status = JobStatus.FAILED,
            createdAt = timestamp.epochSecond,
          )

        val sync =
          mockk<StandardSync>(relaxed = true).also {
            every { it.status } returns StandardSync.Status.ACTIVE
          }

        // the prior job triggered the warning so most recent job is one above the warning threshold
        every { jobService.countFailedJobsSinceLastSuccessForScope(connectionId.toString()) } returns maxJobsBeforeWarning + 1

        every { jobService.getPriorJobWithStatusForScopeAndJobId(connectionId.toString(), jobId3, JobStatus.FAILED) } returns priorJob
        every { jobService.lastSuccessfulJobForScope(connectionId.toString()) } returns null
        every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.of(firstJob)
        every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.of(mostRecentJob)
        every { connectionRepository.getStandardSync(connectionId) } returns sync

        helper.warnOrDisable(connectionService, connectionIdWrapped, timestamp).shouldBeFalse()

        verify(exactly = 0) { jobNotifier.autoDisableConnectionWarning(any(), any()) }
      }

      @Test
      fun `should return false if connection is already inactive`() {
        val failedJob = mockJob(id = jobId1, status = JobStatus.FAILED)
        val sync =
          mockk<StandardSync>(relaxed = true) {
            every { status } returns StandardSync.Status.INACTIVE
          }

        every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.of(failedJob)
        every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.of(failedJob)
        every { connectionRepository.getStandardSync(connectionId) } returns sync
        every { jobService.getPriorJobWithStatusForScopeAndJobId(connectionId.toString(), 1L, JobStatus.FAILED) } returns null
        every { jobService.lastSuccessfulJobForScope(connectionId.toString()) } returns null

        helper.warnOrDisable(connectionService, connectionIdWrapped, timestamp).shouldBeFalse()

        verify(exactly = 0) { connectionRepository.writeStandardSync(any()) }
      }
    }
  }
}
