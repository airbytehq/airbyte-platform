package io.airbyte.commons.server.services

import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.config.Job
import io.airbyte.config.JobStatus
import io.airbyte.config.StandardSync
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.shared.ConnectionAutoDisabledReason
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.UUID

class AutoDisableConnectionServiceTest {
  private val connectionService: ConnectionService = mockk()
  private val jobService: JobService = mockk()
  private val jobPersistence: JobPersistence = mockk()
  private val jobNotifier: JobNotifier = mockk()
  private val connectionTimelineEventHelper: ConnectionTimelineEventHelper = mockk()

  private val maxDaysBeforeDisable = 30
  private val maxJobsBeforeDisable = 5
  private val maxDaysBeforeWarning = 15
  private val maxJobsBeforeWarning = 3

  private val jobId1 = 1L
  private val jobId2 = 2L
  private val jobId3 = 3L

  private val service =
    AutoDisableConnectionService(
      connectionService,
      jobService,
      jobPersistence,
      jobNotifier,
      maxDaysBeforeDisable,
      maxJobsBeforeDisable,
      maxDaysBeforeWarning,
      maxJobsBeforeWarning,
      connectionTimelineEventHelper,
    )

  private val connectionId = UUID.randomUUID()
  private val timestamp = Instant.now()

  private fun mockJob(
    id: Long,
    status: JobStatus,
    createdAt: Long = Instant.now().epochSecond,
  ): Job =
    mockk<Job>().also {
      every { it.id } returns id
      every { it.status } returns status
      every { it.createdAtInSecond } returns createdAt
      every { it.attempts } returns emptyList()
    }

  @BeforeEach
  fun setupMocks() {
    clearAllMocks()
  }

  @Nested
  inner class AutoDisableConnection {
    @Test
    fun `should return false if no replication jobs found`() {
      every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.empty()
      every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.empty()

      service.autoDisableConnection(connectionId, timestamp).shouldBeFalse()

      verify(exactly = 0) { connectionService.getStandardSync(any()) }
      verify(exactly = 0) { connectionService.writeStandardSync(any()) }
      verify(exactly = 0) { jobNotifier.autoDisableConnectionWarning(any(), any()) }
      verify(exactly = 0) { jobNotifier.autoDisableConnection(any(), any()) }
    }

    @Test
    fun `should return false if most recent job is not failed`() {
      val firstJob = mockJob(id = jobId1, status = JobStatus.SUCCEEDED)
      val lastJob = mockJob(id = jobId2, status = JobStatus.SUCCEEDED)

      every { jobPersistence.getFirstReplicationJob(connectionId) } returns Optional.of(firstJob)
      every { jobPersistence.getLastReplicationJob(connectionId) } returns Optional.of(lastJob)

      service.autoDisableConnection(connectionId, timestamp).shouldBeFalse()

      verify(exactly = 0) { connectionService.getStandardSync(any()) }
      verify(exactly = 0) { connectionService.writeStandardSync(any()) }
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
      every { connectionService.getStandardSync(connectionId) } returns sync
      every { connectionService.writeStandardSync(sync) } just Runs
      every {
        connectionTimelineEventHelper.logStatusChangedEventInConnectionTimeline(
          connectionId,
          ConnectionStatus.INACTIVE,
          ConnectionAutoDisabledReason.TOO_MANY_FAILED_JOBS_WITH_NO_RECENT_SUCCESS.name,
          true,
        )
      } just Runs
      every { jobNotifier.autoDisableConnection(any(), any()) } just Runs
      every { jobPersistence.getAttemptStats(any(), any()) } returns mockk()

      service.autoDisableConnection(connectionId, timestamp).shouldBeTrue()

      verify { sync.status = StandardSync.Status.INACTIVE }
      verify(exactly = 1) { connectionService.writeStandardSync(sync) }

      verify {
        connectionTimelineEventHelper.logStatusChangedEventInConnectionTimeline(
          connectionId,
          ConnectionStatus.INACTIVE,
          ConnectionAutoDisabledReason.TOO_MANY_FAILED_JOBS_WITH_NO_RECENT_SUCCESS.name,
          true,
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
      every { connectionService.getStandardSync(connectionId) } returns sync
      every { jobNotifier.autoDisableConnectionWarning(any(), any()) } just Runs
      every { jobPersistence.getAttemptStats(any(), any()) } returns mockk()

      service.autoDisableConnection(connectionId, timestamp).shouldBeFalse()

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
      every { connectionService.getStandardSync(connectionId) } returns sync
      every { jobNotifier.autoDisableConnectionWarning(any(), any()) } just Runs
      every { jobPersistence.getAttemptStats(any(), any()) } returns mockk()

      service.autoDisableConnection(connectionId, timestamp).shouldBeFalse()

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
      every { connectionService.getStandardSync(connectionId) } returns sync

      service.autoDisableConnection(connectionId, timestamp).shouldBeFalse()

      verify(exactly = 0) { connectionService.writeStandardSync(any()) }
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
      every { connectionService.getStandardSync(connectionId) } returns sync

      service.autoDisableConnection(connectionId, timestamp).shouldBeFalse()

      verify(exactly = 0) { connectionService.writeStandardSync(any()) }
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
      every { connectionService.getStandardSync(connectionId) } returns sync

      service.autoDisableConnection(connectionId, timestamp).shouldBeFalse()

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
      every { connectionService.getStandardSync(connectionId) } returns sync
      every { jobService.getPriorJobWithStatusForScopeAndJobId(connectionId.toString(), 1L, JobStatus.FAILED) } returns null
      every { jobService.lastSuccessfulJobForScope(connectionId.toString()) } returns null

      service.autoDisableConnection(connectionId, timestamp).shouldBeFalse()

      verify(exactly = 0) { connectionService.writeStandardSync(any()) }
    }
  }
}
