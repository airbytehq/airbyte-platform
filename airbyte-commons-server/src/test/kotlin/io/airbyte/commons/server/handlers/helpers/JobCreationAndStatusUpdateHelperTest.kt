/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptStatus
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.ReleaseStage
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.domain.services.dataworker.DataWorkerUsageService
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.tracker.JobTracker
import io.mockk.Runs
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.UUID

/**
 * Unit tests for [JobCreationAndStatusUpdateHelper].
 */
internal class JobCreationAndStatusUpdateHelperTest {
  lateinit var mActorDefinitionService: ActorDefinitionService
  lateinit var mConnectionService: ConnectionService
  lateinit var mJobNotifier: JobNotifier

  lateinit var mJobPersistence: JobPersistence

  lateinit var mJobTracker: JobTracker
  lateinit var dataWorkerUsageService: DataWorkerUsageService

  lateinit var helper: JobCreationAndStatusUpdateHelper
  lateinit var connectionTimelineEventHelper: ConnectionTimelineEventHelper
  lateinit var metricClient: MetricClient

  @BeforeEach
  fun setup() {
    mActorDefinitionService = mockk<ActorDefinitionService>()
    mConnectionService = mockk<ConnectionService>()
    mJobNotifier = mockk<JobNotifier>()
    mJobPersistence = mockk<JobPersistence>()
    mJobTracker = mockk<JobTracker>()
    dataWorkerUsageService = mockk<DataWorkerUsageService>()
    connectionTimelineEventHelper = mockk<ConnectionTimelineEventHelper>()
    metricClient = mockk<MetricClient>(relaxed = true)

    helper =
      JobCreationAndStatusUpdateHelper(
        mJobPersistence,
        mActorDefinitionService,
        mConnectionService,
        mJobNotifier,
        mJobTracker,
        connectionTimelineEventHelper,
        metricClient,
        dataWorkerUsageService,
      )
  }

  @Test
  fun findPreviousJob() {
    val jobs =
      listOf<Job>(
        Fixtures.job(1, 50),
        Fixtures.job(2, 20),
        Fixtures.job(3, 10),
        Fixtures.job(4, 60),
        Fixtures.job(5, 70),
        Fixtures.job(6, 80),
      )

    val result1 = helper.findPreviousJob(jobs, 1)
    assertTrue(result1.isPresent)
    assertEquals(2, result1.get().id)
    val result2 = helper.findPreviousJob(jobs, 2)
    assertTrue(result2.isPresent)
    assertEquals(3, result2.get().id)
    val result3 = helper.findPreviousJob(jobs, 3)
    assertTrue(result3.isEmpty)
    val result4 = helper.findPreviousJob(jobs, 4)
    assertTrue(result4.isPresent)
    assertEquals(1, result4.get().id)
    val result5 = helper.findPreviousJob(jobs, 5)
    assertTrue(result5.isPresent)
    assertEquals(4, result5.get().id)
    val result6 = helper.findPreviousJob(jobs, 6)
    assertTrue(result6.isPresent)
    assertEquals(5, result6.get().id)
    val result7 = helper.findPreviousJob(jobs, 7)
    assertTrue(result7.isEmpty)
    val result8 = helper.findPreviousJob(jobs, 8)
    assertTrue(result8.isEmpty)
    val result9 = helper.findPreviousJob(mutableListOf(), 1)
    assertTrue(result9.isEmpty)
  }

  @Test
  fun didJobSucceed() {
    val job1 = Fixtures.job(JobStatus.PENDING)
    val job2 = Fixtures.job(JobStatus.RUNNING)
    val job3 = Fixtures.job(JobStatus.INCOMPLETE)
    val job4 = Fixtures.job(JobStatus.FAILED)
    val job5 = Fixtures.job(JobStatus.SUCCEEDED)
    val job6 = Fixtures.job(JobStatus.CANCELLED)

    assertFalse(helper.didJobSucceed(job1))
    assertFalse(helper.didJobSucceed(job2))
    assertFalse(helper.didJobSucceed(job3))
    assertFalse(helper.didJobSucceed(job4))
    assertTrue(helper.didJobSucceed(job5))
    assertFalse(helper.didJobSucceed(job6))
  }

  @Test
  fun failNonTerminalJobs() {
    val jobId1 = 1
    val jobId2 = 2
    val attemptNo1 = 0
    val attemptNo2 = 1

    val failedAttempt = Fixtures.attempt(attemptNo1, jobId1.toLong(), AttemptStatus.FAILED)
    val runningAttempt = Fixtures.attempt(attemptNo2, jobId1.toLong(), AttemptStatus.RUNNING)

    val runningJob = Fixtures.job(jobId1.toLong(), mutableListOf(failedAttempt, runningAttempt), JobStatus.RUNNING)
    val pendingJob = Fixtures.job(jobId2.toLong(), mutableListOf(), JobStatus.PENDING)

    every {
      mJobPersistence.listJobsForConnectionWithStatuses(
        Fixtures.CONNECTION_ID,
        Job.REPLICATION_TYPES,
        JobStatus.NON_TERMINAL_STATUSES,
      )
    } returns listOf(runningJob, pendingJob)
    every { mJobPersistence.getJob(runningJob.id) } returns runningJob
    every { mJobPersistence.getJob(pendingJob.id) } returns pendingJob
    every { mConnectionService.getDataplaneGroupNameForConnection(any()) } returns "us-west"
    every { mActorDefinitionService.getActorDefinitionVersions(any()) } returns emptyList()
    every { mJobPersistence.failAttempt(any(), any()) } just Runs
    every { mJobPersistence.writeAttemptFailureSummary(any(), any(), any()) } just Runs
    every { mJobPersistence.failJob(any()) } just Runs
    every { mJobPersistence.getAttemptStats(any(), any()) } returns mockk()
    every { dataWorkerUsageService.releaseReservedUsageForJob(any()) } just Runs
    every { mJobNotifier.failJob(any(), any()) } just Runs
    every { connectionTimelineEventHelper.logJobFailureEventInConnectionTimeline(any(), any(), any()) } just Runs
    every { mJobTracker.trackSync(any(), any()) } just Runs

    helper.failNonTerminalJobs(Fixtures.CONNECTION_ID)

    verify { mJobPersistence.failJob(runningJob.id) }
    verify { mJobPersistence.failJob(pendingJob.id) }
    verify { mJobPersistence.getAttemptStats(runningJob.id, attemptNo1) }
    verify { mJobPersistence.getAttemptStats(runningJob.id, attemptNo2) }
    verify { mJobPersistence.failAttempt(runningJob.id, attemptNo2) }
    verify {
      mJobPersistence.writeAttemptFailureSummary(
        runningJob.id,
        attemptNo2,
        any(),
      )
    }
    verify { mJobPersistence.getJob(runningJob.id) }
    verify { mJobPersistence.getJob(pendingJob.id) }
    verify { dataWorkerUsageService.releaseReservedUsageForJob(runningJob.id) }
    verify { dataWorkerUsageService.releaseReservedUsageForJob(pendingJob.id) }
    verify { mJobNotifier.failJob(runningJob, any()) }
    verify { mJobNotifier.failJob(pendingJob, any()) }
    verify { mJobTracker.trackSync(runningJob, JobTracker.JobState.FAILED) }
    verify { mJobTracker.trackSync(pendingJob, JobTracker.JobState.FAILED) }
    verify { mJobPersistence.listJobsForConnectionWithStatuses(Fixtures.CONNECTION_ID, Job.REPLICATION_TYPES, JobStatus.NON_TERMINAL_STATUSES) }
    confirmVerified(mJobPersistence, mJobNotifier, mJobTracker, dataWorkerUsageService)
  }

  @Test
  fun testReportJobStart() {
    val jobId = 5L
    val job = mockk<Job>()
    every { mJobPersistence.getJob(jobId) } returns job
    every { mJobTracker.trackSync(any(), any()) } just Runs

    helper.reportJobStart(jobId)
    verify { mJobTracker.trackSync(job, JobTracker.JobState.STARTED) }
  }

  @Test
  fun testSetJobQueued() {
    val queuedJob = Fixtures.job(Fixtures.JOB_ID, mutableListOf(), JobStatus.QUEUED)
    every { mJobPersistence.queueJob(Fixtures.JOB_ID) } returns true
    every { mJobPersistence.getJob(Fixtures.JOB_ID) } returns queuedJob
    every { mJobNotifier.queuedJob(any()) } just Runs

    helper.setJobQueued(Fixtures.JOB_ID)

    verify { mJobPersistence.queueJob(Fixtures.JOB_ID) }
    verify { mJobPersistence.getJob(Fixtures.JOB_ID) }
    verify { mJobNotifier.queuedJob(queuedJob) }
  }

  @Test
  fun testSetJobQueuedSkipsNotificationForExistingQueuedJob() {
    every { mJobPersistence.queueJob(Fixtures.JOB_ID) } returns false

    helper.setJobQueued(Fixtures.JOB_ID)

    verify { mJobPersistence.queueJob(Fixtures.JOB_ID) }
    verify(exactly = 0) { mJobPersistence.getJob(Fixtures.JOB_ID) }
    verify(exactly = 0) { mJobNotifier.queuedJob(any()) }
  }

  @Test
  fun testCancelQueuedJobReleasesReservedUsage() {
    every { mJobPersistence.cancelQueuedJob(Fixtures.JOB_ID) } returns true

    helper.cancelQueuedJob(Fixtures.JOB_ID)

    verify { mJobPersistence.cancelQueuedJob(Fixtures.JOB_ID) }
    verify { dataWorkerUsageService.releaseReservedUsageForJob(Fixtures.JOB_ID) }
  }

  @Test
  fun testCancelQueuedJobSkipsReleaseWhenJobWasNotQueued() {
    every { mJobPersistence.cancelQueuedJob(Fixtures.JOB_ID) } returns false

    helper.cancelQueuedJob(Fixtures.JOB_ID)

    verify { mJobPersistence.cancelQueuedJob(Fixtures.JOB_ID) }
    verify(exactly = 0) { dataWorkerUsageService.releaseReservedUsageForJob(Fixtures.JOB_ID) }
  }

  @Test
  fun testCancelQueuedJobSwallowsReleaseFailure() {
    every { mJobPersistence.cancelQueuedJob(Fixtures.JOB_ID) } returns true
    every { dataWorkerUsageService.releaseReservedUsageForJob(Fixtures.JOB_ID) } throws RuntimeException("boom")

    helper.cancelQueuedJob(Fixtures.JOB_ID)

    verify { dataWorkerUsageService.releaseReservedUsageForJob(Fixtures.JOB_ID) }
  }

  @Test
  fun testReleaseStageOrdering() {
    val input = listOf(ReleaseStage.ALPHA, ReleaseStage.CUSTOM, ReleaseStage.BETA, ReleaseStage.GENERALLY_AVAILABLE)
    val expected = listOf(ReleaseStage.CUSTOM, ReleaseStage.ALPHA, ReleaseStage.BETA, ReleaseStage.GENERALLY_AVAILABLE)

    org.assertj.core.api.Assertions
      .assertThat(JobCreationAndStatusUpdateHelper.Companion.orderByReleaseStageAsc(input))
      .containsExactlyElementsOf(expected)
  }

  @Test
  fun testGetSyncJobToReleaseStages() {
    val sourceDefVersionId = UUID.randomUUID()
    val destinationDefVersionId = UUID.randomUUID()
    val jobConfig =
      JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(
          JobSyncConfig()
            .withSourceDefinitionVersionId(sourceDefVersionId)
            .withDestinationDefinitionVersionId(destinationDefVersionId),
        )
    val job =
      Job(
        Fixtures.JOB_ID,
        ConfigType.SYNC,
        Fixtures.CONNECTION_ID.toString(),
        jobConfig,
        mutableListOf(),
        JobStatus.PENDING,
        0L,
        0L,
        0L,
        true,
      )

    every {
      mActorDefinitionService.getActorDefinitionVersions(
        listOf(
          destinationDefVersionId,
          sourceDefVersionId,
        ),
      )
    } returns
      listOf(
        ActorDefinitionVersion().withReleaseStage(ReleaseStage.ALPHA),
        ActorDefinitionVersion().withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE),
      )

    val releaseStages = helper.getJobToReleaseStages(job)

    org.assertj.core.api.Assertions
      .assertThat(releaseStages)
      .contains(ReleaseStage.ALPHA, ReleaseStage.GENERALLY_AVAILABLE)
  }

  @Test
  fun testGetResetJobToReleaseStages() {
    val destinationDefVersionId = UUID.randomUUID()
    val jobConfig =
      JobConfig()
        .withConfigType(ConfigType.RESET_CONNECTION)
        .withResetConnection(
          JobResetConnectionConfig()
            .withDestinationDefinitionVersionId(destinationDefVersionId),
        )
    val job =
      Job(
        Fixtures.JOB_ID,
        ConfigType.RESET_CONNECTION,
        Fixtures.CONNECTION_ID.toString(),
        jobConfig,
        mutableListOf(),
        JobStatus.PENDING,
        0L,
        0L,
        0L,
        true,
      )

    every {
      mActorDefinitionService.getActorDefinitionVersions(
        listOf(
          destinationDefVersionId,
        ),
      )
    } returns
      listOf(
        ActorDefinitionVersion().withReleaseStage(ReleaseStage.ALPHA),
      )
    val releaseStages = helper.getJobToReleaseStages(job)

    org.assertj.core.api.Assertions
      .assertThat(releaseStages)
      .contains(ReleaseStage.ALPHA)
  }

  internal object Fixtures {
    val CONNECTION_ID: UUID = UUID.randomUUID()

    const val JOB_ID = 123L

    private val JOB_CONFIG: JobConfig =
      JobConfig().withConfigType(ConfigType.SYNC).withSync(
        JobSyncConfig()
          .withSourceDockerImage("sourceDockerImage")
          .withDestinationDockerImage("destinationDockerImage")
          .withWorkspaceId(UUID.randomUUID())
          .withSourceDefinitionVersionId(UUID.randomUUID())
          .withDestinationDefinitionVersionId(UUID.randomUUID()),
      )

    fun job(
      id: Long,
      createdAt: Long,
    ): Job =
      Job(
        id,
        ConfigType.SYNC,
        CONNECTION_ID.toString(),
        JOB_CONFIG,
        mutableListOf(),
        JobStatus.PENDING,
        null,
        createdAt,
        0,
        false,
      )

    fun job(status: JobStatus): Job = Job(1, ConfigType.SYNC, CONNECTION_ID.toString(), JOB_CONFIG, mutableListOf(), status, null, 0, 0, true)

    fun job(
      id: Long,
      attempts: MutableList<Attempt>,
      status: JobStatus,
    ): Job = Job(id, ConfigType.SYNC, CONNECTION_ID.toString(), JOB_CONFIG, attempts, status, null, 0, 0, true)

    fun attempt(
      number: Int,
      jobId: Long,
      status: AttemptStatus,
    ): Attempt = Attempt(number, jobId, Path.of(""), null, null, status, null, null, 4L, 5L, null)
  }
}
