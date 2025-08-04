/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.tracker.JobTracker
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import java.io.IOException
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

  lateinit var helper: JobCreationAndStatusUpdateHelper
  lateinit var connectionTimelineEventHelper: ConnectionTimelineEventHelper
  lateinit var metricClient: MetricClient

  @BeforeEach
  fun setup() {
    mActorDefinitionService = Mockito.mock(ActorDefinitionService::class.java)
    mConnectionService = Mockito.mock(ConnectionService::class.java)
    mJobNotifier = Mockito.mock(JobNotifier::class.java)
    mJobPersistence = Mockito.mock(JobPersistence::class.java)
    mJobTracker = Mockito.mock(JobTracker::class.java)
    connectionTimelineEventHelper = Mockito.mock(ConnectionTimelineEventHelper::class.java)
    metricClient = Mockito.mock(MetricClient::class.java)

    helper =
      JobCreationAndStatusUpdateHelper(
        mJobPersistence,
        mActorDefinitionService,
        mConnectionService,
        mJobNotifier,
        mJobTracker,
        connectionTimelineEventHelper,
        metricClient,
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
    Assertions.assertTrue(result1.isPresent())
    Assertions.assertEquals(2, result1.get().id)
    val result2 = helper.findPreviousJob(jobs, 2)
    Assertions.assertTrue(result2.isPresent())
    Assertions.assertEquals(3, result2.get().id)
    val result3 = helper.findPreviousJob(jobs, 3)
    Assertions.assertTrue(result3.isEmpty())
    val result4 = helper.findPreviousJob(jobs, 4)
    Assertions.assertTrue(result4.isPresent())
    Assertions.assertEquals(1, result4.get().id)
    val result5 = helper.findPreviousJob(jobs, 5)
    Assertions.assertTrue(result5.isPresent())
    Assertions.assertEquals(4, result5.get().id)
    val result6 = helper.findPreviousJob(jobs, 6)
    Assertions.assertTrue(result6.isPresent())
    Assertions.assertEquals(5, result6.get().id)
    val result7 = helper.findPreviousJob(jobs, 7)
    Assertions.assertTrue(result7.isEmpty())
    val result8 = helper.findPreviousJob(jobs, 8)
    Assertions.assertTrue(result8.isEmpty())
    val result9 = helper.findPreviousJob(mutableListOf(), 1)
    Assertions.assertTrue(result9.isEmpty())
  }

  @Test
  fun didJobSucceed() {
    val job1 = Fixtures.job(JobStatus.PENDING)
    val job2 = Fixtures.job(JobStatus.RUNNING)
    val job3 = Fixtures.job(JobStatus.INCOMPLETE)
    val job4 = Fixtures.job(JobStatus.FAILED)
    val job5 = Fixtures.job(JobStatus.SUCCEEDED)
    val job6 = Fixtures.job(JobStatus.CANCELLED)

    Assertions.assertFalse(helper.didJobSucceed(job1))
    Assertions.assertFalse(helper.didJobSucceed(job2))
    Assertions.assertFalse(helper.didJobSucceed(job3))
    Assertions.assertFalse(helper.didJobSucceed(job4))
    Assertions.assertTrue(helper.didJobSucceed(job5))
    Assertions.assertFalse(helper.didJobSucceed(job6))
  }

  @Test
  @Throws(IOException::class)
  fun failNonTerminalJobs() {
    val jobId1 = 1
    val jobId2 = 2
    val attemptNo1 = 0
    val attemptNo2 = 1

    val failedAttempt = Fixtures.attempt(attemptNo1, jobId1.toLong(), AttemptStatus.FAILED)
    val runningAttempt = Fixtures.attempt(attemptNo2, jobId1.toLong(), AttemptStatus.RUNNING)

    val runningJob = Fixtures.job(jobId1.toLong(), mutableListOf(failedAttempt, runningAttempt), JobStatus.RUNNING)
    val pendingJob = Fixtures.job(jobId2.toLong(), mutableListOf(), JobStatus.PENDING)

    Mockito
      .`when`(
        mJobPersistence.listJobsForConnectionWithStatuses(
          Fixtures.CONNECTION_ID,
          Job.REPLICATION_TYPES,
          JobStatus.NON_TERMINAL_STATUSES,
        ),
      ).thenReturn(listOf(runningJob, pendingJob))
    Mockito.`when`(mJobPersistence.getJob(runningJob.id)).thenReturn(runningJob)
    Mockito.`when`(mJobPersistence.getJob(pendingJob.id)).thenReturn(pendingJob)

    helper.failNonTerminalJobs(Fixtures.CONNECTION_ID)

    Mockito.verify(mJobPersistence).failJob(runningJob.id)
    Mockito.verify(mJobPersistence).failJob(pendingJob.id)
    Mockito.verify(mJobPersistence).getAttemptStats(runningJob.id, attemptNo1)
    Mockito.verify(mJobPersistence).getAttemptStats(runningJob.id, attemptNo2)
    Mockito.verify(mJobPersistence).failAttempt(runningJob.id, attemptNo2)
    Mockito.verify(mJobPersistence).writeAttemptFailureSummary(
      eq(runningJob.id),
      eq(attemptNo2),
      anyOrNull(),
    )
    Mockito.verify(mJobPersistence).getJob(runningJob.id)
    Mockito.verify(mJobPersistence).getJob(pendingJob.id)
    Mockito
      .verify(mJobNotifier)
      .failJob(eq(runningJob), anyOrNull())
    Mockito
      .verify(mJobNotifier)
      .failJob(eq(pendingJob), anyOrNull())
    Mockito.verify(mJobTracker).trackSync(runningJob, JobTracker.JobState.FAILED)
    Mockito.verify(mJobTracker).trackSync(pendingJob, JobTracker.JobState.FAILED)
    Mockito
      .verify(mJobPersistence)
      .listJobsForConnectionWithStatuses(Fixtures.CONNECTION_ID, Job.REPLICATION_TYPES, JobStatus.NON_TERMINAL_STATUSES)
    Mockito.verifyNoMoreInteractions(mJobPersistence, mJobNotifier, mJobTracker)
  }

  @Test
  @Throws(IOException::class)
  fun testReportJobStart() {
    val jobId = 5L
    val job = Mockito.mock(Job::class.java)
    Mockito.`when`(mJobPersistence.getJob(jobId)).thenReturn(job)

    helper.reportJobStart(jobId)
    Mockito.verify(mJobTracker).trackSync(job, JobTracker.JobState.STARTED)
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
  @Throws(IOException::class)
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

    Mockito
      .`when`(
        mActorDefinitionService.getActorDefinitionVersions(
          listOf(
            destinationDefVersionId,
            sourceDefVersionId,
          ),
        ),
      ).thenReturn(
        listOf(
          ActorDefinitionVersion().withReleaseStage(ReleaseStage.ALPHA),
          ActorDefinitionVersion().withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE),
        ),
      )

    val releaseStages = helper.getJobToReleaseStages(job)

    org.assertj.core.api.Assertions
      .assertThat(releaseStages)
      .contains(ReleaseStage.ALPHA, ReleaseStage.GENERALLY_AVAILABLE)
  }

  @Test
  @Throws(IOException::class)
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

    Mockito
      .`when`(
        mActorDefinitionService.getActorDefinitionVersions(
          listOf(
            destinationDefVersionId,
          ),
        ),
      ).thenReturn(
        listOf(
          ActorDefinitionVersion().withReleaseStage(ReleaseStage.ALPHA),
        ),
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
