/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.config.JobConfig.ConfigType.RESET_CONNECTION;
import static io.airbyte.config.JobConfig.ConfigType.SYNC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptStatus;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.ReleaseStage;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.airbyte.persistence.job.tracker.JobTracker.JobState;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JobCreationAndStatusUpdateHelper}.
 */
class JobCreationAndStatusUpdateHelperTest {

  ActorDefinitionService mActorDefinitionService;
  ConnectionService mConnectionService;
  JobNotifier mJobNotifier;

  JobPersistence mJobPersistence;

  JobTracker mJobTracker;

  JobCreationAndStatusUpdateHelper helper;
  ConnectionTimelineEventHelper connectionTimelineEventHelper;

  @BeforeEach
  void setup() {
    mActorDefinitionService = mock(ActorDefinitionService.class);
    mConnectionService = mock(ConnectionService.class);
    mJobNotifier = mock(JobNotifier.class);
    mJobPersistence = mock(JobPersistence.class);
    mJobTracker = mock(JobTracker.class);
    connectionTimelineEventHelper = mock(ConnectionTimelineEventHelper.class);

    helper = new JobCreationAndStatusUpdateHelper(
        mJobPersistence,
        mActorDefinitionService,
        mConnectionService,
        mJobNotifier,
        mJobTracker,
        connectionTimelineEventHelper);
  }

  @Test
  void findPreviousJob() {
    final List<Job> jobs = List.of(
        Fixtures.job(1, 50),
        Fixtures.job(2, 20),
        Fixtures.job(3, 10),
        Fixtures.job(4, 60),
        Fixtures.job(5, 70),
        Fixtures.job(6, 80));

    final var result1 = helper.findPreviousJob(jobs, 1);
    assertTrue(result1.isPresent());
    assertEquals(2, result1.get().getId());
    final var result2 = helper.findPreviousJob(jobs, 2);
    assertTrue(result2.isPresent());
    assertEquals(3, result2.get().getId());
    final var result3 = helper.findPreviousJob(jobs, 3);
    assertTrue(result3.isEmpty());
    final var result4 = helper.findPreviousJob(jobs, 4);
    assertTrue(result4.isPresent());
    assertEquals(1, result4.get().getId());
    final var result5 = helper.findPreviousJob(jobs, 5);
    assertTrue(result5.isPresent());
    assertEquals(4, result5.get().getId());
    final var result6 = helper.findPreviousJob(jobs, 6);
    assertTrue(result6.isPresent());
    assertEquals(5, result6.get().getId());
    final var result7 = helper.findPreviousJob(jobs, 7);
    assertTrue(result7.isEmpty());
    final var result8 = helper.findPreviousJob(jobs, 8);
    assertTrue(result8.isEmpty());
    final var result9 = helper.findPreviousJob(List.of(), 1);
    assertTrue(result9.isEmpty());
  }

  @Test
  void didJobSucceed() {
    final var job1 = Fixtures.job(JobStatus.PENDING);
    final var job2 = Fixtures.job(JobStatus.RUNNING);
    final var job3 = Fixtures.job(JobStatus.INCOMPLETE);
    final var job4 = Fixtures.job(JobStatus.FAILED);
    final var job5 = Fixtures.job(JobStatus.SUCCEEDED);
    final var job6 = Fixtures.job(JobStatus.CANCELLED);

    assertFalse(helper.didJobSucceed(job1));
    assertFalse(helper.didJobSucceed(job2));
    assertFalse(helper.didJobSucceed(job3));
    assertFalse(helper.didJobSucceed(job4));
    assertTrue(helper.didJobSucceed(job5));
    assertFalse(helper.didJobSucceed(job6));
  }

  @Test
  void failNonTerminalJobs() throws IOException {
    final var jobId1 = 1;
    final var jobId2 = 2;
    final var attemptNo1 = 0;
    final var attemptNo2 = 1;

    final Attempt failedAttempt = Fixtures.attempt(attemptNo1, jobId1, AttemptStatus.FAILED);
    final Attempt runningAttempt = Fixtures.attempt(attemptNo2, jobId1, AttemptStatus.RUNNING);

    final Job runningJob = Fixtures.job(jobId1, List.of(failedAttempt, runningAttempt), JobStatus.RUNNING);
    final Job pendingJob = Fixtures.job(jobId2, List.of(), JobStatus.PENDING);

    when(mJobPersistence.listJobsForConnectionWithStatuses(Fixtures.CONNECTION_ID, Job.REPLICATION_TYPES, JobStatus.NON_TERMINAL_STATUSES))
        .thenReturn(List.of(runningJob, pendingJob));
    when(mJobPersistence.getJob(runningJob.getId())).thenReturn(runningJob);
    when(mJobPersistence.getJob(pendingJob.getId())).thenReturn(pendingJob);

    helper.failNonTerminalJobs(Fixtures.CONNECTION_ID);

    verify(mJobPersistence).failJob(runningJob.getId());
    verify(mJobPersistence).failJob(pendingJob.getId());
    verify(mJobPersistence).getAttemptStats(runningJob.getId(), attemptNo1);
    verify(mJobPersistence).getAttemptStats(runningJob.getId(), attemptNo2);
    verify(mJobPersistence).failAttempt(runningJob.getId(), attemptNo2);
    verify(mJobPersistence).writeAttemptFailureSummary(eq(runningJob.getId()), eq(attemptNo2), any());
    verify(mJobPersistence).getJob(runningJob.getId());
    verify(mJobPersistence).getJob(pendingJob.getId());
    verify(mJobNotifier).failJob(eq(runningJob), any());
    verify(mJobNotifier).failJob(eq(pendingJob), any());
    verify(mJobTracker).trackSync(runningJob, JobState.FAILED);
    verify(mJobTracker).trackSync(pendingJob, JobState.FAILED);
    verify(mJobPersistence).listJobsForConnectionWithStatuses(Fixtures.CONNECTION_ID, Job.REPLICATION_TYPES, JobStatus.NON_TERMINAL_STATUSES);
    verifyNoMoreInteractions(mJobPersistence, mJobNotifier, mJobTracker);
  }

  @Test
  void testReportJobStart() throws IOException {
    final Long jobId = 5L;
    final Job job = mock(Job.class);
    when(mJobPersistence.getJob(jobId)).thenReturn(job);

    helper.reportJobStart(jobId);
    verify(mJobTracker).trackSync(job, JobState.STARTED);
  }

  @Test
  void testReleaseStageOrdering() {
    final List<ReleaseStage> input = List.of(ReleaseStage.ALPHA, ReleaseStage.CUSTOM, ReleaseStage.BETA, ReleaseStage.GENERALLY_AVAILABLE);
    final List<ReleaseStage> expected = List.of(ReleaseStage.CUSTOM, ReleaseStage.ALPHA, ReleaseStage.BETA, ReleaseStage.GENERALLY_AVAILABLE);

    Assertions.assertThat(helper.orderByReleaseStageAsc(input))
        .containsExactlyElementsOf(expected);
  }

  @Test
  void testGetSyncJobToReleaseStages() throws IOException {
    final UUID sourceDefVersionId = UUID.randomUUID();
    final UUID destinationDefVersionId = UUID.randomUUID();
    final JobConfig jobConfig = new JobConfig()
        .withConfigType(SYNC)
        .withSync(new JobSyncConfig()
            .withSourceDefinitionVersionId(sourceDefVersionId)
            .withDestinationDefinitionVersionId(destinationDefVersionId));
    final Job job = new Job(Fixtures.JOB_ID, ConfigType.SYNC, Fixtures.CONNECTION_ID.toString(), jobConfig, List.of(), JobStatus.PENDING,
        0L, 0L, 0L);

    when(mActorDefinitionService.getActorDefinitionVersions(List.of(destinationDefVersionId, sourceDefVersionId)))
        .thenReturn(List.of(
            new ActorDefinitionVersion().withReleaseStage(ReleaseStage.ALPHA),
            new ActorDefinitionVersion().withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)));

    final List<ReleaseStage> releaseStages = helper.getJobToReleaseStages(job);

    Assertions.assertThat(releaseStages).contains(ReleaseStage.ALPHA, ReleaseStage.GENERALLY_AVAILABLE);
  }

  @Test
  void testGetResetJobToReleaseStages() throws IOException {
    final UUID destinationDefVersionId = UUID.randomUUID();
    final JobConfig jobConfig = new JobConfig()
        .withConfigType(RESET_CONNECTION)
        .withResetConnection(new JobResetConnectionConfig()
            .withDestinationDefinitionVersionId(destinationDefVersionId));
    final Job job = new Job(Fixtures.JOB_ID, RESET_CONNECTION, Fixtures.CONNECTION_ID.toString(), jobConfig, List.of(), JobStatus.PENDING,
        0L, 0L, 0L);

    when(mActorDefinitionService.getActorDefinitionVersions(List.of(destinationDefVersionId)))
        .thenReturn(List.of(
            new ActorDefinitionVersion().withReleaseStage(ReleaseStage.ALPHA)));
    final List<ReleaseStage> releaseStages = helper.getJobToReleaseStages(job);

    Assertions.assertThat(releaseStages).contains(ReleaseStage.ALPHA);
  }

  static class Fixtures {

    static final UUID CONNECTION_ID = UUID.randomUUID();

    private static final long JOB_ID = 123L;

    static Job job(final long id, final long createdAt) {
      return new Job(id, null, null, null, null, null, null, createdAt, 0);
    }

    static Job job(final JobStatus status) {
      return new Job(1, null, null, null, null, status, null, 0, 0);
    }

    static Job job(final long id, final List<Attempt> attempts, final JobStatus status) {
      return new Job(id, null, null, null, attempts, status, null, 0, 0);
    }

    static Attempt attempt(final int number, final long jobId, final AttemptStatus status) {
      return new Attempt(number, jobId, Path.of(""), null, null, status, null, null, 4L, 5L, null);
    }

  }

}
