/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.models.JobStatus;
import io.airbyte.persistence.job.tracker.JobTracker;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JobCreationAndStatusUpdateHelper}.
 */
class JobCreationAndStatusUpdateHelperTest {

  JobCreationAndStatusUpdateHelper helper;

  @BeforeEach
  void setup() {
    helper = new JobCreationAndStatusUpdateHelper(
        mock(JobPersistence.class),
        mock(ConfigRepository.class),
        mock(JobNotifier.class),
        mock(JobTracker.class));
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

  static class Fixtures {

    static Job job(final long id, final long createdAt) {
      return new Job(id, null, null, null, null, null, null, createdAt, 0);
    }

    static Job job(final JobStatus status) {
      return new Job(1, null, null, null, null, status, null, 0, 0);
    }

  }

}
