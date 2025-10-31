/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.models

import io.airbyte.config.Attempt
import io.airbyte.config.AttemptStatus
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobStatus
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.function.IntFunction
import java.util.stream.IntStream

internal class JobTest {
  @Test
  fun testIsJobInTerminalState() {
    assertFalse(jobWithStatus(JobStatus.PENDING).isJobInTerminalState())
    assertFalse(jobWithStatus(JobStatus.RUNNING).isJobInTerminalState())
    assertFalse(jobWithStatus(JobStatus.INCOMPLETE).isJobInTerminalState())
    assertTrue(jobWithStatus(JobStatus.FAILED).isJobInTerminalState())
    assertTrue(jobWithStatus(JobStatus.SUCCEEDED).isJobInTerminalState())
    assertTrue(jobWithStatus(JobStatus.CANCELLED).isJobInTerminalState())
  }

  @Test
  fun testHasRunningAttempt() {
    assertTrue(jobWithAttemptWithStatus(AttemptStatus.RUNNING).hasRunningAttempt())
    assertFalse(jobWithAttemptWithStatus(AttemptStatus.FAILED).hasRunningAttempt())
    assertFalse(jobWithAttemptWithStatus(AttemptStatus.SUCCEEDED).hasRunningAttempt())
    assertFalse(jobWithAttemptWithStatus().hasRunningAttempt())
    assertTrue(jobWithAttemptWithStatus(AttemptStatus.SUCCEEDED, AttemptStatus.RUNNING).hasRunningAttempt())
  }

  @Test
  fun testGetSuccessfulAttempt() {
    assertTrue(jobWithAttemptWithStatus().getSuccessfulAttempt().isEmpty)
    assertTrue(jobWithAttemptWithStatus(AttemptStatus.FAILED).getSuccessfulAttempt().isEmpty)
    assertThrows(
      IllegalStateException::class.java,
      { jobWithAttemptWithStatus(AttemptStatus.SUCCEEDED, AttemptStatus.SUCCEEDED).getSuccessfulAttempt() },
    )

    val job: Job = jobWithAttemptWithStatus(AttemptStatus.FAILED, AttemptStatus.SUCCEEDED)
    assertTrue(job.getSuccessfulAttempt().isPresent)
    assertEquals(job.attempts[1], job.getSuccessfulAttempt().get())
  }

  @Test
  fun testGetLastFailedAttempt() {
    assertTrue(jobWithAttemptWithStatus().getLastFailedAttempt().isEmpty)
    assertTrue(jobWithAttemptWithStatus(AttemptStatus.SUCCEEDED).getLastFailedAttempt().isEmpty)
    assertTrue(jobWithAttemptWithStatus(AttemptStatus.FAILED).getLastFailedAttempt().isPresent)

    val job: Job = jobWithAttemptWithStatus(AttemptStatus.FAILED, AttemptStatus.FAILED)
    assertTrue(job.getLastFailedAttempt().isPresent)
    assertEquals(2, job.getLastFailedAttempt().get().attemptNumber)
  }

  @Test
  fun testGetLastAttempt() {
    val job: Job = jobWithAttemptWithStatus(AttemptStatus.FAILED, AttemptStatus.FAILED, AttemptStatus.SUCCEEDED)
    assertTrue(job.getLastAttempt().isPresent)
    assertEquals(3, job.getLastAttempt().get().attemptNumber)
  }

  @Test
  fun testGetAttemptByNumber() {
    val job: Job = jobWithAttemptWithStatus(AttemptStatus.FAILED, AttemptStatus.FAILED, AttemptStatus.SUCCEEDED)
    assertTrue(job.getAttemptByNumber(2).isPresent)
    assertEquals(2, job.getAttemptByNumber(2).get().attemptNumber)
  }

  @Test
  fun testValidateStatusTransitionFromPending() {
    val pendingJob: Job = jobWithStatus(JobStatus.PENDING)
    assertDoesNotThrow({ pendingJob.validateStatusTransition(JobStatus.RUNNING) })
    assertDoesNotThrow({ pendingJob.validateStatusTransition(JobStatus.FAILED) })
    assertDoesNotThrow({ pendingJob.validateStatusTransition(JobStatus.CANCELLED) })
    assertDoesNotThrow({ pendingJob.validateStatusTransition(JobStatus.INCOMPLETE) })
    assertThrows(
      IllegalStateException::class.java,
      { pendingJob.validateStatusTransition(JobStatus.SUCCEEDED) },
    )
  }

  @Test
  fun testValidateStatusTransitionFromRunning() {
    val runningJob: Job = jobWithStatus(JobStatus.RUNNING)
    assertDoesNotThrow({ runningJob.validateStatusTransition(JobStatus.INCOMPLETE) })
    assertDoesNotThrow({ runningJob.validateStatusTransition(JobStatus.SUCCEEDED) })
    assertDoesNotThrow({ runningJob.validateStatusTransition(JobStatus.FAILED) })
    assertDoesNotThrow({ runningJob.validateStatusTransition(JobStatus.CANCELLED) })
    assertThrows(
      IllegalStateException::class.java,
      { runningJob.validateStatusTransition(JobStatus.PENDING) },
    )
  }

  @Test
  fun testValidateStatusTransitionFromIncomplete() {
    val incompleteJob: Job = jobWithStatus(JobStatus.INCOMPLETE)
    assertDoesNotThrow({ incompleteJob.validateStatusTransition(JobStatus.PENDING) })
    assertDoesNotThrow({ incompleteJob.validateStatusTransition(JobStatus.RUNNING) })
    assertDoesNotThrow({ incompleteJob.validateStatusTransition(JobStatus.FAILED) })
    assertDoesNotThrow({ incompleteJob.validateStatusTransition(JobStatus.CANCELLED) })
    assertDoesNotThrow({ incompleteJob.validateStatusTransition(JobStatus.SUCCEEDED) })
  }

  @Test
  fun testValidateStatusTransitionFromSucceeded() {
    val suceededJob: Job = jobWithStatus(JobStatus.SUCCEEDED)
    assertThrows(
      IllegalStateException::class.java,
      { suceededJob.validateStatusTransition(JobStatus.PENDING) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { suceededJob.validateStatusTransition(JobStatus.RUNNING) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { suceededJob.validateStatusTransition(JobStatus.INCOMPLETE) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { suceededJob.validateStatusTransition(JobStatus.FAILED) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { suceededJob.validateStatusTransition(JobStatus.CANCELLED) },
    )
  }

  @Test
  fun testValidateStatusTransitionFromFailed() {
    val failedJob: Job = jobWithStatus(JobStatus.FAILED)
    assertThrows(
      IllegalStateException::class.java,
      { failedJob.validateStatusTransition(JobStatus.SUCCEEDED) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { failedJob.validateStatusTransition(JobStatus.PENDING) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { failedJob.validateStatusTransition(JobStatus.RUNNING) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { failedJob.validateStatusTransition(JobStatus.INCOMPLETE) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { failedJob.validateStatusTransition(JobStatus.CANCELLED) },
    )
  }

  @Test
  fun testValidateStatusTransitionFromCancelled() {
    val cancelledJob: Job = jobWithStatus(JobStatus.CANCELLED)
    assertThrows(
      IllegalStateException::class.java,
      { cancelledJob.validateStatusTransition(JobStatus.SUCCEEDED) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { cancelledJob.validateStatusTransition(JobStatus.PENDING) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { cancelledJob.validateStatusTransition(JobStatus.RUNNING) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { cancelledJob.validateStatusTransition(JobStatus.INCOMPLETE) },
    )
    assertThrows(
      IllegalStateException::class.java,
      { cancelledJob.validateStatusTransition(JobStatus.CANCELLED) },
    )
  }

  companion object {
    private fun jobWithStatus(jobStatus: JobStatus): Job = Job(1L, ConfigType.SYNC, "", JobConfig(), mutableListOf(), jobStatus, 0L, 0L, 0L, true)

    private fun jobWithAttemptWithStatus(vararg attemptStatuses: AttemptStatus?): Job {
      val attempts =
        IntStream
          .range(0, attemptStatuses.size)
          .mapToObj(
            IntFunction { idx: Int ->
              Attempt(
                attemptNumber = idx + 1,
                jobId = 1L,
                logPath = null,
                syncConfig = null,
                output = null,
                status = attemptStatuses[idx]!!,
                processingTaskQueue = null,
                failureSummary = null,
                createdAtInSecond = idx.toLong(),
                updatedAtInSecond = 0L,
                endedAtInSecond = null,
              )
            },
          ).toList()
      return Job(1L, ConfigType.SYNC, "", JobConfig(), attempts, JobStatus.PENDING, 0L, 0L, 0L, true)
    }
  }
}
