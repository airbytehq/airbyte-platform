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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.function.IntFunction
import java.util.stream.Collectors
import java.util.stream.IntStream

internal class JobTest {
  @Test
  fun testIsJobInTerminalState() {
    Assertions.assertFalse(jobWithStatus(JobStatus.PENDING).isJobInTerminalState())
    Assertions.assertFalse(jobWithStatus(JobStatus.RUNNING).isJobInTerminalState())
    Assertions.assertFalse(jobWithStatus(JobStatus.INCOMPLETE).isJobInTerminalState())
    Assertions.assertTrue(jobWithStatus(JobStatus.FAILED).isJobInTerminalState())
    Assertions.assertTrue(jobWithStatus(JobStatus.SUCCEEDED).isJobInTerminalState())
    Assertions.assertTrue(jobWithStatus(JobStatus.CANCELLED).isJobInTerminalState())
  }

  @Test
  fun testHasRunningAttempt() {
    Assertions.assertTrue(jobWithAttemptWithStatus(AttemptStatus.RUNNING).hasRunningAttempt())
    Assertions.assertFalse(jobWithAttemptWithStatus(AttemptStatus.FAILED).hasRunningAttempt())
    Assertions.assertFalse(jobWithAttemptWithStatus(AttemptStatus.SUCCEEDED).hasRunningAttempt())
    Assertions.assertFalse(jobWithAttemptWithStatus().hasRunningAttempt())
    Assertions.assertTrue(jobWithAttemptWithStatus(AttemptStatus.SUCCEEDED, AttemptStatus.RUNNING).hasRunningAttempt())
  }

  @Test
  fun testGetSuccessfulAttempt() {
    Assertions.assertTrue(jobWithAttemptWithStatus().getSuccessfulAttempt().isEmpty())
    Assertions.assertTrue(jobWithAttemptWithStatus(AttemptStatus.FAILED).getSuccessfulAttempt().isEmpty())
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { jobWithAttemptWithStatus(AttemptStatus.SUCCEEDED, AttemptStatus.SUCCEEDED).getSuccessfulAttempt() },
    )

    val job: Job = jobWithAttemptWithStatus(AttemptStatus.FAILED, AttemptStatus.SUCCEEDED)
    Assertions.assertTrue(job.getSuccessfulAttempt().isPresent())
    Assertions.assertEquals(job.attempts.get(1), job.getSuccessfulAttempt().get())
  }

  @Test
  fun testGetLastFailedAttempt() {
    Assertions.assertTrue(jobWithAttemptWithStatus().getLastFailedAttempt().isEmpty())
    Assertions.assertTrue(jobWithAttemptWithStatus(AttemptStatus.SUCCEEDED).getLastFailedAttempt().isEmpty())
    Assertions.assertTrue(jobWithAttemptWithStatus(AttemptStatus.FAILED).getLastFailedAttempt().isPresent())

    val job: Job = jobWithAttemptWithStatus(AttemptStatus.FAILED, AttemptStatus.FAILED)
    Assertions.assertTrue(job.getLastFailedAttempt().isPresent())
    Assertions.assertEquals(2, job.getLastFailedAttempt().get().getAttemptNumber())
  }

  @Test
  fun testGetLastAttempt() {
    val job: Job = jobWithAttemptWithStatus(AttemptStatus.FAILED, AttemptStatus.FAILED, AttemptStatus.SUCCEEDED)
    Assertions.assertTrue(job.getLastAttempt().isPresent())
    Assertions.assertEquals(3, job.getLastAttempt().get().getAttemptNumber())
  }

  @Test
  fun testGetAttemptByNumber() {
    val job: Job = jobWithAttemptWithStatus(AttemptStatus.FAILED, AttemptStatus.FAILED, AttemptStatus.SUCCEEDED)
    Assertions.assertTrue(job.getAttemptByNumber(2).isPresent())
    Assertions.assertEquals(2, job.getAttemptByNumber(2).get().getAttemptNumber())
  }

  @Test
  fun testValidateStatusTransitionFromPending() {
    val pendingJob: Job = jobWithStatus(JobStatus.PENDING)
    Assertions.assertDoesNotThrow(Executable { pendingJob.validateStatusTransition(JobStatus.RUNNING) })
    Assertions.assertDoesNotThrow(Executable { pendingJob.validateStatusTransition(JobStatus.FAILED) })
    Assertions.assertDoesNotThrow(Executable { pendingJob.validateStatusTransition(JobStatus.CANCELLED) })
    Assertions.assertDoesNotThrow(Executable { pendingJob.validateStatusTransition(JobStatus.INCOMPLETE) })
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { pendingJob.validateStatusTransition(JobStatus.SUCCEEDED) },
    )
  }

  @Test
  fun testValidateStatusTransitionFromRunning() {
    val runningJob: Job = jobWithStatus(JobStatus.RUNNING)
    Assertions.assertDoesNotThrow(Executable { runningJob.validateStatusTransition(JobStatus.INCOMPLETE) })
    Assertions.assertDoesNotThrow(Executable { runningJob.validateStatusTransition(JobStatus.SUCCEEDED) })
    Assertions.assertDoesNotThrow(Executable { runningJob.validateStatusTransition(JobStatus.FAILED) })
    Assertions.assertDoesNotThrow(Executable { runningJob.validateStatusTransition(JobStatus.CANCELLED) })
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { runningJob.validateStatusTransition(JobStatus.PENDING) },
    )
  }

  @Test
  fun testValidateStatusTransitionFromIncomplete() {
    val incompleteJob: Job = jobWithStatus(JobStatus.INCOMPLETE)
    Assertions.assertDoesNotThrow(Executable { incompleteJob.validateStatusTransition(JobStatus.PENDING) })
    Assertions.assertDoesNotThrow(Executable { incompleteJob.validateStatusTransition(JobStatus.RUNNING) })
    Assertions.assertDoesNotThrow(Executable { incompleteJob.validateStatusTransition(JobStatus.FAILED) })
    Assertions.assertDoesNotThrow(Executable { incompleteJob.validateStatusTransition(JobStatus.CANCELLED) })
    Assertions.assertDoesNotThrow(Executable { incompleteJob.validateStatusTransition(JobStatus.SUCCEEDED) })
  }

  @Test
  fun testValidateStatusTransitionFromSucceeded() {
    val suceededJob: Job = jobWithStatus(JobStatus.SUCCEEDED)
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { suceededJob.validateStatusTransition(JobStatus.PENDING) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { suceededJob.validateStatusTransition(JobStatus.RUNNING) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { suceededJob.validateStatusTransition(JobStatus.INCOMPLETE) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { suceededJob.validateStatusTransition(JobStatus.FAILED) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { suceededJob.validateStatusTransition(JobStatus.CANCELLED) },
    )
  }

  @Test
  fun testValidateStatusTransitionFromFailed() {
    val failedJob: Job = jobWithStatus(JobStatus.FAILED)
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { failedJob.validateStatusTransition(JobStatus.SUCCEEDED) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { failedJob.validateStatusTransition(JobStatus.PENDING) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { failedJob.validateStatusTransition(JobStatus.RUNNING) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { failedJob.validateStatusTransition(JobStatus.INCOMPLETE) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { failedJob.validateStatusTransition(JobStatus.CANCELLED) },
    )
  }

  @Test
  fun testValidateStatusTransitionFromCancelled() {
    val cancelledJob: Job = jobWithStatus(JobStatus.CANCELLED)
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { cancelledJob.validateStatusTransition(JobStatus.SUCCEEDED) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { cancelledJob.validateStatusTransition(JobStatus.PENDING) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { cancelledJob.validateStatusTransition(JobStatus.RUNNING) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { cancelledJob.validateStatusTransition(JobStatus.INCOMPLETE) },
    )
    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable { cancelledJob.validateStatusTransition(JobStatus.CANCELLED) },
    )
  }

  companion object {
    private fun jobWithStatus(jobStatus: JobStatus): Job = Job(1L, ConfigType.SYNC, "", JobConfig(), mutableListOf(), jobStatus, 0L, 0L, 0L, true)

    private fun jobWithAttemptWithStatus(vararg attemptStatuses: AttemptStatus?): Job {
      val attempts =
        IntStream
          .range(0, attemptStatuses.size)
          .mapToObj<Attempt?>(
            IntFunction { idx: Int ->
              Attempt(
                idx + 1,
                1L,
                null,
                null,
                null,
                attemptStatuses[idx]!!,
                null,
                null,
                idx.toLong(),
                0L,
                null,
              )
            },
          ).collect(Collectors.toList()) as MutableList<Attempt>
      return Job(1L, ConfigType.SYNC, "", JobConfig(), attempts, JobStatus.PENDING, 0L, 0L, 0L, true)
    }
  }
}
