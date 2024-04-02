package io.airbyte.server.apis.publicapi.services

import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.commons.server.errors.problems.ConflictProblem
import io.airbyte.commons.server.errors.problems.SyncConflictProblem
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.server.apis.publicapi.errorHandlers.JOB_NOT_RUNNING_MESSAGE
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

@MicronautTest
class JobServiceTest {
  @Inject
  private lateinit var jobService: JobServiceImpl

  private val connectionId = UUID.randomUUID()

  private val schedulerHandler = mockk<SchedulerHandler>()

  @MockBean(SchedulerHandler::class)
  fun schedulerHandler(): SchedulerHandler {
    return schedulerHandler
  }

  @Test
  fun `test sync already running value conflict known exception`() {
    val failureReason = "A sync is already running for: $connectionId"
    val schedulerHandler = schedulerHandler()
    every { schedulerHandler.syncConnection(any()) } throws
      ValueConflictKnownException(failureReason)

    assertThrows<SyncConflictProblem>(failureReason) { jobService.sync(connectionId) }
  }

  @Test
  fun `test sync already running illegal state exception`() {
    val failureReason = "A sync is already running for: $connectionId"
    val schedulerHandler = schedulerHandler()
    every { schedulerHandler.syncConnection(any()) } throws
      IllegalStateException(failureReason)

    assertThrows<ConflictProblem>(failureReason) { jobService.sync(connectionId) }
  }

  @Test
  fun `test cancel non-running sync`() {
    // This is a real error message that we can get.
    // Happens because after canceling a job we go to the job persistence to fetch it but have no ID

    val couldNotFindJobMessage = "Could not find job with id: -1"
    every { schedulerHandler.syncConnection(any()) } throws RuntimeException(couldNotFindJobMessage)
    assertThrows<ConflictProblem>(JOB_NOT_RUNNING_MESSAGE) { jobService.sync(connectionId) }

    val failureReason = "Failed to cancel job with id: -1"
    every { schedulerHandler.syncConnection(any()) } throws IllegalStateException(failureReason)
    assertThrows<ConflictProblem>(JOB_NOT_RUNNING_MESSAGE) { jobService.sync(connectionId) }
  }
}
