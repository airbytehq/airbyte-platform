/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobReadList
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.api.problems.throwable.generated.TryAgainLaterConflictProblem
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.micronaut.runtime.AirbyteApiConfig
import io.airbyte.publicApi.server.generated.models.JobTypeEnum
import io.airbyte.server.apis.publicapi.errorHandlers.JOB_NOT_RUNNING_MESSAGE
import io.airbyte.server.apis.publicapi.filters.JobsFilter
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream

class JobServiceTest {
  private lateinit var jobService: JobServiceImpl
  private val schedulerHandler: SchedulerHandler = mockk()
  private val jobHistoryHandler: JobHistoryHandler = mockk()

  private val connectionId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    jobService =
      JobServiceImpl(
        schedulerHandler = schedulerHandler,
        jobHistoryHandler = jobHistoryHandler,
        currentUserService = mockk(),
        userService = mockk(),
        airbyteApiConfig = AirbyteApiConfig(),
      )
  }

  @Test
  fun `test sync already running value conflict known exception`() {
    val failureReason = "A sync is already running for: $connectionId"
    every { schedulerHandler.syncConnection(any()) } throws
      ValueConflictKnownException(failureReason)

    assertThrows<TryAgainLaterConflictProblem>(failureReason) { jobService.sync(connectionId) }
  }

  @Test
  fun `test sync already running illegal state exception`() {
    val failureReason = "A sync is already running for: $connectionId"
    every { schedulerHandler.syncConnection(any()) } throws
      IllegalStateException(failureReason)

    assertThrows<StateConflictProblem>(failureReason) { jobService.sync(connectionId) }
  }

  @Test
  fun `test cancel non-running sync`() {
    // This is a real error message that we can get.
    // Happens because after canceling a job we go to the job persistence to fetch it but have no ID

    val couldNotFindJobMessage = "Could not find job with id: -1"
    every { schedulerHandler.syncConnection(any()) } throws RuntimeException(couldNotFindJobMessage)
    assertThrows<StateConflictProblem>(JOB_NOT_RUNNING_MESSAGE) { jobService.sync(connectionId) }

    val failureReason = "Failed to cancel job with id: -1"
    every { schedulerHandler.syncConnection(any()) } throws IllegalStateException(failureReason)
    assertThrows<StateConflictProblem>(JOB_NOT_RUNNING_MESSAGE) { jobService.sync(connectionId) }
  }

  @Test
  fun `test getJobList with null jobType returns all job types`() {
    val requestBodySlot = slot<io.airbyte.api.model.generated.JobListRequestBody>()
    every { jobHistoryHandler.listJobsForLight(capture(requestBodySlot)) } returns JobReadList().jobs(emptyList()).totalJobCount(0L)

    val jobsFilter =
      JobsFilter(
        createdAtStart = null,
        createdAtEnd = null,
        updatedAtStart = null,
        updatedAtEnd = null,
        limit = 10,
        offset = 0,
        jobType = null,
        status = null,
      )
    jobService.getJobList(connectionId, jobsFilter)

    verify { jobHistoryHandler.listJobsForLight(any()) }
    val capturedConfigTypes = requestBodySlot.captured.configTypes
    assertEquals(4, capturedConfigTypes.size)
    assertTrue(capturedConfigTypes.contains(JobConfigType.SYNC))
    assertTrue(capturedConfigTypes.contains(JobConfigType.RESET_CONNECTION))
    assertTrue(capturedConfigTypes.contains(JobConfigType.CLEAR))
    assertTrue(capturedConfigTypes.contains(JobConfigType.REFRESH))
  }

  @ParameterizedTest(name = "jobType {0} should return config type {1}")
  @MethodSource("jobTypeToConfigTypeProvider")
  fun `test getJobList with specific jobType returns only that type`(
    jobType: JobTypeEnum,
    expectedConfigType: JobConfigType,
  ) {
    val requestBodySlot = slot<io.airbyte.api.model.generated.JobListRequestBody>()
    every { jobHistoryHandler.listJobsForLight(capture(requestBodySlot)) } returns JobReadList().jobs(emptyList()).totalJobCount(0L)

    val filter =
      JobsFilter(
        createdAtStart = null,
        createdAtEnd = null,
        updatedAtStart = null,
        updatedAtEnd = null,
        limit = 10,
        offset = 0,
        jobType = jobType,
        status = null,
      )
    jobService.getJobList(connectionId, filter)
    assertEquals(listOf(expectedConfigType), requestBodySlot.captured.configTypes)
  }

  companion object {
    @JvmStatic
    fun jobTypeToConfigTypeProvider(): Stream<Arguments> =
      Stream.of(
        Arguments.of(JobTypeEnum.SYNC, JobConfigType.SYNC),
        Arguments.of(JobTypeEnum.RESET, JobConfigType.RESET_CONNECTION),
        Arguments.of(JobTypeEnum.CLEAR, JobConfigType.CLEAR),
        Arguments.of(JobTypeEnum.REFRESH, JobConfigType.REFRESH),
      )
  }
}
