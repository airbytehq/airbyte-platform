/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.JobCreate
import io.airbyte.api.server.generated.models.JobDebugInfoRead
import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.JobInfoRead
import io.airbyte.api.server.generated.models.JobRead
import io.airbyte.api.server.generated.models.JobStatus
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class JobsApiControllerTest {
  private lateinit var controller: JobsApiController
  private val schedulerHandler: SchedulerHandler = mockk()
  private val jobHistoryHandler: JobHistoryHandler = mockk()

  @BeforeEach
  fun setUp() {
    controller =
      JobsApiController(
        schedulerHandler = schedulerHandler,
        jobHistoryHandler = jobHistoryHandler,
        jobInputHandler = mockk(),
        jobsHandler = mockk(),
        streamResetRecordsHelper = mockk(),
        jobObservabilityService = mockk(),
      )
  }

  @Test
  fun testCreateJob() {
    every { schedulerHandler.createJob(any()) } returns JobInfoRead(job = sampleJobRead(), attempts = emptyList())

    val jobCreate = JobCreate(connectionId = UUID.randomUUID(), isScheduled = true)
    val result = controller.createJob(jobCreate)

    assertNotNull(result)
  }

  @Test
  fun testCancelJob() {
    every { schedulerHandler.cancelJob(any()) } returns JobInfoRead(job = sampleJobRead(), attempts = emptyList())

    val jobIdRequestBody = JobIdRequestBody(id = 10L)
    val result = controller.cancelJob(jobIdRequestBody)

    assertNotNull(result)
  }

  @Test
  fun testGetJobDebugInfo() {
    every { jobHistoryHandler.getJobDebugInfo(any()) } returns JobDebugInfoRead(job = mockk(), attempts = emptyList())

    val jobIdRequestBody = JobIdRequestBody(id = 10L)
    val result = controller.getJobDebugInfo(jobIdRequestBody)

    assertNotNull(result)
  }

  private fun sampleJobRead() =
    JobRead(
      id = 1L,
      configType = JobConfigType.SYNC,
      configId = UUID.randomUUID().toString(),
      createdAt = 0L,
      updatedAt = 0L,
      status = JobStatus.RUNNING,
    )
}
