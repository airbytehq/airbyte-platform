/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.JobCreate
import io.airbyte.api.model.generated.JobDebugInfoRead
import io.airbyte.api.model.generated.JobIdRequestBody
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

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
    every { schedulerHandler.createJob(any()) } returns JobInfoRead()

    val jobCreate = JobCreate()
    val result = controller.createJob(jobCreate)

    assert(result != null)
  }

  @Test
  fun testCancelJob() {
    every { schedulerHandler.cancelJob(any()) } returns JobInfoRead()

    val jobIdRequestBody = JobIdRequestBody()
    val result = controller.cancelJob(jobIdRequestBody)

    assert(result != null)
  }

  @Test
  fun testGetJobDebugInfo() {
    every { jobHistoryHandler.getJobDebugInfo(any()) } returns JobDebugInfoRead()

    val jobIdRequestBody = JobIdRequestBody().id(10L)
    val result = controller.getJobDebugInfo(jobIdRequestBody)

    assert(result != null)
  }
}
