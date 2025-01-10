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
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@MicronautTest
internal class JobsApiControllerTest {
  @Inject
  lateinit var schedulerHandler: SchedulerHandler

  @Inject
  lateinit var jobHistoryHandler: JobHistoryHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(SchedulerHandler::class)
  fun schedulerHandler(): SchedulerHandler = mockk()

  @MockBean(JobHistoryHandler::class)
  fun jobHistoryHandler(): JobHistoryHandler = mockk()

  @Test
  fun testCreateJob() {
    every { schedulerHandler.createJob(any()) } returns JobInfoRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/jobs/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, JobCreate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, JobCreate())))
  }

  @Test
  fun testCancelJob() {
    every { schedulerHandler.cancelJob(any()) } returns JobInfoRead()

    val path = "/api/v1/jobs/cancel"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, JobIdRequestBody())))
  }

  @Test
  fun testGetJobDebugInfo() {
    every { jobHistoryHandler.getJobDebugInfo(any()) } returns JobDebugInfoRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/jobs/get_debug_info"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, JobIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, JobIdRequestBody())))
  }
}
