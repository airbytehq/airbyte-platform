/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.JobIdRequestBody
import io.airbyte.api.model.generated.JobRetryStateRequestBody
import io.airbyte.api.model.generated.RetryStateRead
import io.airbyte.server.assertStatus
import io.airbyte.server.handlers.RetryStatesHandler
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
import java.util.Optional
import java.util.UUID

private const val PATH_BASE: String = "/api/v1/jobs/retry_states"
private const val PATH_GET: String = PATH_BASE + "/get"
private const val PATH_PUT: String = PATH_BASE + "/create_or_update"
private const val JOB_ID1: Long = 21891253
private val jobIdRequestBody = JobIdRequestBody().id(JOB_ID1)
private val jobRetryStateRequestBody =
  JobRetryStateRequestBody()
    .id(UUID.randomUUID())
    .connectionId(UUID.randomUUID())
    .jobId(JOB_ID1)
    .successiveCompleteFailures(8)
    .totalCompleteFailures(12)
    .successivePartialFailures(4)
    .totalPartialFailures(42)

@MicronautTest
internal class JobRetryStatesApiControllerTest {
  @Inject
  lateinit var retryStatesHandler: RetryStatesHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(RetryStatesHandler::class)
  fun retryStatesHandler(): RetryStatesHandler = mockk()

  @Test
  fun forJobFound() {
    every { retryStatesHandler.getByJobId(any()) } returns Optional.of(RetryStateRead())
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(PATH_GET, jobIdRequestBody)))
  }

  @Test
  fun forJobNotFound() {
    every { retryStatesHandler.getByJobId(any()) } returns Optional.empty()
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(PATH_GET, jobIdRequestBody)))
  }

  @Test
  fun putForJob() {
    every { retryStatesHandler.putByJobId(jobRetryStateRequestBody) } returns Unit
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(PATH_PUT, jobRetryStateRequestBody)))
  }
}
