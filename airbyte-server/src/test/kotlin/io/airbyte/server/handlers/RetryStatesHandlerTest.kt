/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.JobRetryStateRequestBody
import io.airbyte.api.server.generated.models.RetryStateRead
import io.airbyte.server.handlers.apidomainmapping.RetryStatesMapper
import io.airbyte.server.repositories.RetryStatesRepository
import io.airbyte.server.repositories.domain.RetryState
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class RetryStatesHandlerTest {
  private lateinit var repo: RetryStatesRepository
  private lateinit var mapper: RetryStatesMapper
  private lateinit var handler: RetryStatesHandler

  @BeforeEach
  fun setup() {
    repo = mockk()
    mapper = mockk()
    handler = RetryStatesHandler(repo, mapper)
  }

  @Test
  fun testGetByJobId() {
    val jobId = 1345L
    val apiReq = JobIdRequestBody(id = jobId)
    val domain = RetryState.RetryStateBuilder().build()
    val apiResp =
      RetryStateRead(
        id = UUID.randomUUID(),
        connectionId = UUID.randomUUID(),
        jobId = jobId,
        successiveCompleteFailures = 0,
        totalCompleteFailures = 0,
        successivePartialFailures = 0,
        totalPartialFailures = 0,
      )

    every { repo.findByJobId(jobId) } returns domain
    every { mapper.map(domain) } returns apiResp

    val result = handler.getByJobId(apiReq)

    Assertions.assertNotNull(result)
    Assertions.assertSame(apiResp, result)
  }

  @Test
  fun testPutByJobIdUpdate() {
    val jobId = 1345L
    val apiReq =
      JobRetryStateRequestBody(
        connectionId = UUID.randomUUID(),
        jobId = jobId,
        successiveCompleteFailures = 0,
        totalCompleteFailures = 0,
        successivePartialFailures = 0,
        totalPartialFailures = 0,
      )

    val update =
      RetryState
        .RetryStateBuilder()
        .jobId(jobId)
        .build()

    every { mapper.map(apiReq) } returns update
    every { repo.createOrUpdateByJobId(jobId, update) } returns Unit

    handler.putByJobId(apiReq)

    verify(exactly = 1) { repo.createOrUpdateByJobId(jobId, update) }
  }
}
