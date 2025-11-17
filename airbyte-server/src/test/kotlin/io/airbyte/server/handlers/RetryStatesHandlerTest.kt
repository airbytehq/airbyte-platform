/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.model.generated.JobIdRequestBody
import io.airbyte.api.model.generated.JobRetryStateRequestBody
import io.airbyte.api.model.generated.RetryStateRead
import io.airbyte.server.handlers.apidomainmapping.RetryStatesMapper
import io.airbyte.server.repositories.RetryStatesRepository
import io.airbyte.server.repositories.domain.RetryState
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

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
    val apiReq = JobIdRequestBody().id(jobId)
    val domain = RetryState.RetryStateBuilder().build()
    val apiResp = RetryStateRead()

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
      JobRetryStateRequestBody()
        .jobId(jobId)

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
