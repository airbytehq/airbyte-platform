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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito

internal class RetryStatesHandlerTest {
  @Mock
  var repo: RetryStatesRepository? = null

  @Mock
  var mapper: RetryStatesMapper? = null

  var handler: RetryStatesHandler? = null

  @BeforeEach
  fun setup() {
    repo = Mockito.mock(RetryStatesRepository::class.java)
    mapper = Mockito.mock(RetryStatesMapper::class.java)
    handler = RetryStatesHandler(repo!!, mapper!!)
  }

  @Test
  fun testGetByJobId() {
    val jobId = 1345L
    val apiReq = JobIdRequestBody().id(jobId)
    val domain = RetryState.RetryStateBuilder().build()
    val apiResp = RetryStateRead()

    Mockito
      .`when`(repo!!.findByJobId(jobId))
      .thenReturn(domain)
    Mockito
      .`when`(mapper!!.map(domain))
      .thenReturn(apiResp)

    val result = handler!!.getByJobId(apiReq)

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

    Mockito
      .`when`(mapper!!.map(apiReq))
      .thenReturn(update)

    handler!!.putByJobId(apiReq)

    Mockito.verify(repo, Mockito.times(1))?.createOrUpdateByJobId(jobId, update)
  }
}
