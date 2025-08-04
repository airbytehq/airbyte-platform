/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.model.generated.DeleteStreamResetRecordsForJobRequest
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity.DeleteStreamResetRecordsForJobInput
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.function.Executable
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import java.io.IOException
import java.util.UUID

@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
internal class StreamResetActivityTest {
  @Mock
  private lateinit var airbyteApiClient: AirbyteApiClient

  @Mock
  private lateinit var jobsApi: JobsApi

  @InjectMocks
  private lateinit var streamResetActivity: StreamResetActivityImpl

  @BeforeEach
  fun setup() {
    whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
  }

  @Test
  @Throws(IOException::class)
  fun deleteStreamResetRecordsForJobSuccess() {
    val input = DeleteStreamResetRecordsForJobInput(UUID.randomUUID(), "123".toLong())

    val req = argumentCaptor<DeleteStreamResetRecordsForJobRequest>()

    streamResetActivity.deleteStreamResetRecordsForJob(input)

    verify(jobsApi).deleteStreamResetRecordsForJob(req.capture())
    Assertions.assertEquals(input.jobId, req.firstValue.jobId)
    Assertions.assertEquals(input.connectionId, req.firstValue.connectionId)
  }

  @Test
  @Throws(IOException::class)
  fun deleteStreamResetRecordsForJobThrowsRetryableException() {
    val input = DeleteStreamResetRecordsForJobInput(UUID.randomUUID(), "123".toLong())

    doThrow(IOException("bang."))
      .whenever(jobsApi)
      .deleteStreamResetRecordsForJob(any<DeleteStreamResetRecordsForJobRequest>())

    Assertions.assertThrows<RetryableException>(
      RetryableException::class.java,
      Executable { streamResetActivity.deleteStreamResetRecordsForJob(input) },
    )
  }
}
