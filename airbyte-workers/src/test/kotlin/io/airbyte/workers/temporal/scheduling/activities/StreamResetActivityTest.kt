/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.model.generated.DeleteStreamResetRecordsForJobRequest
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity.DeleteStreamResetRecordsForJobInput
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

private val CONNECTION_ID = UUID.randomUUID()
private const val JOB_ID = 123L

internal class StreamResetActivityTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var jobsApi: JobsApi
  private lateinit var streamResetActivity: StreamResetActivityImpl

  @BeforeEach
  fun setup() {
    airbyteApiClient = mockk()
    jobsApi = mockk(relaxed = true)
    every { airbyteApiClient.jobsApi } returns jobsApi
    streamResetActivity = StreamResetActivityImpl(airbyteApiClient)
  }

  @Test
  fun deleteStreamResetRecordsForJobSuccess() {
    val input = DeleteStreamResetRecordsForJobInput(CONNECTION_ID, JOB_ID)

    val req = slot<DeleteStreamResetRecordsForJobRequest>()

    streamResetActivity.deleteStreamResetRecordsForJob(input)

    verify { jobsApi.deleteStreamResetRecordsForJob(capture(req)) }
    Assertions.assertEquals(input.jobId, req.captured.jobId)
    Assertions.assertEquals(input.connectionId, req.captured.connectionId)
  }

  @Test
  fun deleteStreamResetRecordsForJobThrowsRetryableException() {
    val input = DeleteStreamResetRecordsForJobInput(CONNECTION_ID, JOB_ID)

    every { jobsApi.deleteStreamResetRecordsForJob(any<DeleteStreamResetRecordsForJobRequest>()) } throws IOException("bang.")

    Assertions.assertThrows(
      RetryableException::class.java,
    ) { streamResetActivity.deleteStreamResetRecordsForJob(input) }
  }
}
