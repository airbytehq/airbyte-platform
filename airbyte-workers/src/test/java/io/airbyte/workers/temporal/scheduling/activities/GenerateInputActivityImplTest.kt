/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.commons.temporal.utils.PayloadChecker
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension

@ExtendWith(MockitoExtension::class)
internal class GenerateInputActivityImplTest {
  @Mock
  private val mAirbyteApiClient: AirbyteApiClient? = null

  @Mock
  private val mJobsApi: JobsApi? = null

  @Mock
  private val mPayloadChecker: PayloadChecker? = null

  private var activity: GenerateInputActivity? = null

  @BeforeEach
  fun setUp() {
    Mockito.`when`<JobsApi?>(mAirbyteApiClient!!.jobsApi).thenReturn(mJobsApi)
    activity = GenerateInputActivityImpl(mAirbyteApiClient, mPayloadChecker!!)
  }
}
