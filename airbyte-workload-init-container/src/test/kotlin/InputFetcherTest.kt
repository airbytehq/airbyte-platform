/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer

import io.airbyte.initContainer.InputFetcherTest.Fixtures.WORKLOAD_ID
import io.airbyte.initContainer.InputFetcherTest.Fixtures.workload
import io.airbyte.initContainer.input.InputHydrationProcessor
import io.airbyte.initContainer.system.SystemClient
import io.airbyte.metrics.MetricClient
import io.airbyte.workers.models.InitContainerConstants
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadType
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import secrets.persistence.SecretCoordinateException
import java.util.UUID

@ExtendWith(MockKExtension::class)
class InputFetcherTest {
  @MockK
  lateinit var workloadApiClient: WorkloadApiClient

  @MockK
  lateinit var inputProcessor: InputHydrationProcessor

  @MockK
  lateinit var systemClient: SystemClient

  @MockK(relaxed = true)
  lateinit var metricClient: MetricClient

  private lateinit var fetcher: InputFetcher

  @BeforeEach
  fun setup() {
    fetcher =
      InputFetcher(
        workloadApiClient,
        inputProcessor,
        systemClient,
        metricClient,
        WORKLOAD_ID,
      )
  }

  @Test
  fun `fetches input and processes it`() {
    every { workloadApiClient.workloadApi.workloadGet(WORKLOAD_ID) } returns workload
    every { inputProcessor.process(workload) } returns Unit

    fetcher.fetch()

    verify { workloadApiClient.workloadApi.workloadGet(WORKLOAD_ID) }
    verify { inputProcessor.process(workload) }
  }

  @Test
  fun `fails workload on workload fetch error`() {
    every { workloadApiClient.workloadApi.workloadGet(WORKLOAD_ID) } throws Exception("bang")
    every { workloadApiClient.workloadApi.workloadFailure(any()) } returns Unit
    every { systemClient.exitProcess(any()) } returns Unit

    fetcher.fetch()

    verify { workloadApiClient.workloadApi.workloadFailure(any()) }
    verify { systemClient.exitProcess(InitContainerConstants.WORKLOAD_API_ERROR_EXIT_CODE) }
  }

  @Test
  fun `fails workload on workload process error`() {
    every { workloadApiClient.workloadApi.workloadGet(WORKLOAD_ID) } returns workload
    every { inputProcessor.process(workload) } throws Exception("bang")
    every { workloadApiClient.workloadApi.workloadFailure(any()) } returns Unit
    every { systemClient.exitProcess(any()) } returns Unit

    fetcher.fetch()

    verify { workloadApiClient.workloadApi.workloadFailure(any()) }
    verify { systemClient.exitProcess(InitContainerConstants.UNEXPECTED_ERROR_EXIT_CODE) }
  }

  @Test
  fun `fails workload on with specific error on secret coordinate error`() {
    every { workloadApiClient.workloadApi.workloadGet(WORKLOAD_ID) } returns workload
    every { inputProcessor.process(workload) } throws SecretCoordinateException("bang")
    every { workloadApiClient.workloadApi.workloadFailure(any()) } returns Unit
    every { systemClient.exitProcess(any()) } returns Unit

    fetcher.fetch()

    verify { workloadApiClient.workloadApi.workloadFailure(any()) }
    verify { systemClient.exitProcess(InitContainerConstants.SECRET_HYDRATION_ERROR_EXIT_CODE) }
  }

  @Test
  fun `exception not thrown if failure when failing workload`() {
    every { workloadApiClient.workloadApi.workloadGet(WORKLOAD_ID) } throws Exception("bang")
    every { workloadApiClient.workloadApi.workloadFailure(any()) } throws Exception("bang 1")
    every { systemClient.exitProcess(any()) } returns Unit

    fetcher.fetch()

    verify { systemClient.exitProcess(any()) }
  }

  object Fixtures {
    const val WORKLOAD_ID = "workload-id-13"

    val workload =
      Workload(
        id = WORKLOAD_ID,
        labels = listOf(),
        inputPayload = "inputPayload",
        logPath = "logPath",
        type = WorkloadType.SYNC,
        autoId = UUID.randomUUID(),
      )
  }
}
