/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer

import io.airbyte.config.WorkloadType
import io.airbyte.initContainer.InputFetcherTest.Fixtures.WORKLOAD_ID
import io.airbyte.initContainer.InputFetcherTest.Fixtures.workload
import io.airbyte.initContainer.input.InputHydrationProcessor
import io.airbyte.initContainer.system.SystemClient
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteContextConfig
import io.airbyte.workers.models.InitContainerConstants
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.LogDeliveryMode
import io.airbyte.workload.api.domain.Workload
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import secrets.persistence.SecretCoordinateException
import java.util.UUID

@ExtendWith(MockKExtension::class)
internal class InputFetcherTest {
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
        AirbyteContextConfig(workloadId = WORKLOAD_ID),
      )
  }

  @Test
  fun `STANDARD SYNC passes the fetched workload directly to hydration`() {
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns workload
    every { inputProcessor.process(workload) } returns Unit

    fetcher.fetch()

    verifyOrder {
      workloadApiClient.workloadGet(WORKLOAD_ID)
      inputProcessor.process(workload)
    }
    verify(exactly = 0) { workloadApiClient.workloadLogUploadAuthorization(any()) }
  }

  @Test
  fun `FLEX SYNC passes mode through workload hydration without fetching or serializing authorization`() {
    val flexWorkload = workload.copy(logDeliveryMode = LogDeliveryMode.FLEX)
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns flexWorkload
    every { inputProcessor.process(flexWorkload) } returns Unit

    fetcher.fetch()

    verifyOrder {
      workloadApiClient.workloadGet(WORKLOAD_ID)
      inputProcessor.process(flexWorkload)
    }
    verify(exactly = 0) { workloadApiClient.workloadLogUploadAuthorization(any()) }
    verifyHydrationContinued(flexWorkload)
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadType::class, names = ["SYNC"], mode = EnumSource.Mode.EXCLUDE)
  fun `non-SYNC workloads process without authorization lookup`(workloadType: WorkloadType) {
    val nonSyncWorkload = workload.copy(type = workloadType, logDeliveryMode = LogDeliveryMode.FLEX)
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns nonSyncWorkload
    every { inputProcessor.process(nonSyncWorkload) } returns Unit

    fetcher.fetch()

    verify(exactly = 0) { workloadApiClient.workloadLogUploadAuthorization(any()) }
    verifyHydrationContinued(nonSyncWorkload)
  }

  @Test
  fun `fails workload on workload fetch error`() {
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } throws Exception("bang")
    every { workloadApiClient.workloadFailure(any()) } returns Unit
    every { systemClient.exitProcess(any()) } returns Unit

    fetcher.fetch()

    verify { workloadApiClient.workloadFailure(any()) }
    verify { systemClient.exitProcess(InitContainerConstants.WORKLOAD_API_ERROR_EXIT_CODE) }
  }

  @Test
  fun `fails workload on workload process error`() {
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns workload
    every { inputProcessor.process(workload) } throws Exception("bang")
    every { workloadApiClient.workloadFailure(any()) } returns Unit
    every { systemClient.exitProcess(any()) } returns Unit

    fetcher.fetch()

    verify { workloadApiClient.workloadFailure(any()) }
    verify { systemClient.exitProcess(InitContainerConstants.UNEXPECTED_ERROR_EXIT_CODE) }
  }

  @Test
  fun `fails workload on with specific error on secret coordinate error`() {
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns workload
    every { inputProcessor.process(workload) } throws SecretCoordinateException("bang")
    every { workloadApiClient.workloadFailure(any()) } returns Unit
    every { systemClient.exitProcess(any()) } returns Unit

    fetcher.fetch()

    verify { workloadApiClient.workloadFailure(any()) }
    verify { systemClient.exitProcess(InitContainerConstants.SECRET_HYDRATION_ERROR_EXIT_CODE) }
  }

  @Test
  fun `exception not thrown if failure when failing workload`() {
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } throws Exception("bang")
    every { workloadApiClient.workloadFailure(any()) } throws Exception("bang 1")
    every { systemClient.exitProcess(any()) } returns Unit

    fetcher.fetch()

    verify { systemClient.exitProcess(any()) }
  }

  private fun verifyHydrationContinued(expectedWorkload: Workload) {
    verify(exactly = 1) { inputProcessor.process(expectedWorkload) }
    verify(exactly = 0) { workloadApiClient.workloadFailure(any()) }
    verify(exactly = 0) { systemClient.exitProcess(any()) }
  }

  object Fixtures {
    const val WORKLOAD_ID = "workload-id-13"

    val workload =
      Workload(
        id = WORKLOAD_ID,
        labels = mutableListOf(),
        inputPayload = "inputPayload",
        logPath = "logPath",
        type = WorkloadType.SYNC,
        autoId = UUID.randomUUID(),
      )
  }
}
