/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer

import io.airbyte.commons.json.Jsons
import io.airbyte.config.WorkloadType
import io.airbyte.initContainer.InputFetcherTest.Fixtures.WORKLOAD_ID
import io.airbyte.initContainer.InputFetcherTest.Fixtures.workload
import io.airbyte.initContainer.input.InputHydrationProcessor
import io.airbyte.initContainer.serde.ObjectSerializer
import io.airbyte.initContainer.system.FileClient
import io.airbyte.initContainer.system.SystemClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.micronaut.runtime.AirbyteContextConfig
import io.airbyte.workers.models.InitContainerConstants
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.GcsDownscopedOAuthLogUploadAuthorization
import io.airbyte.workload.api.domain.LogDeliveryMode
import io.airbyte.workload.api.domain.Workload
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifyOrder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import secrets.persistence.SecretCoordinateException
import java.time.OffsetDateTime
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

  @MockK(relaxed = true)
  lateinit var fileClient: FileClient

  private lateinit var serializer: ObjectSerializer

  private lateinit var fetcher: InputFetcher

  @BeforeEach
  fun setup() {
    serializer = spyk(ObjectSerializer())
    fetcher =
      InputFetcher(
        workloadApiClient,
        inputProcessor,
        systemClient,
        metricClient,
        AirbyteContextConfig(workloadId = WORKLOAD_ID),
        serializer,
        fileClient,
      )
  }

  @Test
  fun `STANDARD SYNC writes its mode without fetching authorization and processes the workload`() {
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns workload
    every { inputProcessor.process(workload) } returns Unit

    fetcher.fetch()

    verifyOrder {
      workloadApiClient.workloadGet(WORKLOAD_ID)
      fileClient.writeInputFile(FileConstants.LOG_DELIVERY_MODE_FILE, "\"STANDARD\"")
      inputProcessor.process(workload)
    }
    verify(exactly = 0) { workloadApiClient.workloadLogUploadAuthorization(any()) }
    verify(exactly = 0) { fileClient.writeInputFileAtomically(any(), any()) }
  }

  @Test
  fun `FLEX SYNC writes mode and typed authorization before processing the workload`() {
    val flexWorkload = workload.copy(logDeliveryMode = LogDeliveryMode.FLEX)
    val authorization = Fixtures.authorization
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns flexWorkload
    every { workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns authorization
    every { inputProcessor.process(flexWorkload) } returns Unit

    fetcher.fetch()

    verifyOrder {
      workloadApiClient.workloadGet(WORKLOAD_ID)
      fileClient.writeInputFile(FileConstants.LOG_DELIVERY_MODE_FILE, "\"FLEX\"")
      workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID)
      fileClient.writeInputFileAtomically(FileConstants.LOG_UPLOAD_AUTHORIZATION_FILE, any())
      inputProcessor.process(flexWorkload)
    }
    verify(exactly = 1) { workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }
    val serialized = slot<String>()
    verify(exactly = 1) {
      fileClient.writeInputFileAtomically(FileConstants.LOG_UPLOAD_AUTHORIZATION_FILE, capture(serialized))
    }
    assertThat(Jsons.deserialize(serialized.captured))
      .isEqualTo(
        Jsons.deserialize(
          """
          {
            "type": "GCS_DOWNSCOPED_OAUTH",
            "accessToken": "opaque-token",
            "expiresAt": "2026-09-02T12:34:56Z",
            "bucketName": "customer-logs",
            "objectKeyPrefix": "workspace/workload/"
          }
          """.trimIndent(),
        ),
      )
  }

  @Test
  fun `FLEX SYNC with no authorization keeps its mode and processes the workload`() {
    val flexWorkload = workload.copy(logDeliveryMode = LogDeliveryMode.FLEX)
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns flexWorkload
    every { workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns null
    every { inputProcessor.process(flexWorkload) } returns Unit

    fetcher.fetch()

    verify { fileClient.writeInputFile(FileConstants.LOG_DELIVERY_MODE_FILE, "\"FLEX\"") }
    verify(exactly = 1) { workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }
    verify(exactly = 0) { fileClient.writeInputFileAtomically(any(), any()) }
    verifyHydrationContinued(flexWorkload)
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadType::class, names = ["SYNC"], mode = EnumSource.Mode.EXCLUDE)
  fun `non-SYNC workloads skip the log delivery handoff`(workloadType: WorkloadType) {
    val nonSyncWorkload = workload.copy(type = workloadType, logDeliveryMode = LogDeliveryMode.FLEX)
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns nonSyncWorkload
    every { inputProcessor.process(nonSyncWorkload) } returns Unit

    fetcher.fetch()

    verify(exactly = 0) { fileClient.writeInputFile(FileConstants.LOG_DELIVERY_MODE_FILE, any()) }
    verify(exactly = 0) { workloadApiClient.workloadLogUploadAuthorization(any()) }
    verify(exactly = 0) { fileClient.writeInputFileAtomically(any(), any()) }
    verifyHydrationContinued(nonSyncWorkload)
  }

  @Test
  fun `mode serialization failure skips authorization and processes the workload`() {
    val flexWorkload = workload.copy(logDeliveryMode = LogDeliveryMode.FLEX)
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns flexWorkload
    every { serializer.serialize(LogDeliveryMode.FLEX) } throws IllegalStateException(Fixtures.SENSITIVE_ERROR)
    every { inputProcessor.process(flexWorkload) } returns Unit

    fetcher.fetch()

    verify(exactly = 0) { fileClient.writeInputFile(FileConstants.LOG_DELIVERY_MODE_FILE, any()) }
    verify(exactly = 0) { workloadApiClient.workloadLogUploadAuthorization(any()) }
    verifyHandoffFailure("mode-serialization")
    verifyHydrationContinued(flexWorkload)
  }

  @Test
  fun `mode file failure skips authorization and processes the workload`() {
    val flexWorkload = workload.copy(logDeliveryMode = LogDeliveryMode.FLEX)
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns flexWorkload
    every {
      fileClient.writeInputFile(FileConstants.LOG_DELIVERY_MODE_FILE, "\"FLEX\"")
    } throws IllegalStateException(Fixtures.SENSITIVE_ERROR)
    every { inputProcessor.process(flexWorkload) } returns Unit

    fetcher.fetch()

    verify(exactly = 0) { workloadApiClient.workloadLogUploadAuthorization(any()) }
    verifyHandoffFailure("mode-file-write")
    verifyHydrationContinued(flexWorkload)
  }

  @Test
  fun `authorization fetch failure keeps mode and processes the workload`() {
    val flexWorkload = workload.copy(logDeliveryMode = LogDeliveryMode.FLEX)
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns flexWorkload
    every { workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws IllegalStateException(Fixtures.SENSITIVE_ERROR)
    every { inputProcessor.process(flexWorkload) } returns Unit

    fetcher.fetch()

    verify { fileClient.writeInputFile(FileConstants.LOG_DELIVERY_MODE_FILE, "\"FLEX\"") }
    verify(exactly = 0) { fileClient.writeInputFileAtomically(any(), any()) }
    verifyHandoffFailure("authorization-fetch")
    verifyHydrationContinued(flexWorkload)
  }

  @Test
  fun `authorization serialization failure writes no authorization and processes the workload`() {
    val flexWorkload = workload.copy(logDeliveryMode = LogDeliveryMode.FLEX)
    val authorization = Fixtures.authorization
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns flexWorkload
    every { workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns authorization
    every { serializer.serialize(authorization) } throws IllegalStateException(Fixtures.SENSITIVE_ERROR)
    every { inputProcessor.process(flexWorkload) } returns Unit

    fetcher.fetch()

    verify(exactly = 0) { fileClient.writeInputFileAtomically(any(), any()) }
    verifyHandoffFailure("authorization-serialization")
    verifyHydrationContinued(flexWorkload)
  }

  @Test
  fun `authorization atomic replacement failure processes the workload`() {
    val flexWorkload = workload.copy(logDeliveryMode = LogDeliveryMode.FLEX)
    val authorization = Fixtures.authorization
    every { workloadApiClient.workloadGet(WORKLOAD_ID) } returns flexWorkload
    every { workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns authorization
    every {
      fileClient.writeInputFileAtomically(FileConstants.LOG_UPLOAD_AUTHORIZATION_FILE, any())
    } throws IllegalStateException(Fixtures.SENSITIVE_ERROR)
    every { inputProcessor.process(flexWorkload) } returns Unit

    fetcher.fetch()

    verifyHandoffFailure("authorization-file-write")
    verifyHydrationContinued(flexWorkload)
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

  private fun verifyHandoffFailure(step: String) {
    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.WORKLOAD_LOG_DELIVERY_HANDOFF_FAILURE,
        attributes = arrayOf(MetricAttribute("step", step)),
      )
    }
  }

  private fun verifyHydrationContinued(expectedWorkload: Workload) {
    verify(exactly = 1) { inputProcessor.process(expectedWorkload) }
    verify(exactly = 0) { workloadApiClient.workloadFailure(any()) }
    verify(exactly = 0) { systemClient.exitProcess(any()) }
  }

  object Fixtures {
    const val WORKLOAD_ID = "workload-id-13"
    const val SENSITIVE_ERROR = "access-token bucket-name object-key-prefix"

    val workload =
      Workload(
        id = WORKLOAD_ID,
        labels = mutableListOf(),
        inputPayload = "inputPayload",
        logPath = "logPath",
        type = WorkloadType.SYNC,
        autoId = UUID.randomUUID(),
      )

    val authorization =
      GcsDownscopedOAuthLogUploadAuthorization(
        accessToken = "opaque-token",
        expiresAt = OffsetDateTime.parse("2026-09-02T12:34:56Z"),
        bucketName = "customer-logs",
        objectKeyPrefix = "workspace/workload/",
      )
  }
}
