/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.api.client.ApiException
import io.airbyte.config.WorkloadType
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadListResponse
import io.airbyte.workload.launcher.ClaimProcessorTracker
import io.airbyte.workload.launcher.ClaimedProcessor
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.kotlintest.milliseconds
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import reactor.core.publisher.Mono
import java.net.ConnectException
import java.net.SocketException
import java.net.SocketTimeoutException
import java.util.UUID
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

class ClaimedProcessorTest {
  private lateinit var claimedProcessor: ClaimedProcessor
  private lateinit var workloadApiClient: WorkloadApiClient
  private lateinit var claimProcessorTracker: ClaimProcessorTracker
  private lateinit var launchPipeline: LaunchPipeline

  @BeforeEach
  fun setup() {
    workloadApiClient = mockk()
    claimProcessorTracker = mockk(relaxed = true)
    launchPipeline =
      mockk(relaxed = true) {
        every { buildPipeline(any()) } returns Mono.empty()
      }
    claimedProcessor =
      ClaimedProcessor(
        workloadApiClient = workloadApiClient,
        pipe = launchPipeline,
        metricClient = mockk(relaxed = true),
        parallelism = 10,
        claimProcessorTracker = claimProcessorTracker,
        backoffDuration = 1.milliseconds.toKotlinDuration().toJavaDuration(),
        backoffMaxDelay = 2.milliseconds.toKotlinDuration().toJavaDuration(),
      )
  }

  @Test
  fun `test retrieve and process when there are no workload to resume`() {
    every { workloadApiClient.workloadList(any()) } returns WorkloadListResponse(listOf())
    claimedProcessor.retrieveAndProcess("dataplane1")

    verify { claimProcessorTracker.trackNumberOfClaimsToResume(0) }
  }

  @Test
  fun `test retrieve and process with two workloads to resume`() {
    every { workloadApiClient.workloadList(any()) } returns
      WorkloadListResponse(
        listOf(
          Workload(
            id = "1",
            labels = mutableListOf(),
            inputPayload = "payload",
            logPath = "logPath",
            type = WorkloadType.SYNC,
            autoId = UUID.randomUUID(),
          ),
          Workload(
            id = "2",
            labels = mutableListOf(),
            inputPayload = "payload",
            logPath = "logPath",
            type = WorkloadType.SYNC,
            autoId = UUID.randomUUID(),
          ),
        ),
      )
    claimedProcessor.retrieveAndProcess("dataplane1")

    verify { claimProcessorTracker.trackNumberOfClaimsToResume(2) }
    verify(exactly = 2) { launchPipeline.buildPipeline(any()) }
  }

  @ParameterizedTest
  @ValueSource(ints = [400, 401, 403])
  fun `test resume fails when unable to fetch workloads on non-transient errors`(statusCode: Int) {
    every { workloadApiClient.workloadList(any()) } throws
      ApiException(statusCode, "http://localhost.test", "")

    assertThrows<ApiException> { claimedProcessor.retrieveAndProcess("dataplane1") }
    verify(exactly = 0) { claimProcessorTracker.trackNumberOfClaimsToResume(any()) }
  }

  @Test
  fun `test retrieve and process recovers after network issues`() {
    every { workloadApiClient.workloadList(any()) }
      .throwsMany(
        listOf(
          ApiException(HttpStatus.INTERNAL_SERVER_ERROR.code, "http://localhost.test", ""),
          ApiException(HttpStatus.BAD_GATEWAY.code, "http://localhost.test", ""),
          SocketException(),
          ConnectException(),
          SocketTimeoutException(),
        ),
      ) andThenAnswer {
      WorkloadListResponse(
        listOf(
          Workload(
            id = "1",
            labels = mutableListOf(),
            inputPayload = "payload",
            logPath = "logPath",
            type = WorkloadType.SYNC,
            autoId = UUID.randomUUID(),
          ),
          Workload(
            id = "2",
            labels = mutableListOf(),
            inputPayload = "payload",
            logPath = "logPath",
            type = WorkloadType.SYNC,
            autoId = UUID.randomUUID(),
          ),
          Workload(
            id = "3",
            labels = mutableListOf(),
            inputPayload = "payload",
            logPath = "logPath",
            type = WorkloadType.SYNC,
            autoId = UUID.randomUUID(),
          ),
        ),
      )
    }
    claimedProcessor.retrieveAndProcess("dataplane1")

    verify { claimProcessorTracker.trackNumberOfClaimsToResume(3) }
    verify(exactly = 3) { launchPipeline.buildPipeline(any()) }
  }
}
