/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.generated.infrastructure.ServerException
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadListResponse
import io.airbyte.workload.api.client.model.generated.WorkloadType
import io.airbyte.workload.launcher.ClaimProcessorTracker
import io.airbyte.workload.launcher.ClaimedProcessor
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.kotlintest.milliseconds
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
  private lateinit var workloadApi: WorkloadApi
  private lateinit var claimedProcessor: ClaimedProcessor
  private lateinit var apiClient: WorkloadApiClient
  private lateinit var claimProcessorTracker: ClaimProcessorTracker
  private lateinit var launchPipeline: LaunchPipeline

  @BeforeEach
  fun setup() {
    workloadApi = mockk()
    apiClient =
      mockk {
        every { workloadApi } returns this@ClaimedProcessorTest.workloadApi
      }
    claimProcessorTracker = mockk(relaxed = true)
    launchPipeline =
      mockk(relaxed = true) {
        every { buildPipeline(any()) } returns Mono.empty()
      }
    claimedProcessor =
      ClaimedProcessor(
        apiClient = apiClient,
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
    every { workloadApi.workloadList(any()) } returns WorkloadListResponse(listOf())
    claimedProcessor.retrieveAndProcess("dataplane1")

    verify { claimProcessorTracker.trackNumberOfClaimsToResume(0) }
  }

  @Test
  fun `test retrieve and process with two workloads to resume`() {
    every { workloadApi.workloadList(any()) } returns
      WorkloadListResponse(
        listOf(
          Workload(
            id = "1",
            labels = listOf(),
            inputPayload = "payload",
            logPath = "logPath",
            type = WorkloadType.SYNC,
            autoId = UUID.randomUUID(),
          ),
          Workload(
            id = "2",
            labels = listOf(),
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
    every { workloadApi.workloadList(any()) } throws ServerException("Message shouldn't matter here", statusCode)

    assertThrows<ServerException> {
      claimedProcessor.retrieveAndProcess("dataplane1")
    }
    verify(exactly = 0) { claimProcessorTracker.trackNumberOfClaimsToResume(any()) }
  }

  @Test
  fun `test retrieve and process recovers after network issues`() {
    every { workloadApi.workloadList(any()) }
      .throwsMany(
        (1..5)
          .flatMap {
            listOf(
              ServerException("oops", 500),
              ServerException("oops", 502),
              SocketException(),
              ConnectException(),
              SocketTimeoutException(),
            )
          }.toList(),
      ).andThenAnswer {
        WorkloadListResponse(
          listOf(
            Workload(
              id = "1",
              labels = listOf(),
              inputPayload = "payload",
              logPath = "logPath",
              type = WorkloadType.SYNC,
              autoId = UUID.randomUUID(),
            ),
            Workload(
              id = "2",
              labels = listOf(),
              inputPayload = "payload",
              logPath = "logPath",
              type = WorkloadType.SYNC,
              autoId = UUID.randomUUID(),
            ),
            Workload(
              id = "3",
              labels = listOf(),
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
