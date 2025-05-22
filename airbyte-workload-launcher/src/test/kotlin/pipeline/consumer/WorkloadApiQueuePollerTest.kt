/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.consumer

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadLabel
import io.airbyte.workload.api.client.model.generated.WorkloadPriority
import io.airbyte.workload.api.client.model.generated.WorkloadType
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.model.toLauncherInput
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueuePollerTest.Fixtures.groupId
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueuePollerTest.Fixtures.workload1
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueuePollerTest.Fixtures.workload2
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueuePollerTest.Fixtures.workload3
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueuePollerTest.Fixtures.workload4
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueuePollerTest.Fixtures.workload5
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import reactor.test.StepVerifier
import java.time.Duration
import java.util.UUID

@ExtendWith(MockKExtension::class)
class WorkloadApiQueuePollerTest {
  @MockK(relaxed = true)
  lateinit var workloadApiClient: WorkloadApiClient

  @MockK(relaxed = true)
  lateinit var metricClient: MetricClient

  @MockK(relaxed = true)
  lateinit var featureFlagClient: FeatureFlagClient

  private val pollSizeItems: Int = 10

  private val pollIntervalSeconds: Long = 5

  private val priority: WorkloadPriority = WorkloadPriority.DEFAULT

  lateinit var poller: WorkloadApiQueuePoller

  @BeforeEach
  fun setup() {
    poller =
      WorkloadApiQueuePoller(
        workloadApiClient,
        metricClient,
        featureFlagClient,
        pollSizeItems,
        pollIntervalSeconds,
        priority,
      )
  }

  @Test
  fun `creates an input flux that polls with configured args at the configured interval`() {
    every { workloadApiClient.pollQueue(groupId, priority, pollSizeItems) } returns
      listOf(
        workload1,
        workload2,
      ) andThen
      listOf(
        workload3,
        workload4,
      ) andThen
      listOf(
        workload5,
      )

    StepVerifier
      .withVirtualTime {
        poller.initialize(groupId)
        poller.resumePolling()
        poller.flux.take(5) // take the first 5 so the flux terminates
      }.thenAwait(Duration.ofSeconds(pollIntervalSeconds))
      .expectNext(workload1.toLauncherInput())
      .expectNext(workload2.toLauncherInput())
      .expectNoEvent(Duration.ofSeconds(pollIntervalSeconds - 1))
      .thenAwait(Duration.ofSeconds(1))
      .expectNext(workload3.toLauncherInput())
      .expectNext(workload4.toLauncherInput())
      .expectNoEvent(Duration.ofSeconds(pollIntervalSeconds - 1))
      .thenAwait(Duration.ofSeconds(1))
      .expectNext(workload5.toLauncherInput())
      .verifyComplete()

    verify(exactly = 3) { workloadApiClient.pollQueue(groupId, priority, pollSizeItems) }
  }

  @Test
  fun `polling can be suspended and resumed`() {
    every { workloadApiClient.pollQueue(groupId, priority, pollSizeItems) } returns
      listOf(
        workload1,
        workload2,
      ) andThen
      listOf(
        workload3,
        workload4,
      )

    StepVerifier
      .withVirtualTime {
        poller.initialize(groupId)
        poller.resumePolling()
        poller.flux.take(4)
      }.thenAwait(Duration.ofSeconds(pollIntervalSeconds))
      .expectNext(workload1.toLauncherInput())
      .expectNext(workload2.toLauncherInput())
      .then { poller.suspendPolling() }
      .expectNoEvent(Duration.ofSeconds(pollIntervalSeconds * 2))
      .then { poller.resumePolling() }
      .thenAwait(Duration.ofSeconds(pollIntervalSeconds))
      .expectNext(workload3.toLauncherInput())
      .expectNext(workload4.toLauncherInput())
      .verifyComplete()

    verify(exactly = 2) { workloadApiClient.pollQueue(groupId, priority, pollSizeItems) }
  }

  @Test
  fun `handles errors and continues polling`() {
    every { workloadApiClient.pollQueue(groupId, priority, pollSizeItems) } returns
      listOf(
        workload1,
        workload2,
      ) andThenThrows RuntimeException("bang") andThen
      listOf( // kotlin won't let me break these lines :/
        workload3,
        workload4,
      )

    StepVerifier
      .withVirtualTime {
        poller.initialize(groupId)
        poller.resumePolling()
        poller.flux.take(4) // take 4 so we terminate the flux
      }.thenAwait(Duration.ofSeconds(pollIntervalSeconds))
      .expectNext(workload1.toLauncherInput())
      .expectNext(workload2.toLauncherInput())
      .expectNoEvent(Duration.ofSeconds(pollIntervalSeconds)) // exception happens here
      .thenAwait(Duration.ofSeconds(pollIntervalSeconds))
      .expectNext(workload3.toLauncherInput())
      .expectNext(workload4.toLauncherInput())
      .verifyComplete()

    verify(exactly = 3) { workloadApiClient.pollQueue(groupId, priority, pollSizeItems) }
  }

  object Fixtures {
    val groupId = "dataplane-group-1"

    val workload1 = workload("workload-1")
    val workload2 = workload("workload-2")
    val workload3 = workload("workload-3")
    val workload4 = workload("workload-4")
    val workload5 = workload("workload-5")

    fun workload(
      id: String = "1234_test",
      labels: List<WorkloadLabel> = listOf(),
      inputPayload: String = "payload",
      logPath: String = "/path",
      type: WorkloadType = WorkloadType.SYNC,
      autoId: UUID = UUID.randomUUID(),
    ): Workload =
      Workload(
        id = id,
        labels = labels,
        inputPayload = inputPayload,
        logPath = logPath,
        type = type,
        autoId = autoId,
      )
  }
}
