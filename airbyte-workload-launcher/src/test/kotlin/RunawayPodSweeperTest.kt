/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.WorkloadListActiveResponse
import io.airbyte.workload.api.domain.WorkloadSummary
import io.airbyte.workload.launcher.RunawayPodSweeper
import io.airbyte.workload.launcher.RunawayPodSweeper.Companion.DELETE_BY
import io.airbyte.workload.launcher.RunawayPodSweeper.Companion.DELETION_GRACE_PERIOD
import io.airbyte.workload.launcher.client.KubernetesClientWrapper
import io.airbyte.workload.launcher.pods.AUTO_ID
import io.fabric8.kubernetes.api.model.Pod
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Clock
import java.time.Instant
import java.util.UUID
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class RunawayPodSweeperTest {
  lateinit var runawayPodSweeper: RunawayPodSweeper
  lateinit var workloadApi: WorkloadApiClient
  lateinit var k8sWrapper: KubernetesClientWrapper
  lateinit var metricClient: MetricClient
  lateinit var clock: Clock

  @BeforeEach
  fun setup() {
    workloadApi = mockk()
    k8sWrapper = mockk()
    metricClient = mockk(relaxed = true)
    clock = mockk(relaxed = true)

    runawayPodSweeper =
      RunawayPodSweeper(
        workloadApi = workloadApi,
        k8sWrapper = k8sWrapper,
        metricClient = metricClient,
        clock = clock,
        featureFlagClient = mockk { every { boolVariation(any(), any()) } returns true },
        namespace = "yolo",
      )
  }

  @Test
  fun `mark should update delete-by only for the expected pods`() {
    val now = Instant.now()
    every { clock.instant() } returns now

    val activeAutoId1 = UUID.randomUUID().toString()
    val activeAutoId2 = UUID.randomUUID().toString()
    val activeWorkloads = listOf(WorkloadSummary(autoId = activeAutoId1), WorkloadSummary(autoId = activeAutoId2))
    every { workloadApi.workloadListActive(any()) } returns WorkloadListActiveResponse(activeWorkloads)

    val podToDelete1 = mockPod(autoId = UUID.randomUUID().toString())
    val podToDelete2 = mockPod(autoId = UUID.randomUUID().toString())
    val podMarkedForDelete = mockPod(autoId = UUID.randomUUID().toString(), deletedBy = "somewhere in the future")
    val pods =
      listOf(
        podToDelete1,
        mockPod(autoId = activeAutoId1),
        mockPod(autoId = UUID.randomUUID().toString(), podPhase = "Failed"),
        mockPod(autoId = UUID.randomUUID().toString(), podPhase = "Failed"),
        mockPod(autoId = activeAutoId2),
        podMarkedForDelete,
        podToDelete2,
      )
    every { k8sWrapper.listJobPods(any()) } returns mockk { every { items } returns pods }
    every { k8sWrapper.addLabelsToPod(podToDelete1, any()) } returns Unit
    every { k8sWrapper.addLabelsToPod(podToDelete2, any()) } returns Unit

    runawayPodSweeper.mark(UUID.randomUUID())

    verify(exactly = 1) { metricClient.count(OssMetricsRegistry.WORKLOAD_RUNAWAY_POD, 3) }

    val expectedDeleteBy = now.plus(DELETION_GRACE_PERIOD).epochSecond.toString()
    verify(exactly = 1) {
      k8sWrapper.addLabelsToPod(podToDelete1, mapOf(DELETE_BY to expectedDeleteBy))
      k8sWrapper.addLabelsToPod(podToDelete2, mapOf(DELETE_BY to expectedDeleteBy))
    }
  }

  @Test
  fun `mark should not do anything if the workload-list call fails`() {
    val now = Instant.now()
    every { clock.instant() } returns now

    every { workloadApi.workloadListActive(any()) } throws RuntimeException("fail to get workload")

    val deletablePod = mockPod(autoId = UUID.randomUUID().toString())
    val pods = listOf(deletablePod)
    every { k8sWrapper.listJobPods(any()) } returns mockk { every { items } returns pods }

    // This doesn't necessarily need to throw, what matters is that we never call addLabelsToPod
    assertThrows<RuntimeException> {
      runawayPodSweeper.mark(UUID.randomUUID())
    }

    verify(exactly = 0) {
      k8sWrapper.addLabelsToPod(any(), any())
    }
  }

  @Test
  fun `sweep should only delete pods where delete-by is in the past`() {
    val now = Instant.now()
    every { clock.instant() } returns now

    val podToDelete = mockPod(autoId = UUID.randomUUID().toString(), deletedBy = now.minus(1.seconds.toJavaDuration()).epochSecond.toString())
    val survivorPod = mockPod(autoId = UUID.randomUUID().toString(), deletedBy = now.plus(1.seconds.toJavaDuration()).epochSecond.toString())
    val podWithBadDeleteBy = mockPod(autoId = UUID.randomUUID().toString(), deletedBy = "not a number")
    every { k8sWrapper.listJobPods(any(), any()) } returns mockk { every { items } returns listOf(podWithBadDeleteBy, podToDelete, survivorPod) }
    every { k8sWrapper.deletePod(podToDelete, any(), any()) } returns true

    runawayPodSweeper.sweep(UUID.randomUUID())

    verify(exactly = 1) { k8sWrapper.deletePod(podToDelete, any(), any()) }
    verify(exactly = 0) { k8sWrapper.deletePod(podWithBadDeleteBy, any(), any()) }
    verify(exactly = 0) { k8sWrapper.deletePod(survivorPod, any(), any()) }
  }

  private fun mockPod(
    autoId: String,
    podPhase: String = "Running",
    deletedBy: String? = null,
  ) = mockk<Pod>(relaxed = true) {
    every { metadata } returns
      mockk {
        every { name } returns "pod-$autoId"
        every { labels } returns mapOf(AUTO_ID to autoId, DELETE_BY to deletedBy)
      }
    every { status } returns
      mockk {
        every { phase } returns podPhase
      }
  }
}
