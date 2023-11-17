/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pods.KubeResourceMonitor.Companion.PENDING
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodList
import io.fabric8.kubernetes.client.KubernetesClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.Instant

class KubeResourceMonitorTest {
  @Test
  fun `test that when a pending pod exceeds the allowed time, a metric is recorded`() {
    val kubernetesClient: KubernetesClient = mockk()
    val namespace = "namespace"
    val pendingTimeLimitSec = 30L
    val customMetricPublisher: CustomMetricPublisher = mockk()
    val pod: Pod = mockk()
    val pods = listOf(pod)

    every {
      customMetricPublisher.gauge(
        WorkloadLauncherMetricMetadata.TOTAL_PENDING_PODS, any(PodList::class),
        any(),
      )
    } returns Unit
    every {
      customMetricPublisher.gauge(
        WorkloadLauncherMetricMetadata.OLDEST_PENDING_JOB_POD_TIME, any(Long::class),
        any(),
      )
    } returns Unit

    every { pod.status } returns
      mockk {
        every { startTime } returns Instant.ofEpochMilli(0L).toString()
      }

    every { kubernetesClient.pods() } returns
      mockk {
        every { inNamespace(namespace) } returns
          mockk {
            every { withField(KubeResourceMonitor.STATUS_PHASE, PENDING) } returns
              mockk {
                every { list() } returns
                  mockk {
                    every { items } returns pods
                  }
              }
          }
      }

    val kubeResourceMonitor = KubeResourceMonitor(kubernetesClient, namespace, pendingTimeLimitSec, customMetricPublisher)

    kubeResourceMonitor.checkKubernetesResources()

    verify(exactly = 1) {
      customMetricPublisher.gauge(
        WorkloadLauncherMetricMetadata.TOTAL_PENDING_PODS,
        any(PodList::class),
        any(),
      )
    }
    verify(exactly = 1) {
      customMetricPublisher.gauge(
        WorkloadLauncherMetricMetadata.OLDEST_PENDING_JOB_POD_TIME,
        any(Long::class),
        any(),
      )
    }
  }

  @Test
  fun `test that when the oldest pending pod does not exceed the allowed time, a metric is not recorded`() {
    val kubernetesClient: KubernetesClient = mockk()
    val namespace = "namespace"
    val pendingTimeLimitSec = 30L
    val customMetricPublisher: CustomMetricPublisher = mockk()
    val pod: Pod = mockk()
    val pods = listOf(pod)

    every {
      customMetricPublisher.gauge(
        WorkloadLauncherMetricMetadata.TOTAL_PENDING_PODS, any(PodList::class),
        any(),
      )
    } returns Unit
    every {
      customMetricPublisher.gauge(
        WorkloadLauncherMetricMetadata.OLDEST_PENDING_JOB_POD_TIME, any(Long::class),
        any(),
      )
    } returns Unit

    every { pod.status } returns
      mockk {
        every { startTime } returns Instant.now().toString()
      }

    every { kubernetesClient.pods() } returns
      mockk {
        every { inNamespace(namespace) } returns
          mockk {
            every { withField(KubeResourceMonitor.STATUS_PHASE, PENDING) } returns
              mockk {
                every { list() } returns
                  mockk {
                    every { items } returns pods
                  }
              }
          }
      }

    val kubeResourceMonitor = KubeResourceMonitor(kubernetesClient, namespace, pendingTimeLimitSec, customMetricPublisher)

    kubeResourceMonitor.checkKubernetesResources()

    verify(exactly = 1) {
      customMetricPublisher.gauge(
        WorkloadLauncherMetricMetadata.TOTAL_PENDING_PODS,
        any(PodList::class),
        any(),
      )
    }
    verify(exactly = 0) {
      customMetricPublisher.gauge(
        WorkloadLauncherMetricMetadata.OLDEST_PENDING_JOB_POD_TIME,
        any(Long::class),
        any(),
      )
    }
  }
}
