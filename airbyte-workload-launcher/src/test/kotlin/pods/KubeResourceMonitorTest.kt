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
import io.temporal.worker.WorkerFactory
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
    val workerFactory: WorkerFactory = mockk()

    mockMetricPublisherAndWorkerFactory(customMetricPublisher, workerFactory)

    mockAboveAllowedTime(pod, kubernetesClient, namespace)

    val kubeResourceMonitor = KubeResourceMonitor(kubernetesClient, namespace, pendingTimeLimitSec, customMetricPublisher, workerFactory)

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
    verify(exactly = 1) { workerFactory.suspendPolling() }
  }

  @Test
  fun `test that when the oldest pending pod does not exceed the allowed time, a metric is not recorded`() {
    val kubernetesClient: KubernetesClient = mockk()
    val namespace = "namespace"
    val pendingTimeLimitSec = 30L
    val customMetricPublisher: CustomMetricPublisher = mockk()
    val pod: Pod = mockk()
    val workerFactory: WorkerFactory = mockk()

    mockMetricPublisherAndWorkerFactory(customMetricPublisher, workerFactory)

    mockBellowAllowedTime(pod, kubernetesClient, namespace)

    val kubeResourceMonitor = KubeResourceMonitor(kubernetesClient, namespace, pendingTimeLimitSec, customMetricPublisher, workerFactory)

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

  @Test
  fun `test that when a pending pod exceeds the allowed time, the polling is suspended and resumed if the allowed time goes back to normal`() {
    val kubernetesClient: KubernetesClient = mockk()
    val namespace = "namespace"
    val pendingTimeLimitSec = 30L
    val customMetricPublisher: CustomMetricPublisher = mockk()
    val pod: Pod = mockk()
    val workerFactory: WorkerFactory = mockk()

    mockMetricPublisherAndWorkerFactory(customMetricPublisher, workerFactory)

    mockAboveAllowedTime(pod, kubernetesClient, namespace)

    val kubeResourceMonitor = KubeResourceMonitor(kubernetesClient, namespace, pendingTimeLimitSec, customMetricPublisher, workerFactory)

    kubeResourceMonitor.checkKubernetesResources()

    verify(exactly = 1) { workerFactory.suspendPolling() }

    mockBellowAllowedTime(pod, kubernetesClient, namespace)

    kubeResourceMonitor.checkKubernetesResources()

    verify(exactly = 1) { workerFactory.resumePolling() }
  }

  private fun mockMetricPublisherAndWorkerFactory(
    customMetricPublisher: CustomMetricPublisher,
    workerFactory: WorkerFactory,
  ) {
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
    every {
      customMetricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_LAUNCHER_POLLER_STATUS, any())
    } returns Unit
    every { workerFactory.suspendPolling() } returns Unit
    every { workerFactory.resumePolling() } returns Unit
  }

  private fun mockAboveAllowedTime(
    pod: Pod,
    kubernetesClient: KubernetesClient,
    namespace: String,
  ) {
    val pods = listOf(pod)

    every { pod.status } returns
      mockk {
        every { conditions } returns
          listOf(
            mockk {
              every { lastTransitionTime } returns Instant.ofEpochMilli(0L).toString()
            },
          )
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
  }

  private fun mockBellowAllowedTime(
    pod: Pod,
    kubernetesClient: KubernetesClient,
    namespace: String,
  ) {
    val pods = listOf(pod)
    every { pod.status } returns
      mockk {
        every { conditions } returns
          listOf(
            mockk {
              every { lastTransitionTime } returns java.time.Instant.now().toString()
            },
          )
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
  }
}
