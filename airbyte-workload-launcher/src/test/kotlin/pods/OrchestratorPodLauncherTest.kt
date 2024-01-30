package pods

import dev.failsafe.RetryPolicy
import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workload.launcher.pods.OrchestratorPodLauncher
import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.api.model.ObjectMeta
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodList
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientException
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable
import io.fabric8.kubernetes.client.dsl.MixedOperation
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation
import io.fabric8.kubernetes.client.dsl.PodResource
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@ExtendWith(MockKExtension::class)
class OrchestratorPodLauncherTest {
  @MockK
  private lateinit var kubernetesClient: KubernetesClient

  @MockK
  private lateinit var featureFlagClient: FeatureFlagClient

  @MockK
  private lateinit var metricClient: MetricClient

  private lateinit var orchestratorPodLauncher: OrchestratorPodLauncher

  private lateinit var kubernetesClientRetryPolicy: RetryPolicy<Any>

  @BeforeEach
  fun setup() {
    kubernetesClientRetryPolicy = RetryPolicy.ofDefaults()
    orchestratorPodLauncher =
      OrchestratorPodLauncher(
        kubernetesClient,
        featureFlagClient,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        listOf(),
        listOf(),
        metricClient,
        kubernetesClientRetryPolicy,
      )

    every { featureFlagClient.stringVariation(any(), any()) } returns ""
    every { kubernetesClient.pods() } throws IllegalStateException()
    every { kubernetesClient.resource(any<Pod>()) } throws IllegalStateException()
    every { metricClient.count(any(), any(), any()) } returns Unit
  }

  @Test
  fun `test fail to create pod`() {
    assertThrows<IllegalStateException> {
      orchestratorPodLauncher.create(
        mapOf(),
        ResourceRequirements(),
        mapOf(),
        KubePodInfo("", "", KubeContainerInfo("", "")),
        mapOf(),
        mapOf(),
      )
    }

    checkMetricSend("pod_create")
  }

  @Test
  fun `test fail to wait for pod init`() {
    val pod: Pod = mockk()

    assertThrows<IllegalStateException> {
      orchestratorPodLauncher.waitForPodInit(
        pod,
        Duration.ZERO,
      )
    }

    checkMetricSend("wait")
  }

  @Test
  fun `test fail to wait for pod ready or terminal`() {
    assertThrows<IllegalStateException> {
      orchestratorPodLauncher.waitForPodReadyOrTerminal(
        mapOf(),
        Duration.ZERO,
      )
    }

    checkMetricSend("wait")
  }

  @Test
  fun `test fail to check if pod exist`() {
    assertFalse(orchestratorPodLauncher.podsExist(mapOf()))

    checkMetricSend("list")
  }

  @Test
  fun `test fail to delete pod`() {
    assertThrows<IllegalStateException> {
      orchestratorPodLauncher.deleteActivePods(
        mapOf(),
      )
    }

    checkMetricSend("delete")
  }

  @Test
  fun `test retry on socket timeout exception`() {
    val maxRetries = 3
    val counter = AtomicInteger(0)
    val kubernetesClientRetryPolicy =
      RetryPolicy.builder<Any>()
        .handleIf { e -> e.cause is SocketTimeoutException }
        .onRetry { counter.incrementAndGet() }
        .withMaxRetries(maxRetries)
        .build()

    val pods: MixedOperation<Pod, PodList, PodResource> = mockk()
    val namespaceable: NonNamespaceOperation<Pod, PodList, PodResource> = mockk()
    val labels: FilterWatchListDeletable<Pod, PodList, PodResource> = mockk()

    every { pods.inNamespace(any()) } returns namespaceable
    every { namespaceable.withLabels(any()) } returns labels
    every { labels.waitUntilCondition(any(), any(), any()) } throws
      KubernetesClientException("An error has occurred", SocketTimeoutException("timeout"))
    every { kubernetesClient.pods() } returns pods

    val orchestratorPodLauncher =
      OrchestratorPodLauncher(
        kubernetesClient,
        featureFlagClient,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        listOf(),
        listOf(),
        metricClient,
        kubernetesClientRetryPolicy,
      )

    assertThrows<KubernetesClientException> {
      orchestratorPodLauncher.waitForPodReadyOrTerminal(mapOf("label" to "value"), Duration.ofSeconds(30))
    }
    assertEquals(maxRetries, counter.get())
  }

  @Test
  fun `test retry is skipped on non-socket timeout exception`() {
    val maxRetries = 3
    val counter = AtomicInteger(0)
    val kubernetesClientRetryPolicy =
      RetryPolicy.builder<Any>()
        .handleIf { e -> e.cause is SocketTimeoutException }
        .onRetry { counter.incrementAndGet() }
        .withMaxRetries(maxRetries)
        .build()

    val pods: MixedOperation<Pod, PodList, PodResource> = mockk()
    val namespaceable: NonNamespaceOperation<Pod, PodList, PodResource> = mockk()
    val labels: FilterWatchListDeletable<Pod, PodList, PodResource> = mockk()
    val hasMetadata: HasMetadata = mockk()

    every { hasMetadata.kind } returns "kind"
    every { hasMetadata.metadata } returns ObjectMeta()
    every { pods.inNamespace(any()) } returns namespaceable
    every { namespaceable.withLabels(any()) } returns labels
    every { labels.waitUntilCondition(any(), any(), any()) } throws
      KubernetesClientTimeoutException(hasMetadata, 2L, TimeUnit.SECONDS)
    every { kubernetesClient.pods() } returns pods

    val orchestratorPodLauncher =
      OrchestratorPodLauncher(
        kubernetesClient,
        featureFlagClient,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        listOf(),
        listOf(),
        metricClient,
        kubernetesClientRetryPolicy,
      )

    assertThrows<KubernetesClientException> {
      orchestratorPodLauncher.waitForPodReadyOrTerminal(mapOf("label" to "value"), Duration.ofSeconds(30))
    }
    assertEquals(0, counter.get())
  }

  private fun checkMetricSend(tag: String) {
    val attributes: List<MetricAttribute> = listOf(MetricAttribute("operation", tag))
    val attributesArray = attributes.toTypedArray<MetricAttribute>()
    verify {
      metricClient.count(OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_ERROR, 1, *attributesArray)
    }
  }
}
