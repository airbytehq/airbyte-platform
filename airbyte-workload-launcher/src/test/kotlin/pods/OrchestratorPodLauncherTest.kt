package pods

import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workload.launcher.pods.OrchestratorPodLauncher
import io.fabric8.kubernetes.client.KubernetesClient
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import java.lang.IllegalStateException
import java.time.Duration

@ExtendWith(MockKExtension::class)
class OrchestratorPodLauncherTest {
  @MockK
  private lateinit var kubernetesClient: KubernetesClient

  @MockK
  private lateinit var featureFlagClient: FeatureFlagClient

  @MockK
  private lateinit var metricClient: MetricClient

  private lateinit var orchestratorPodLauncher: OrchestratorPodLauncher

  @BeforeEach
  fun setup() {
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
      )

    every { featureFlagClient.stringVariation(any(), any()) } returns ""
    every { kubernetesClient.pods() } throws IllegalStateException()
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
      )
    }

    checkMetricSend("pod_create")
  }

  @Test
  fun `test fail to wait for pod init`() {
    assertThrows<IllegalStateException> {
      orchestratorPodLauncher.waitForPodInit(
        mapOf(),
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

  private fun checkMetricSend(tag: String) {
    val attributes: List<MetricAttribute> = listOf(MetricAttribute("operation", tag))
    val attributesArray = attributes.toTypedArray<MetricAttribute>()
    verify {
      metricClient.count(OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_ERROR, 1, *attributesArray)
    }
  }
}
