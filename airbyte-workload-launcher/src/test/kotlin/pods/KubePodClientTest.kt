package io.airbyte.workload.launcher.pods

import io.airbyte.config.ResourceRequirements
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.kubeInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.workloadId
import io.fabric8.kubernetes.api.model.Pod
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import java.lang.RuntimeException

@ExtendWith(MockKExtension::class)
class KubePodClientTest {
  @MockK
  private lateinit var launcher: OrchestratorPodLauncher

  @MockK
  private lateinit var mapper: PayloadKubeInputMapper

  @MockK
  private lateinit var input: ReplicationInput

  @MockK
  private lateinit var pod: Pod

  private lateinit var client: KubePodClient

  @BeforeEach
  fun setup() {
    client =
      KubePodClient(
        launcher,
        mapper,
      )

    every { mapper.toKubeInput(input, workloadId) } returns kubeInput

    every { launcher.create(any(), any(), any(), any()) } returns pod
    every { launcher.waitForPodsWithLabels(any()) } returns Unit
    every { launcher.copyFilesToKubeConfigVolumeMain(any(), any()) } returns Unit
  }

  @Test
  fun `launchReplication starts an orchestrator and waits on all 3 pods`() {
    client.launchReplication(input, workloadId)

    verify {
      launcher.create(
        kubeInput.orchestratorLabels,
        kubeInput.resourceReqs,
        kubeInput.nodeSelectors,
        kubeInput.kubePodInfo,
      )
    }

    verify { launcher.waitForPodsWithLabels(kubeInput.orchestratorLabels) }

    verify { launcher.copyFilesToKubeConfigVolumeMain(pod, kubeInput.fileMap) }

    verify { launcher.waitForPodsWithLabels(kubeInput.sourceLabels) }

    verify { launcher.waitForPodsWithLabels(kubeInput.destinationLabels) }
  }

  @Test
  fun `launchReplication propagates orchestrator creation error`() {
    every { launcher.create(any(), any(), any(), any()) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchReplication(input, workloadId)
    }
  }

  @Test
  fun `launchReplication propagates orchestrator wait for init error`() {
    every { launcher.waitForPodsWithLabels(kubeInput.orchestratorLabels) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchReplication(input, workloadId)
    }
  }

  @Test
  fun `launchReplication propagates orchestrator copy file map error`() {
    every { launcher.copyFilesToKubeConfigVolumeMain(any(), kubeInput.fileMap) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchReplication(input, workloadId)
    }
  }

  @Test
  fun `launchReplication propagates source wait for init error`() {
    every { launcher.waitForPodsWithLabels(kubeInput.sourceLabels) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchReplication(input, workloadId)
    }
  }

  @Test
  fun `launchReplication propagates destination wait for init error`() {
    every { launcher.waitForPodsWithLabels(kubeInput.destinationLabels) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchReplication(input, workloadId)
    }
  }

  object Fixtures {
    val kubeInput =
      OrchestratorKubeInput(
        mapOf("test-orch-label" to "val1"),
        mapOf("test-source-label" to "val2"),
        mapOf("test-dest-label" to "val3"),
        mapOf("test-selector" to "val4"),
        KubePodInfo("test-namespace", "test-name", null),
        mapOf("test-file" to "val5"),
        ResourceRequirements().withCpuRequest("test-cpu").withMemoryRequest("test-mem"),
      )
    val workloadId = "workload-id"
  }
}
