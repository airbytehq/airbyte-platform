package io.airbyte.workload.launcher.pods

import fixtures.RecordFixtures
import io.airbyte.config.ResourceRequirements
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workload.launcher.model.setConnectorLabels
import io.airbyte.workload.launcher.model.setDestinationLabels
import io.airbyte.workload.launcher.model.setSourceLabels
import io.airbyte.workload.launcher.pods.KubePodClient.Companion.CONNECTOR_STARTUP_TIMEOUT_VALUE
import io.airbyte.workload.launcher.pods.KubePodClient.Companion.ORCHESTRATOR_INIT_TIMEOUT_VALUE
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.checkKubeInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.launcherInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.replKubeInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.sharedLabels
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
  private lateinit var labeler: PodLabeler

  @MockK
  private lateinit var mapper: PayloadKubeInputMapper

  @MockK
  private lateinit var pod: Pod

  private lateinit var client: KubePodClient

  private lateinit var replInput: ReplicationInput

  private lateinit var checkInput: CheckConnectionInput

  @BeforeEach
  fun setup() {
    client =
      KubePodClient(
        launcher,
        labeler,
        mapper,
      )

    replInput =
      ReplicationInput()
        .withSourceLauncherConfig(IntegrationLauncherConfig())
        .withDestinationLauncherConfig(IntegrationLauncherConfig())

    checkInput =
      CheckConnectionInput(null, IntegrationLauncherConfig(), null)

    every { labeler.getSharedLabels(any(), any(), any()) } returns sharedLabels

    every { mapper.toKubeInput(replInput, sharedLabels) } returns replKubeInput

    every { mapper.toKubeInput(checkInput, sharedLabels) } returns checkKubeInput

    every { launcher.create(any(), any(), any(), any(), any()) } returns pod
    every { launcher.waitForPodInit(any(), any()) } returns Unit
    every { launcher.copyFilesToKubeConfigVolumeMain(any(), any()) } returns Unit
    every { launcher.waitForPodReadyOrTerminal(any(), any()) } returns Unit
  }

  @Test
  fun `launchReplication starts an orchestrator and waits on all 3 pods`() {
    client.launchReplication(replInput, launcherInput)

    verify {
      launcher.create(
        replKubeInput.orchestratorLabels,
        replKubeInput.resourceReqs,
        replKubeInput.nodeSelectors,
        replKubeInput.kubePodInfo,
        replKubeInput.annotations,
      )
    }

    verify { launcher.waitForPodInit(replKubeInput.orchestratorLabels, ORCHESTRATOR_INIT_TIMEOUT_VALUE) }

    verify { launcher.copyFilesToKubeConfigVolumeMain(pod, replKubeInput.fileMap) }

    verify { launcher.waitForPodReadyOrTerminal(replKubeInput.sourceLabels, CONNECTOR_STARTUP_TIMEOUT_VALUE) }

    verify { launcher.waitForPodReadyOrTerminal(replKubeInput.destinationLabels, CONNECTOR_STARTUP_TIMEOUT_VALUE) }
  }

  @Test
  fun `launchReplication sets pass-through labels for propagation to source and destination`() {
    every { labeler.getSharedLabels(any(), any(), any()) } returns sharedLabels
    every { mapper.toKubeInput(replInput, sharedLabels) } returns replKubeInput

    client.launchReplication(replInput, launcherInput)

    val inputWithLabels = replInput.setDestinationLabels(sharedLabels).setSourceLabels(sharedLabels)

    verify { mapper.toKubeInput(inputWithLabels, sharedLabels) }
  }

  @Test
  fun `launchReplication propagates orchestrator creation error`() {
    every { launcher.create(any(), any(), any(), any(), any()) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchReplication(replInput, launcherInput)
    }
  }

  @Test
  fun `launchReplication propagates orchestrator wait for init error`() {
    every { launcher.waitForPodInit(replKubeInput.orchestratorLabels, ORCHESTRATOR_INIT_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchReplication(replInput, launcherInput)
    }
  }

  @Test
  fun `launchReplication propagates orchestrator copy file map error`() {
    every { launcher.copyFilesToKubeConfigVolumeMain(any(), replKubeInput.fileMap) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchReplication(replInput, launcherInput)
    }
  }

  @Test
  fun `launchReplication propagates source wait for init error`() {
    every { launcher.waitForPodReadyOrTerminal(replKubeInput.sourceLabels, CONNECTOR_STARTUP_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchReplication(replInput, launcherInput)
    }
  }

  @Test
  fun `launchReplication propagates destination wait for init error`() {
    every { launcher.waitForPodReadyOrTerminal(replKubeInput.destinationLabels, CONNECTOR_STARTUP_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchReplication(replInput, launcherInput)
    }
  }

  @Test
  fun `launchCheck starts an orchestrator and waits on both pods`() {
    client.launchCheck(checkInput, launcherInput)

    verify {
      launcher.create(
        checkKubeInput.orchestratorLabels,
        checkKubeInput.resourceReqs,
        checkKubeInput.nodeSelectors,
        checkKubeInput.kubePodInfo,
        checkKubeInput.annotations,
      )
    }

    verify { launcher.waitForPodInit(checkKubeInput.orchestratorLabels, ORCHESTRATOR_INIT_TIMEOUT_VALUE) }

    verify { launcher.copyFilesToKubeConfigVolumeMain(pod, checkKubeInput.fileMap) }

    verify { launcher.waitForPodReadyOrTerminal(checkKubeInput.connectorLabels, CONNECTOR_STARTUP_TIMEOUT_VALUE) }
  }

  @Test
  fun `launchCheck sets pass-through labels for propagation to connector`() {
    every { labeler.getSharedLabels(any(), any(), any()) } returns sharedLabels
    every { mapper.toKubeInput(checkInput, sharedLabels) } returns checkKubeInput

    client.launchCheck(checkInput, launcherInput)

    val inputWithLabels = checkInput.setConnectorLabels(sharedLabels)

    verify { mapper.toKubeInput(inputWithLabels, sharedLabels) }
  }

  @Test
  fun `launchCheck propagates orchestrator creation error`() {
    every { launcher.create(any(), any(), any(), any(), any()) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchCheck(checkInput, launcherInput)
    }
  }

  @Test
  fun `launchCheck propagates orchestrator wait for init error`() {
    every { launcher.waitForPodInit(checkKubeInput.orchestratorLabels, ORCHESTRATOR_INIT_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchCheck(checkInput, launcherInput)
    }
  }

  @Test
  fun `launchCheck propagates orchestrator copy file map error`() {
    every { launcher.copyFilesToKubeConfigVolumeMain(any(), checkKubeInput.fileMap) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchCheck(checkInput, launcherInput)
    }
  }

  @Test
  fun `launchCheck propagates source wait for init error`() {
    every { launcher.waitForPodReadyOrTerminal(checkKubeInput.connectorLabels, CONNECTOR_STARTUP_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<KubePodInitException> {
      client.launchCheck(checkInput, launcherInput)
    }
  }

  object Fixtures {
    val replKubeInput =
      ReplicationOrchestratorKubeInput(
        mapOf("test-orch-label" to "val1"),
        mapOf("test-source-label" to "val2"),
        mapOf("test-dest-label" to "val3"),
        mapOf("test-selector" to "val4"),
        KubePodInfo("test-namespace", "test-name", null),
        mapOf("test-file" to "val5"),
        ResourceRequirements().withCpuRequest("test-cpu").withMemoryRequest("test-mem"),
        mapOf("test-annotation" to "val6"),
      )

    val checkKubeInput =
      CheckOrchestratorKubeInput(
        mapOf("test-orch-label" to "val1"),
        mapOf("test-connector-label" to "val2"),
        mapOf("test-selector" to "val3"),
        KubePodInfo("test-namespace", "test-name", null),
        mapOf("test-file" to "val4"),
        ResourceRequirements().withCpuRequest("test-cpu").withMemoryRequest("test-mem"),
        mapOf("test-annotation" to "val5"),
      )

    val workloadId = "workload-id"
    val passThroughLabels = mapOf("labels" to "we get", "from" to "the activity")
    val sharedLabels = mapOf("arbitrary" to "label", "literally" to "anything")

    val launcherInput = RecordFixtures.launcherInput(workloadId = workloadId, labels = passThroughLabels)
  }
}
