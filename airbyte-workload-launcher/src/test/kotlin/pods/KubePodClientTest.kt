package io.airbyte.workload.launcher.pods

import fixtures.RecordFixtures
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workload.launcher.model.setDestinationLabels
import io.airbyte.workload.launcher.model.setSourceLabels
import io.airbyte.workload.launcher.pods.KubePodClient.Companion.ORCHESTRATOR_STARTUP_TIMEOUT_VALUE
import io.airbyte.workload.launcher.pods.KubePodClient.Companion.POD_INIT_TIMEOUT_VALUE
import io.airbyte.workload.launcher.pods.KubePodClient.Companion.REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.checkLauncherInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.connectorKubeInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.discoverLauncherInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.replKubeInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.replLauncherInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.sharedLabels
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.specLauncherInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.workloadId
import io.airbyte.workload.launcher.pods.factories.ConnectorPodFactory
import io.airbyte.workload.launcher.pods.factories.OrchestratorPodFactory
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import java.lang.RuntimeException
import java.util.UUID

@ExtendWith(MockKExtension::class)
class KubePodClientTest {
  @MockK
  private lateinit var launcher: KubePodLauncher

  @MockK
  private lateinit var labeler: PodLabeler

  @MockK
  private lateinit var mapper: PayloadKubeInputMapper

  @MockK
  private lateinit var pod: Pod

  @MockK
  private lateinit var orchestratorPodFactory: OrchestratorPodFactory

  @MockK
  private lateinit var checkPodFactory: ConnectorPodFactory

  @MockK
  private lateinit var discoverPodFactory: ConnectorPodFactory

  @MockK
  private lateinit var specPodFactory: ConnectorPodFactory

  @MockK
  private lateinit var podFactory: ConnectorPodFactory

  private lateinit var client: KubePodClient

  private lateinit var replInput: ReplicationInput

  private lateinit var resetInput: ReplicationInput

  private lateinit var checkInput: CheckConnectionInput

  private lateinit var discoverInput: DiscoverCatalogInput

  private lateinit var specInput: SpecInput

  @BeforeEach
  fun setup() {
    client =
      KubePodClient(
        launcher,
        labeler,
        mapper,
        featureFlagClient = TestClient(emptyMap()),
        orchestratorPodFactory,
        checkPodFactory,
        discoverPodFactory,
        specPodFactory,
      )

    replInput =
      ReplicationInput()
        .withSourceLauncherConfig(IntegrationLauncherConfig())
        .withDestinationLauncherConfig(IntegrationLauncherConfig())
        .withConnectionId(UUID.randomUUID())

    resetInput =
      ReplicationInput()
        .withSourceLauncherConfig(IntegrationLauncherConfig())
        .withDestinationLauncherConfig(IntegrationLauncherConfig())
        .withIsReset(true)
        .withConnectionId(UUID.randomUUID())

    checkInput =
      CheckConnectionInput(
        JobRunConfig().withJobId("jobId").withAttemptId(1),
        IntegrationLauncherConfig().withDockerImage("dockerImage"),
        null,
      )

    discoverInput =
      DiscoverCatalogInput(
        JobRunConfig().withJobId("jobId").withAttemptId(1),
        IntegrationLauncherConfig().withDockerImage("dockerImage"),
        null,
      )

    specInput =
      SpecInput(JobRunConfig().withJobId("jobId").withAttemptId(1), IntegrationLauncherConfig().withDockerImage("dockerImage"))

    every { labeler.getSharedLabels(any(), any(), any(), any()) } returns sharedLabels

    every { mapper.toKubeInput(workloadId, replInput, sharedLabels) } returns replKubeInput
    every { mapper.toKubeInput(workloadId, resetInput, sharedLabels) } returns replKubeInput
    every { mapper.toKubeInput(workloadId, checkInput, sharedLabels) } returns connectorKubeInput
    every { mapper.toKubeInput(workloadId, discoverInput, sharedLabels) } returns connectorKubeInput
    every { mapper.toKubeInput(workloadId, specInput, sharedLabels) } returns connectorKubeInput

    every {
      orchestratorPodFactory.create(
        any(),
        replKubeInput.orchestratorLabels,
        replKubeInput.resourceReqs,
        replKubeInput.nodeSelectors,
        replKubeInput.kubePodInfo,
        replKubeInput.annotations,
        mapOf(),
      )
    } returns pod

    every {
      podFactory.create(
        connectorKubeInput.connectorLabels,
        connectorKubeInput.nodeSelectors,
        connectorKubeInput.kubePodInfo,
        connectorKubeInput.annotations,
        connectorKubeInput.extraEnv,
      )
    } returns pod

    val slot = slot<Pod>()
    every { launcher.create(capture(slot)) } answers { slot.captured }
    every { launcher.waitForPodInit(any(), any()) } returns Unit
    every { launcher.copyFilesToKubeConfigVolumeMain(any(), any()) } returns Unit
    every { launcher.waitForPodReadyOrTerminalByPod(any(Pod::class), any()) } returns Unit
    every { launcher.waitForPodReadyOrTerminal(any(), any()) } returns Unit
  }

  @Test
  fun `launchReplication starts an orchestrator and waits on all 3 pods`() {
    val orchestrator =
      PodBuilder()
        .withNewMetadata()
        .withName("special")
        .endMetadata()
        .build()

    every {
      orchestratorPodFactory.create(
        any(),
        replKubeInput.orchestratorLabels,
        replKubeInput.resourceReqs,
        replKubeInput.nodeSelectors,
        replKubeInput.kubePodInfo,
        replKubeInput.annotations,
        mapOf(),
      )
    } returns orchestrator

    client.launchReplication(replInput, replLauncherInput)

    verify { launcher.create(orchestrator) }

    verify { launcher.waitForPodInit(orchestrator, POD_INIT_TIMEOUT_VALUE) }

    verify { launcher.copyFilesToKubeConfigVolumeMain(orchestrator, replKubeInput.fileMap) }

    verify { launcher.waitForPodReadyOrTerminal(replKubeInput.destinationLabels, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) }

    verify { launcher.waitForPodReadyOrTerminalByPod(orchestrator, ORCHESTRATOR_STARTUP_TIMEOUT_VALUE) }

    verify { launcher.waitForPodReadyOrTerminal(replKubeInput.sourceLabels, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) }
  }

  @Test
  fun `launchReplication starts an orchestrator and waits on all 2 pods for resets`() {
    val orchestrator =
      PodBuilder()
        .withNewMetadata()
        .withName("special")
        .endMetadata()
        .build()

    every {
      orchestratorPodFactory.create(
        any(),
        replKubeInput.orchestratorLabels,
        replKubeInput.resourceReqs,
        replKubeInput.nodeSelectors,
        replKubeInput.kubePodInfo,
        replKubeInput.annotations,
        mapOf(),
      )
    } returns orchestrator

    client.launchReplication(resetInput, replLauncherInput)

    verify { launcher.waitForPodInit(orchestrator, POD_INIT_TIMEOUT_VALUE) }

    verify { launcher.copyFilesToKubeConfigVolumeMain(orchestrator, replKubeInput.fileMap) }

    verify(exactly = 0) { launcher.waitForPodReadyOrTerminal(replKubeInput.sourceLabels, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) }

    verify { launcher.waitForPodReadyOrTerminal(replKubeInput.destinationLabels, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) }
  }

  @Test
  fun `launchReplication sets pass-through labels for propagation to source and destination`() {
    every { labeler.getSharedLabels(any(), any(), any(), any()) } returns sharedLabels
    every { mapper.toKubeInput(workloadId, replInput, sharedLabels) } returns replKubeInput

    client.launchReplication(replInput, replLauncherInput)

    val inputWithLabels = replInput.setDestinationLabels(sharedLabels).setSourceLabels(sharedLabels)

    verify { mapper.toKubeInput(workloadId, inputWithLabels, sharedLabels) }
  }

  @Test
  fun `launchReplication propagates orchestrator creation error`() {
    every { launcher.create(any()) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchReplication(replInput, replLauncherInput)
    }
  }

  @Test
  fun `launchReplication propagates orchestrator wait for init error`() {
    every { launcher.waitForPodInit(pod, POD_INIT_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchReplication(replInput, replLauncherInput)
    }
  }

  @Test
  fun `launchReplication propagates orchestrator copy file map error`() {
    every { launcher.copyFilesToKubeConfigVolumeMain(any(), replKubeInput.fileMap) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchReplication(replInput, replLauncherInput)
    }
  }

  @Test
  fun `launchReplication propagates source wait for init error`() {
    every { launcher.waitForPodReadyOrTerminal(replKubeInput.sourceLabels, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchReplication(replInput, replLauncherInput)
    }
  }

  @Test
  fun `launchReplication propagates destination wait for init error`() {
    every {
      launcher.waitForPodReadyOrTerminal(
        replKubeInput.destinationLabels,
        REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE,
      )
    } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchReplication(replInput, replLauncherInput)
    }
  }

  @Test
  fun `launchCheck delegates to launchConnectorWithSidecar`() {
    client = spyk(client)

    every {
      checkPodFactory.create(
        connectorKubeInput.connectorLabels,
        connectorKubeInput.nodeSelectors,
        connectorKubeInput.kubePodInfo,
        connectorKubeInput.annotations,
        connectorKubeInput.extraEnv,
      )
    } returns pod

    client.launchCheck(checkInput, checkLauncherInput)

    verify { client.launchConnectorWithSidecar(connectorKubeInput, checkPodFactory, "CHECK") }
  }

  @Test
  fun `launchDiscover delegates to launchConnectorWithSidecar`() {
    client = spyk(client)

    every {
      discoverPodFactory.create(
        connectorKubeInput.connectorLabels,
        connectorKubeInput.nodeSelectors,
        connectorKubeInput.kubePodInfo,
        connectorKubeInput.annotations,
        connectorKubeInput.extraEnv,
      )
    } returns pod

    client.launchDiscover(discoverInput, discoverLauncherInput)

    verify { client.launchConnectorWithSidecar(connectorKubeInput, discoverPodFactory, "DISCOVER") }
  }

  @Test
  fun `launchSpec delegates to launchConnectorWithSidecar`() {
    client = spyk(client)

    every {
      specPodFactory.create(
        connectorKubeInput.connectorLabels,
        connectorKubeInput.nodeSelectors,
        connectorKubeInput.kubePodInfo,
        connectorKubeInput.annotations,
        connectorKubeInput.extraEnv,
      )
    } returns pod

    client.launchSpec(specInput, specLauncherInput)

    verify { client.launchConnectorWithSidecar(connectorKubeInput, specPodFactory, "SPEC") }
  }

  @Test
  fun `launchConnectorWithSidecar starts a pod and waits on it`() {
    val connector =
      PodBuilder()
        .withNewMetadata()
        .withName("connector-with-sidecar")
        .endMetadata()
        .build()

    every {
      podFactory.create(
        connectorKubeInput.connectorLabels,
        connectorKubeInput.nodeSelectors,
        connectorKubeInput.kubePodInfo,
        connectorKubeInput.annotations,
        connectorKubeInput.extraEnv,
      )
    } returns connector

    client.launchConnectorWithSidecar(connectorKubeInput, podFactory, "OPERATION NAME")

    verify { launcher.waitForPodInit(connector, POD_INIT_TIMEOUT_VALUE) }

    verify { launcher.copyFilesToKubeConfigVolumeMain(connector, connectorKubeInput.fileMap) }

    verify { launcher.waitForPodReadyOrTerminalByPod(connector, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) }
  }

  @Test
  fun `launchConnectorWithSidecar propagates pod creation error`() {
    every { launcher.create(any()) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchConnectorWithSidecar(connectorKubeInput, podFactory, "OPERATION NAME")
    }
  }

  @Test
  fun `launchConnectorWithSidecar propagates pod wait for init error`() {
    every { launcher.waitForPodInit(pod, POD_INIT_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchConnectorWithSidecar(connectorKubeInput, podFactory, "OPERATION NAME")
    }
  }

  @Test
  fun `launchConnectorWithSidecar propagates orchestrator copy file map error`() {
    every { launcher.copyFilesToKubeConfigVolumeMain(any(), connectorKubeInput.fileMap) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchConnectorWithSidecar(connectorKubeInput, podFactory, "OPERATION NAME")
    }
  }

  @Test
  fun `launchConnectorWithSidecar propagates source wait for init error`() {
    every { launcher.waitForPodReadyOrTerminalByPod(pod, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchConnectorWithSidecar(connectorKubeInput, podFactory, "OPERATION NAME")
    }
  }

  object Fixtures {
    val replKubeInput =
      OrchestratorKubeInput(
        mapOf("test-orch-label" to "val1"),
        mapOf("test-source-label" to "val2"),
        mapOf("test-dest-label" to "val3"),
        mapOf("test-selector" to "val4"),
        KubePodInfo("test-namespace", "test-name", null),
        mapOf("test-file" to "val5"),
        ResourceRequirements().withCpuRequest("test-cpu").withMemoryRequest("test-mem"),
        mapOf("test-annotation" to "val6"),
      )

    val connectorKubeInput =
      ConnectorKubeInput(
        mapOf("test-connector-label" to "val2"),
        mapOf("test-selector" to "val3"),
        KubePodInfo("test-namespace", "test-name", null),
        mapOf("test-file" to "val4"),
        mapOf("test-annotation" to "val5"),
        listOf(EnvVar("extra-env", "val6", null)),
      )

    val workloadId = "workload-id"
    val passThroughLabels = mapOf("labels" to "we get", "from" to "the activity")
    val sharedLabels = mapOf("arbitrary" to "label", "literally" to "anything")

    val replLauncherInput = RecordFixtures.launcherInput(workloadId = workloadId, labels = passThroughLabels)
    val checkLauncherInput =
      RecordFixtures.launcherInput(
        workloadId = workloadId,
        labels = passThroughLabels,
        workloadType = WorkloadType.CHECK,
      )
    val discoverLauncherInput =
      RecordFixtures.launcherInput(
        workloadId = workloadId,
        labels = passThroughLabels,
        workloadType = WorkloadType.DISCOVER,
      )
    val specLauncherInput =
      RecordFixtures.launcherInput(
        workloadId = workloadId,
        labels = passThroughLabels,
        workloadType = WorkloadType.SPEC,
      )
  }
}
