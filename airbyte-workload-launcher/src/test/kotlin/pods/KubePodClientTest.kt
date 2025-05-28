/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import fixtures.RecordFixtures
import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.EnableAsyncProfiler
import io.airbyte.featureflag.SocketTest
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.exception.KubeClientException
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pods.KubePodClient.Companion.POD_INIT_TIMEOUT_VALUE
import io.airbyte.workload.launcher.pods.KubePodClient.Companion.REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.WORKLOAD_ID
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.checkLauncherInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.connectorKubeInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.discoverLauncherInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.replLauncherInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.replicationKubeInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.sharedLabels
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.specLauncherInput
import io.airbyte.workload.launcher.pods.KubePodClientTest.Fixtures.workspaceId
import io.airbyte.workload.launcher.pods.factories.ConnectorPodFactory
import io.airbyte.workload.launcher.pods.factories.ReplicationPodFactory
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import java.util.UUID
import java.util.concurrent.TimeoutException

@ExtendWith(MockKExtension::class)
internal class KubePodClientTest {
  @MockK
  private lateinit var launcher: KubePodLauncher

  @MockK
  private lateinit var labeler: PodLabeler

  @MockK
  private lateinit var mapper: PayloadKubeInputMapper

  @MockK
  private lateinit var pod: Pod

  @MockK
  private lateinit var replicationPodFactory: ReplicationPodFactory

  @MockK
  private lateinit var checkPodFactory: ConnectorPodFactory

  @MockK
  private lateinit var discoverPodFactory: ConnectorPodFactory

  @MockK
  private lateinit var specPodFactory: ConnectorPodFactory

  @MockK
  private lateinit var podFactory: ConnectorPodFactory

  @MockK
  private lateinit var featureFlagClient: TestClient

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
        kubePodLauncher = launcher,
        labeler = labeler,
        mapper = mapper,
        replicationPodFactory = replicationPodFactory,
        checkPodFactory = checkPodFactory,
        discoverPodFactory = discoverPodFactory,
        specPodFactory = specPodFactory,
        featureFlagClient = featureFlagClient,
      )

    replInput =
      ReplicationInput()
        .withSourceLauncherConfig(IntegrationLauncherConfig())
        .withDestinationLauncherConfig(IntegrationLauncherConfig())
        .withConnectionId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withSourceConfiguration(Jsons.emptyObject())

    resetInput =
      ReplicationInput()
        .withSourceLauncherConfig(IntegrationLauncherConfig())
        .withDestinationLauncherConfig(IntegrationLauncherConfig())
        .withIsReset(true)
        .withConnectionId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)

    checkInput =
      CheckConnectionInput(
        JobRunConfig().withJobId("jobId").withAttemptId(1),
        IntegrationLauncherConfig().withDockerImage("dockerImage").withWorkspaceId(workspaceId),
        StandardCheckConnectionInput(),
      )

    discoverInput =
      DiscoverCatalogInput(
        JobRunConfig().withJobId("jobId").withAttemptId(1),
        IntegrationLauncherConfig().withDockerImage("dockerImage").withWorkspaceId(workspaceId),
        StandardDiscoverCatalogInput(),
      )

    specInput =
      SpecInput(
        JobRunConfig().withJobId("jobId").withAttemptId(1),
        IntegrationLauncherConfig().withDockerImage("dockerImage").withWorkspaceId(workspaceId),
      )

    every { labeler.getSharedLabels(any(), any(), any(), any(), any(), any()) } returns sharedLabels

    every { featureFlagClient.boolVariation(EnableAsyncProfiler, any()) } returns false
    every { featureFlagClient.boolVariation(SocketTest, any()) } returns false

    every { mapper.toKubeInput(WORKLOAD_ID, checkInput, sharedLabels) } returns connectorKubeInput
    every { mapper.toKubeInput(WORKLOAD_ID, discoverInput, sharedLabels) } returns connectorKubeInput
    every { mapper.toKubeInput(WORKLOAD_ID, specInput, sharedLabels) } returns connectorKubeInput

    every {
      podFactory.create(
        connectorKubeInput.connectorLabels,
        connectorKubeInput.nodeSelectors,
        connectorKubeInput.kubePodInfo,
        connectorKubeInput.annotations,
        connectorKubeInput.connectorReqs,
        connectorKubeInput.initReqs,
        connectorKubeInput.runtimeEnvVars,
        any(),
      )
    } returns pod

    val slot = slot<Pod>()
    every { launcher.create(capture(slot)) } answers { slot.captured }
    every { launcher.waitForPodInitStartup(any(), any()) } returns Unit
    every { launcher.waitForPodInitComplete(any(), any()) } returns Unit
    every { launcher.waitForPodReadyOrTerminalByPod(any(Pod::class), any()) } returns Unit
    every { launcher.waitForPodReadyOrTerminal(any(), any()) } returns Unit
  }

  @Test
  fun `launchReplication happy path`() {
    val kubeInput =
      ReplicationKubeInput(
        podName = "podName",
        labels = mapOf("label" to "value"),
        annotations = mapOf("annotation" to "value"),
        nodeSelectors = mapOf("selector" to "value"),
        orchestratorImage = "orch-image",
        sourceImage = "source-image",
        destinationImage = "destination-image",
        orchestratorReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        sourceReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        destinationReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        initReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        orchestratorRuntimeEnvVars = listOf(EnvVar("name", "value", null)),
        sourceRuntimeEnvVars = listOf(EnvVar("name", "value", null)),
        destinationRuntimeEnvVars = listOf(EnvVar("name", "value", null)),
      )
    val syncPayload = SyncPayload(replInput)
    every { mapper.toKubeInput(WORKLOAD_ID, syncPayload, any()) } returns kubeInput
    every {
      replicationPodFactory.create(
        kubeInput.podName,
        kubeInput.labels,
        kubeInput.annotations,
        kubeInput.nodeSelectors,
        kubeInput.orchestratorImage,
        kubeInput.sourceImage,
        kubeInput.destinationImage,
        kubeInput.orchestratorReqs,
        kubeInput.sourceReqs,
        kubeInput.destinationReqs,
        kubeInput.orchestratorRuntimeEnvVars,
        kubeInput.sourceRuntimeEnvVars,
        kubeInput.destinationRuntimeEnvVars,
        false,
        workspaceId,
      )
    } returns pod
    client.launchReplication(
      payload = syncPayload,
      launcherInput = replLauncherInput,
    )

    verify(exactly = 1) { launcher.create(pod) }
    verify(exactly = 1) { launcher.waitForPodInitComplete(pod, POD_INIT_TIMEOUT_VALUE) }
  }

  @Test
  fun `launchReplication happy path with exposed ports`() {
    val kubeInput =
      ReplicationKubeInput(
        podName = "podName",
        labels = mapOf("label" to "value"),
        annotations = mapOf("annotation" to "value"),
        nodeSelectors = mapOf("selector" to "value"),
        orchestratorImage = "orch-image",
        sourceImage = "source-image",
        destinationImage = "destination-image",
        orchestratorReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        sourceReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        destinationReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        initReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        orchestratorRuntimeEnvVars = listOf(EnvVar("name", "value", null)),
        sourceRuntimeEnvVars = listOf(EnvVar("name", "value", null)),
        destinationRuntimeEnvVars = listOf(EnvVar("name", "value", null)),
      )
    val syncPayload = SyncPayload(replInput)
    every { mapper.toKubeInput(WORKLOAD_ID, syncPayload, any()) } returns kubeInput
    every {
      replicationPodFactory.create(
        kubeInput.podName,
        kubeInput.labels,
        kubeInput.annotations,
        kubeInput.nodeSelectors,
        kubeInput.orchestratorImage,
        kubeInput.sourceImage,
        kubeInput.destinationImage,
        kubeInput.orchestratorReqs,
        kubeInput.sourceReqs,
        kubeInput.destinationReqs,
        kubeInput.orchestratorRuntimeEnvVars,
        kubeInput.sourceRuntimeEnvVars,
        kubeInput.destinationRuntimeEnvVars,
        false,
        workspaceId,
        false,
      )
    } returns pod
    client.launchReplication(
      payload = syncPayload,
      launcherInput = replLauncherInput,
    )

    verify(exactly = 1) { launcher.create(pod) }
    verify(exactly = 1) { launcher.waitForPodInitComplete(pod, POD_INIT_TIMEOUT_VALUE) }
  }

  @Test
  fun `launchReplication propagates pod creation error`() {
    val syncPayload = SyncPayload(replInput)
    every { mapper.toKubeInput(WORKLOAD_ID, syncPayload, any()) } returns replicationKubeInput
    every {
      replicationPodFactory.create(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    } returns Pod()
    every { launcher.create(any()) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchReplication(syncPayload, replLauncherInput)
    }
  }

  @Test
  fun `launchReplication propagates pod wait for init timeout as kube exception`() {
    val syncPayload = SyncPayload(replInput)
    every { mapper.toKubeInput(WORKLOAD_ID, syncPayload, any()) } returns replicationKubeInput
    every {
      replicationPodFactory.create(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    } returns pod
    every { launcher.waitForPodInitComplete(pod, POD_INIT_TIMEOUT_VALUE) } throws TimeoutException("bang")

    assertThrows<KubeClientException> {
      client.launchReplication(syncPayload, replLauncherInput)
    }
  }

  @Test
  fun `launchReset happy path`() {
    val kubeInput =
      ReplicationKubeInput(
        podName = "podName",
        labels = mapOf("label" to "value"),
        annotations = mapOf("annotation" to "value"),
        nodeSelectors = mapOf("selector" to "value"),
        orchestratorImage = "orch-image",
        sourceImage = "source-image",
        destinationImage = "destination-image",
        orchestratorReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        sourceReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        destinationReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        initReqs = mockk<io.fabric8.kubernetes.api.model.ResourceRequirements>(),
        orchestratorRuntimeEnvVars = listOf(EnvVar("name", "value", null)),
        sourceRuntimeEnvVars = listOf(EnvVar("name", "value", null)),
        destinationRuntimeEnvVars = listOf(EnvVar("name", "value", null)),
      )
    val syncPayload = SyncPayload(replInput)
    every { mapper.toKubeInput(WORKLOAD_ID, syncPayload, any()) } returns kubeInput
    every {
      replicationPodFactory.createReset(
        kubeInput.podName,
        kubeInput.labels,
        kubeInput.annotations,
        kubeInput.nodeSelectors,
        kubeInput.orchestratorImage,
        kubeInput.destinationImage,
        kubeInput.orchestratorReqs,
        kubeInput.destinationReqs,
        kubeInput.orchestratorRuntimeEnvVars,
        kubeInput.destinationRuntimeEnvVars,
        false,
        workspaceId,
      )
    } returns pod
    client.launchReset(
      payload = syncPayload,
      launcherInput = replLauncherInput,
    )

    verify(exactly = 1) { launcher.create(pod) }
    verify(exactly = 1) { launcher.waitForPodInitComplete(pod, POD_INIT_TIMEOUT_VALUE) }
  }

  @Test
  fun `launchReset propagates pod creation error`() {
    val syncPayload = SyncPayload(replInput)
    every { mapper.toKubeInput(WORKLOAD_ID, syncPayload, any()) } returns replicationKubeInput
    every {
      replicationPodFactory.createReset(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    } returns Pod()
    every { launcher.create(any()) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchReset(syncPayload, replLauncherInput)
    }
  }

  @Test
  fun `launchReset propagates pod wait for init timeout as kube exception`() {
    val syncPayload = SyncPayload(replInput)
    every { mapper.toKubeInput(WORKLOAD_ID, syncPayload, any()) } returns replicationKubeInput
    every {
      replicationPodFactory.createReset(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    } returns pod
    every { launcher.waitForPodInitComplete(pod, POD_INIT_TIMEOUT_VALUE) } throws TimeoutException("bang")

    assertThrows<KubeClientException> {
      client.launchReset(syncPayload, replLauncherInput)
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
        connectorKubeInput.connectorReqs,
        connectorKubeInput.initReqs,
        connectorKubeInput.runtimeEnvVars,
        workspaceId,
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
        connectorKubeInput.connectorReqs,
        connectorKubeInput.initReqs,
        connectorKubeInput.runtimeEnvVars,
        workspaceId,
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
        connectorKubeInput.connectorReqs,
        connectorKubeInput.initReqs,
        connectorKubeInput.runtimeEnvVars,
        workspaceId,
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
        connectorKubeInput.connectorReqs,
        connectorKubeInput.initReqs,
        connectorKubeInput.runtimeEnvVars,
        workspaceId,
      )
    } returns connector

    client.launchConnectorWithSidecar(connectorKubeInput, podFactory, "OPERATION NAME")

    verify { launcher.waitForPodInitComplete(connector, POD_INIT_TIMEOUT_VALUE) }

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
  fun `launchConnectorWithSidecar propagates wait for init timeout as kube exception`() {
    every { launcher.waitForPodInitComplete(pod, POD_INIT_TIMEOUT_VALUE) } throws TimeoutException("bang")

    assertThrows<KubeClientException> {
      client.launchConnectorWithSidecar(connectorKubeInput, podFactory, "OPERATION NAME")
    }
  }

  @Test
  fun `launchConnectorWithSidecar propagates init failures as normal errors`() {
    every { launcher.waitForPodInitComplete(pod, POD_INIT_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<RuntimeException> {
      client.launchConnectorWithSidecar(connectorKubeInput, podFactory, "OPERATION NAME")
    }
  }

  @Test
  fun `launchConnectorWithSidecar propagates connector wait for init error`() {
    every { launcher.waitForPodReadyOrTerminalByPod(pod, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) } throws RuntimeException("bang")

    assertThrows<KubeClientException> {
      client.launchConnectorWithSidecar(connectorKubeInput, podFactory, "OPERATION NAME")
    }
  }

  object Fixtures {
    val replicationKubeInput =
      ReplicationKubeInput(
        "pod-name",
        emptyMap(),
        emptyMap(),
        emptyMap(),
        "orchestrator-image",
        "source-image",
        "destination-image",
        io.fabric8.kubernetes.api.model
          .ResourceRequirements(),
        io.fabric8.kubernetes.api.model
          .ResourceRequirements(),
        io.fabric8.kubernetes.api.model
          .ResourceRequirements(),
        io.fabric8.kubernetes.api.model
          .ResourceRequirements(),
        emptyList(),
        emptyList(),
        emptyList(),
      )

    val workspaceId = UUID.randomUUID()
    val connectorKubeInput =
      ConnectorKubeInput(
        mapOf("test-connector-label" to "val2"),
        mapOf("test-selector" to "val3"),
        KubePodInfo("test-namespace", "test-name", null),
        mapOf("test-annotation" to "val5"),
        io.fabric8.kubernetes.api.model
          .ResourceRequirements(),
        io.fabric8.kubernetes.api.model
          .ResourceRequirements(),
        listOf(EnvVar("extra-env", "val6", null)),
        workspaceId,
      )

    const val WORKLOAD_ID = "workload-id"
    private val passThroughLabels = mapOf("labels" to "we get", "from" to "the activity")
    val sharedLabels = mapOf("arbitrary" to "label", "literally" to "anything")

    val replLauncherInput = RecordFixtures.launcherInput(workloadId = WORKLOAD_ID, labels = passThroughLabels)
    val checkLauncherInput =
      RecordFixtures.launcherInput(
        workloadId = WORKLOAD_ID,
        labels = passThroughLabels,
        workloadType = WorkloadType.CHECK,
      )
    val discoverLauncherInput =
      RecordFixtures.launcherInput(
        workloadId = WORKLOAD_ID,
        labels = passThroughLabels,
        workloadType = WorkloadType.DISCOVER,
      )
    val specLauncherInput =
      RecordFixtures.launcherInput(
        workloadId = WORKLOAD_ID,
        labels = passThroughLabels,
        workloadType = WorkloadType.SPEC,
      )
  }
}
