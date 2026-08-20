/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import fixtures.RecordFixtures
import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.EnableAsyncProfiler
import io.airbyte.featureflag.ProfilingMode
import io.airbyte.featureflag.ShouldWaitForMainContainersOnReplication
import io.airbyte.featureflag.SocketTest
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.exception.ImagePullException
import io.airbyte.workers.exception.KubeClientException
import io.airbyte.workers.exception.KubeCommandType
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.launcher.constants.ContainerConstants
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
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.ContainerStateTerminatedBuilder
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.PodConditionBuilder
import io.fabric8.kubernetes.api.model.PodSpecBuilder
import io.fabric8.kubernetes.api.model.PodStatusBuilder
import io.mockk.Runs
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.just
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
  private lateinit var metricClient: MetricClient

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
        metricClient = metricClient,
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
    every { featureFlagClient.stringVariation(ProfilingMode, any()) } returns "cpu"
    every { featureFlagClient.boolVariation(SocketTest, any()) } returns false
    every { featureFlagClient.boolVariation(ShouldWaitForMainContainersOnReplication, any()) } returns true

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
    every { launcher.waitForPodInitComplete(any(), any()) } just Runs
    every { launcher.waitForPodReadyOrTerminalByPod(any(Pod::class), any()) } returns pod
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
  fun `launchReplication waits for main containers when feature flag is enabled`() {
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
    every { featureFlagClient.boolVariation(ShouldWaitForMainContainersOnReplication, any()) } returns true

    client.launchReplication(syncPayload, replLauncherInput)

    verify(exactly = 1) { launcher.waitForPodReadyOrTerminalByPod(pod, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) }
  }

  @Test
  fun `launchReplication skips waiting for main containers when feature flag is disabled`() {
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
    every { featureFlagClient.boolVariation(ShouldWaitForMainContainersOnReplication, any()) } returns false

    client.launchReplication(syncPayload, replLauncherInput)

    verify(exactly = 0) { launcher.waitForPodReadyOrTerminalByPod(any(), any()) }
  }

  @Test
  fun `launchReplication propagates image pull exception from main containers`() {
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
    every { featureFlagClient.boolVariation(ShouldWaitForMainContainersOnReplication, any()) } returns true
    every { launcher.waitForPodReadyOrTerminalByPod(pod, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) } throws
      ImagePullException("Failed to pull image", KubeCommandType.WAIT_MAIN)

    assertThrows<ImagePullException> {
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
  fun `launchReset waits for main containers when feature flag is enabled`() {
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
    every { featureFlagClient.boolVariation(ShouldWaitForMainContainersOnReplication, any()) } returns true

    client.launchReset(syncPayload, replLauncherInput)

    verify(exactly = 1) { launcher.waitForPodReadyOrTerminalByPod(pod, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) }
  }

  @Test
  fun `launchReset skips waiting for main containers when feature flag is disabled`() {
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
    every { featureFlagClient.boolVariation(ShouldWaitForMainContainersOnReplication, any()) } returns false

    client.launchReset(syncPayload, replLauncherInput)

    verify(exactly = 0) { launcher.waitForPodReadyOrTerminalByPod(any(), any()) }
  }

  @Test
  fun `launchReset propagates image pull exception from main containers`() {
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
    every { featureFlagClient.boolVariation(ShouldWaitForMainContainersOnReplication, any()) } returns true
    every { launcher.waitForPodReadyOrTerminalByPod(pod, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) } throws
      ImagePullException("Failed to pull image", KubeCommandType.WAIT_MAIN)

    assertThrows<ImagePullException> {
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
      podWithLifecycle(
        name = "connector-with-sidecar",
        creationTimestamp = "2026-01-01T00:00:00Z",
      )
    val initialized =
      podWithLifecycle(
        name = "connector-with-sidecar",
        creationTimestamp = "2026-01-01T00:00:00Z",
        scheduledTimestamp = "2026-01-01T00:00:02Z",
        initializedTimestamp = "2026-01-01T00:00:05Z",
      )
    val ready =
      podWithLifecycle(
        name = "connector-with-sidecar",
        creationTimestamp = "2026-01-01T00:00:00Z",
        scheduledTimestamp = "2026-01-01T00:00:02Z",
        initializedTimestamp = "2026-01-01T00:00:05Z",
        readyTimestamp = "2026-01-01T00:00:09Z",
        initContainerStartedTimestamp = "2026-01-01T00:00:03Z",
        initContainerFinishedTimestamp = "2026-01-01T00:00:05Z",
      )

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
    every { launcher.waitForPodInitComplete(connector, POD_INIT_TIMEOUT_VALUE) } just Runs
    every { launcher.waitForPodReadyOrTerminalByPod(connector, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE) } returns ready
    every { metricClient.distribution(any(), any(), *anyVararg()) } returns null

    client.launchConnectorWithSidecar(connectorKubeInput, podFactory, "OPERATION NAME")

    verify(exactly = 1) { launcher.waitForPodInitComplete(connector, POD_INIT_TIMEOUT_VALUE) }

    verify(exactly = 1) {
      launcher.waitForPodReadyOrTerminalByPod(connector, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE)
    }
    verify(exactly = 0) {
      launcher.waitForPodReadyOrTerminalByPod(initialized, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE)
    }

    val attributes =
      arrayOf(
        MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, "OPERATION NAME"),
        MetricAttribute(MetricTags.CONNECTOR_IMAGE, "airbyte/source-postgres"),
      )
    verify(exactly = 1) {
      metricClient.distribution(
        OssMetricsRegistry.WORKLOAD_LAUNCH_POD_CREATE_TO_SCHEDULED_DURATION,
        2.0,
        *attributes,
      )
    }
    verify(exactly = 1) {
      metricClient.distribution(
        OssMetricsRegistry.WORKLOAD_LAUNCH_POD_SCHEDULED_TO_INITIALIZED_DURATION,
        3.0,
        *attributes,
      )
    }
    verify(exactly = 1) {
      metricClient.distribution(
        OssMetricsRegistry.WORKLOAD_LAUNCH_POD_SCHEDULED_TO_INIT_CONTAINER_STARTED_DURATION,
        1.0,
        *attributes,
      )
    }
    verify(exactly = 1) {
      metricClient.distribution(
        OssMetricsRegistry.WORKLOAD_LAUNCH_POD_INIT_CONTAINER_STARTED_TO_FINISHED_DURATION,
        2.0,
        *attributes,
      )
    }
    verify(exactly = 1) {
      metricClient.distribution(
        OssMetricsRegistry.WORKLOAD_LAUNCH_POD_INITIALIZED_TO_READY_DURATION,
        4.0,
        *attributes,
      )
    }
  }

  private fun podWithLifecycle(
    name: String,
    creationTimestamp: String,
    scheduledTimestamp: String? = null,
    initializedTimestamp: String? = null,
    readyTimestamp: String? = null,
    initContainerStartedTimestamp: String? = null,
    initContainerFinishedTimestamp: String? = null,
  ): Pod =
    PodBuilder()
      .withNewMetadata()
      .withName(name)
      .withCreationTimestamp(creationTimestamp)
      .endMetadata()
      .build()
      .apply {
        spec =
          PodSpecBuilder()
            .withContainers(
              listOf(
                ContainerBuilder()
                  .withName(ContainerConstants.MAIN_CONTAINER_NAME)
                  .withImage("airbyte/source-postgres:3.6.1")
                  .build(),
              ),
            ).build()
        status =
          PodStatusBuilder()
            .withConditions(
              listOfNotNull(
                scheduledTimestamp?.let { podCondition("PodScheduled", it) },
                initializedTimestamp?.let { podCondition("Initialized", it) },
                readyTimestamp?.let { podCondition("Ready", it) },
              ),
            ).withInitContainerStatuses(
              listOfNotNull(
                if (initContainerStartedTimestamp != null && initContainerFinishedTimestamp != null) {
                  ContainerStatusBuilder()
                    .withName("init")
                    .withNewState()
                    .withTerminated(
                      ContainerStateTerminatedBuilder()
                        .withStartedAt(initContainerStartedTimestamp)
                        .withFinishedAt(initContainerFinishedTimestamp)
                        .withExitCode(0)
                        .build(),
                    ).endState()
                    .build()
                } else {
                  null
                },
              ),
            ).build()
      }

  private fun podCondition(
    type: String,
    timestamp: String,
  ) = PodConditionBuilder()
    .withType(type)
    .withStatus("True")
    .withLastTransitionTime(timestamp)
    .build()

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

    assertThrows<RuntimeException> {
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
