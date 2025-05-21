/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorType
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.ConnectorApmEnabled
import io.airbyte.featureflag.ContainerOrchestratorDevImage
import io.airbyte.featureflag.NodeSelectorOverride
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.getActorType
import io.airbyte.workers.input.getAttemptId
import io.airbyte.workers.input.getJobId
import io.airbyte.workers.input.getOrganizationId
import io.airbyte.workers.input.usesCustomConnector
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pods.factories.ResourceRequirementsFactory
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactory
import io.fabric8.kubernetes.api.model.EnvVar
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID
import java.util.stream.Stream

internal class PayloadKubeInputMapperTest {
  @ParameterizedTest
  @MethodSource("replicationFlagsInputMatrix")
  fun `builds a kube input from a replication payload`(useCustomConnector: Boolean) {
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val imageRegistry = null
    val podName = "a-repl-pod"
    val podNameGenerator: PodNameGenerator = mockk()
    every { podNameGenerator.getReplicationPodName(any(), any()) } returns podName
    val containerInfo = KubeContainerInfo("img-name", "pull-policy")
    val envVarFactory: RuntimeEnvVarFactory = mockk()
    val replSelectors = mapOf("test-selector" to "normal-repl")
    val replCustomSelectors = mapOf("test-selector" to "custom-repl")
    val checkConfigs: WorkerConfigs = mockk()
    val discoverConfigs: WorkerConfigs = mockk()
    val specConfigs: WorkerConfigs = mockk()
    val replConfigs: WorkerConfigs = mockk()
    every { replConfigs.workerKubeNodeSelectors } returns replSelectors
    every { replConfigs.workerIsolatedKubeNodeSelectors } returns replCustomSelectors
    val annotations = mapOf("annotation" to "value2")
    every { replConfigs.workerKubeAnnotations } returns annotations
    val ffClient: TestClient = mockk()
    every { ffClient.stringVariation(ContainerOrchestratorDevImage, any()) } returns ""
    every { ffClient.stringVariation(NodeSelectorOverride, any()) } returns ""
    every { ffClient.boolVariation(ConnectorApmEnabled, any()) } returns false
    val resourceReqFactory: ResourceRequirementsFactory = mockk()
    val nodeSelector = KubeNodeSelector(ffClient, listOf())

    val mapper =
      PayloadKubeInputMapper(
        labeler,
        podNameGenerator,
        namespace,
        imageRegistry,
        containerInfo,
        replConfigs,
        checkConfigs,
        discoverConfigs,
        specConfigs,
        resourceReqFactory,
        envVarFactory,
        ffClient,
        nodeSelector,
      )
    val input: ReplicationInput = mockk()

    mockkStatic("io.airbyte.workers.input.ReplicationInputExtensionsKt")
    val jobId = "415"
    val attemptId = 7654L
    val workloadId = UUID.randomUUID().toString()
    val resourceReqs1 =
      ResourceRequirements()
        .withCpuLimit("1")
        .withMemoryRequest("7Mi")
    val resourceReqs2 =
      ResourceRequirements()
        .withCpuLimit("2")
        .withMemoryRequest("3Mi")
    val resourceReqs3 =
      ResourceRequirements()
        .withCpuLimit("2")
        .withMemoryRequest("300Mi")
    val resourceReqs4 =
      ResourceRequirements()
        .withCpuLimit("1.5")
        .withMemoryRequest("500Mi")
    val srcLauncherConfig =
      IntegrationLauncherConfig()
        .withDockerImage("src-docker-img")
    val destLauncherConfig =
      IntegrationLauncherConfig()
        .withDockerImage("dest-docker-img")
    val expectedSrcRuntimeEnvVars =
      listOf(
        EnvVar("env-1", "val-1", null),
        EnvVar("env-2", "val-2", null),
      )
    val expectedDestRuntimeEnvVars =
      listOf(
        EnvVar("env-3", "val-3", null),
      )
    val expectedOrchestratorRuntimeEnvVars =
      listOf(
        EnvVar("env-4", "val-4", null),
        EnvVar("env-5", "val-5", null),
        EnvVar("env-6", "val-6", null),
        EnvVar("env-7", "val-7", null),
      )

    every { envVarFactory.replicationConnectorEnvVars(srcLauncherConfig, resourceReqs2, any()) } returns expectedSrcRuntimeEnvVars
    every { envVarFactory.replicationConnectorEnvVars(destLauncherConfig, resourceReqs3, any()) } returns expectedDestRuntimeEnvVars
    every { envVarFactory.orchestratorEnvVars(input, workloadId) } returns expectedOrchestratorRuntimeEnvVars

    every { resourceReqFactory.orchestrator(input) } returns resourceReqs1
    every { resourceReqFactory.replSource(input) } returns resourceReqs2
    every { resourceReqFactory.replDestination(input) } returns resourceReqs3
    every { resourceReqFactory.replInit(input) } returns resourceReqs4
    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.usesCustomConnector() } returns useCustomConnector
    every { input.jobRunConfig } returns mockk<JobRunConfig>()
    every { input.sourceLauncherConfig } returns srcLauncherConfig
    every { input.destinationLauncherConfig } returns destLauncherConfig
    every { input.connectionId } returns mockk<UUID>()
    every { input.workspaceId } returns mockk<UUID>()
    every { input.useFileTransfer } returns false
    val syncPayload = SyncPayload(input)

    val replLabels = mapOf("orchestrator" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every {
      labeler.getReplicationLabels(
        containerInfo.image,
        srcLauncherConfig.dockerImage,
        destLauncherConfig.dockerImage,
      )
    } returns replLabels
    val result = mapper.toKubeInput(workloadId, syncPayload, sharedLabels)

    assertEquals(podName, result.podName)
    assertEquals(replLabels + sharedLabels, result.labels)
    assertEquals(if (useCustomConnector) replCustomSelectors else replSelectors, result.nodeSelectors)
    assertEquals(annotations, result.annotations)
    assertEquals(containerInfo.image, result.orchestratorImage)
    assertEquals(srcLauncherConfig.dockerImage, result.sourceImage)
    assertEquals(destLauncherConfig.dockerImage, result.destinationImage)
    assertEquals(ResourceConversionUtils.domainToApi(resourceReqs1), result.orchestratorReqs)
    assertEquals(ResourceConversionUtils.domainToApi(resourceReqs2), result.sourceReqs)
    assertEquals(ResourceConversionUtils.domainToApi(resourceReqs3), result.destinationReqs)
    assertEquals(ResourceConversionUtils.domainToApi(resourceReqs4), result.initReqs)

    assertEquals(expectedOrchestratorRuntimeEnvVars, result.orchestratorRuntimeEnvVars)
    assertEquals(expectedSrcRuntimeEnvVars, result.sourceRuntimeEnvVars)
    assertEquals(expectedDestRuntimeEnvVars, result.destinationRuntimeEnvVars)
  }

  @ParameterizedTest
  @MethodSource("connectorInputMatrix")
  fun `builds a kube input from a check payload`(
    customConnector: Boolean,
    workloadPriority: WorkloadPriority,
  ) {
    val ffClient = TestClient()

    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val imageRegistry = null
    val podName = "check-pod"
    val podNameGenerator: PodNameGenerator = mockk()
    every { podNameGenerator.getCheckPodName(any(), any(), any()) } returns podName
    val orchestratorContainerInfo = KubeContainerInfo("img-name", "pull-policy")
    val envVarFactory: RuntimeEnvVarFactory = mockk()
    val checkSelectors = mapOf("test-selector" to "normal-check")
    val pullPolicy = "pull-policy"
    val checkCustomSelectors = mapOf("test-selector" to "custom-check")
    val checkConfigs: WorkerConfigs = mockk()
    every { checkConfigs.workerKubeAnnotations } returns mapOf("annotation" to "value1")
    every { checkConfigs.workerIsolatedKubeNodeSelectors } returns checkCustomSelectors
    every { checkConfigs.workerKubeNodeSelectors } returns checkSelectors
    every { checkConfigs.jobImagePullPolicy } returns pullPolicy
    val discoverConfigs: WorkerConfigs = mockk()
    val specConfigs: WorkerConfigs = mockk()
    val replConfigs: WorkerConfigs = mockk()
    val replSelectors = mapOf("test-selector-repl" to "normal-repl")
    every { replConfigs.workerKubeNodeSelectors } returns replSelectors
    val resourceReqFactory: ResourceRequirementsFactory = mockk()
    val nodeSelector = KubeNodeSelector(ffClient, listOf())

    val mapper =
      PayloadKubeInputMapper(
        labeler,
        podNameGenerator,
        namespace,
        imageRegistry,
        orchestratorContainerInfo,
        replConfigs,
        checkConfigs,
        discoverConfigs,
        specConfigs,
        resourceReqFactory,
        envVarFactory,
        ffClient,
        nodeSelector,
      )
    val input: CheckConnectionInput = mockk()

    mockkStatic("io.airbyte.workers.input.CheckConnectionInputExtensionsKt")
    val jobId = "415"
    val attemptId = 7654L
    val imageName = "image-name"
    val organizationId = UUID.randomUUID()
    val workspaceId1 = UUID.randomUUID()
    val workloadId = UUID.randomUUID().toString()
    val launcherConfig =
      mockk<IntegrationLauncherConfig> {
        every { connectionId } returns UUID.randomUUID()
        every { dockerImage } returns imageName
        every { isCustomConnector } returns customConnector
        every { workspaceId } returns workspaceId1
        every { priority } returns workloadPriority
      }
    val expectedEnv = listOf(EnvVar("key-1", "value-1", null))
    every { envVarFactory.checkConnectorEnvVars(launcherConfig, organizationId, workloadId) } returns expectedEnv
    val jobRunConfig = mockk<JobRunConfig>()
    val checkInputConfig = mockk<JsonNode>()
    val checkConnectionInput = mockk<StandardCheckConnectionInput>()
    every { checkConnectionInput.connectionConfiguration } returns checkInputConfig
    val resourceReqs1 =
      ResourceRequirements()
        .withCpuLimit("11")
        .withMemoryRequest("8Mi")
    val resourceReqs2 =
      ResourceRequirements()
        .withCpuLimit("1")
        .withMemoryRequest("3Mi")
    every { resourceReqFactory.checkConnector(input) } returns resourceReqs1
    every { resourceReqFactory.checkInit(input) } returns resourceReqs2

    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.getActorType() } returns ActorType.SOURCE
    every { input.getOrganizationId() } returns organizationId
    every { input.jobRunConfig } returns jobRunConfig
    every { input.launcherConfig } returns launcherConfig
    every { input.checkConnectionInput } returns checkConnectionInput

    val connectorLabels = mapOf("connector" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getCheckLabels() } returns connectorLabels
    val result = mapper.toKubeInput(workloadId, input, sharedLabels)

    assertEquals(connectorLabels + sharedLabels, result.connectorLabels)
    assertEquals(
      if (customConnector) {
        checkCustomSelectors
      } else if (WorkloadPriority.DEFAULT == workloadPriority) {
        replSelectors
      } else {
        checkSelectors
      },
      result.nodeSelectors,
    )
    assertEquals(namespace, result.kubePodInfo.namespace)
    assertEquals(podName, result.kubePodInfo.name)
    assertEquals(imageName, result.kubePodInfo.mainContainerInfo?.image)
    assertEquals(pullPolicy, result.kubePodInfo.mainContainerInfo?.pullPolicy)
    assertEquals(expectedEnv, result.runtimeEnvVars)
    assertEquals(ResourceConversionUtils.domainToApi(resourceReqs1), result.connectorReqs)
    assertEquals(ResourceConversionUtils.domainToApi(resourceReqs2), result.initReqs)
  }

  @ParameterizedTest
  @MethodSource("connectorInputMatrix")
  fun `builds a kube input from a discover payload`(
    customConnector: Boolean,
    workloadPriority: WorkloadPriority,
  ) {
    val ffClient = TestClient()

    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val imageRegistry = null
    val podName = "check-pod"
    val podNameGenerator: PodNameGenerator = mockk()
    every { podNameGenerator.getDiscoverPodName(any(), any(), any()) } returns podName
    val orchestratorContainerInfo = KubeContainerInfo("img-name", "pull-policy")
    val envVarFactory: RuntimeEnvVarFactory = mockk()
    val checkSelectors = mapOf("test-selector" to "normal-check")
    val pullPolicy = "pull-policy"
    val checkCustomSelectors = mapOf("test-selector" to "custom-check")
    val checkConfigs: WorkerConfigs = mockk()
    val discoverConfigs: WorkerConfigs = mockk()
    every { discoverConfigs.workerKubeAnnotations } returns mapOf("annotation" to "value1")
    every { discoverConfigs.workerIsolatedKubeNodeSelectors } returns checkCustomSelectors
    every { discoverConfigs.workerKubeNodeSelectors } returns checkSelectors
    every { discoverConfigs.jobImagePullPolicy } returns pullPolicy
    val specConfigs: WorkerConfigs = mockk()
    val replConfigs: WorkerConfigs = mockk()
    val replSelectors = mapOf("test-selector-repl" to "normal-repl")
    every { replConfigs.workerKubeNodeSelectors } returns replSelectors
    val resourceReqFactory: ResourceRequirementsFactory = mockk()
    val nodeSelector = KubeNodeSelector(ffClient, listOf())

    val mapper =
      PayloadKubeInputMapper(
        labeler,
        podNameGenerator,
        namespace,
        imageRegistry,
        orchestratorContainerInfo,
        replConfigs,
        checkConfigs,
        discoverConfigs,
        specConfigs,
        resourceReqFactory,
        envVarFactory,
        ffClient,
        nodeSelector,
      )
    val input: DiscoverCatalogInput = mockk()

    mockkStatic("io.airbyte.workers.input.DiscoverCatalogInputExtensionsKt")
    val jobId = "415"
    val attemptId = 7654L
    val imageName = "image-name"
    val organizationId = UUID.randomUUID()
    val workspaceId1 = UUID.randomUUID()
    val workloadId = UUID.randomUUID().toString()
    val launcherConfig =
      mockk<IntegrationLauncherConfig> {
        every { connectionId } returns UUID.randomUUID()
        every { dockerImage } returns imageName
        every { isCustomConnector } returns customConnector
        every { workspaceId } returns workspaceId1
        every { priority } returns workloadPriority
      }
    val expectedEnv = listOf(EnvVar("key-1", "value-1", null))
    every { envVarFactory.discoverConnectorEnvVars(launcherConfig, organizationId, workloadId) } returns expectedEnv
    val jobRunConfig = mockk<JobRunConfig>()
    val catalogInputConfig = mockk<JsonNode>()
    val discoverCatalogInput = mockk<StandardDiscoverCatalogInput>()
    every { discoverCatalogInput.connectionConfiguration } returns catalogInputConfig
    val resourceReqs1 =
      ResourceRequirements()
        .withCpuLimit("4")
        .withCpuRequest("4")
        .withMemoryRequest("800Mi")
        .withMemoryLimit("800Mi")
    val resourceReqs2 =
      ResourceRequirements()
        .withCpuLimit("3")
        .withMemoryRequest("3Mi")
    every { resourceReqFactory.discoverConnector(input) } returns resourceReqs1
    every { resourceReqFactory.discoverInit(input) } returns resourceReqs2

    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.getOrganizationId() } returns organizationId
    every { input.jobRunConfig } returns jobRunConfig
    every { input.launcherConfig } returns launcherConfig
    every { input.discoverCatalogInput } returns discoverCatalogInput

    val connectorLabels = mapOf("connector" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getDiscoverLabels() } returns connectorLabels
    val result = mapper.toKubeInput(workloadId, input, sharedLabels)

    assertEquals(connectorLabels + sharedLabels, result.connectorLabels)
    assertEquals(
      if (customConnector) {
        checkCustomSelectors
      } else if (WorkloadPriority.DEFAULT == workloadPriority) {
        replSelectors
      } else {
        checkSelectors
      },
      result.nodeSelectors,
    )
    assertEquals(namespace, result.kubePodInfo.namespace)
    assertEquals(podName, result.kubePodInfo.name)
    assertEquals(imageName, result.kubePodInfo.mainContainerInfo?.image)
    assertEquals(pullPolicy, result.kubePodInfo.mainContainerInfo?.pullPolicy)
    assertEquals(expectedEnv, result.runtimeEnvVars)
    assertEquals(ResourceConversionUtils.domainToApi(resourceReqs1), result.connectorReqs)
    assertEquals(ResourceConversionUtils.domainToApi(resourceReqs2), result.initReqs)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `builds a kube input from a spec payload`(customConnector: Boolean) {
    val ffClient = TestClient()

    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val imageRegistry = null
    val podName = "check-pod"
    val podNameGenerator: PodNameGenerator = mockk()
    every { podNameGenerator.getSpecPodName(any(), any(), any()) } returns podName
    val orchestratorContainerInfo = KubeContainerInfo("img-name", "pull-policy")
    val envVarFactory: RuntimeEnvVarFactory = mockk()
    val checkSelectors = mapOf("test-selector" to "normal-check")
    val pullPolicy = "pull-policy"
    val checkCustomSelectors = mapOf("test-selector" to "custom-check")
    val checkConfigs: WorkerConfigs = mockk()
    val discoverConfigs: WorkerConfigs = mockk()
    val specConfigs: WorkerConfigs = mockk()
    every { specConfigs.workerKubeAnnotations } returns mapOf("annotation" to "value1")
    every { specConfigs.workerIsolatedKubeNodeSelectors } returns checkCustomSelectors
    every { specConfigs.workerKubeNodeSelectors } returns checkSelectors
    every { specConfigs.jobImagePullPolicy } returns pullPolicy
    val replConfigs: WorkerConfigs = mockk()
    val resourceReqFactory: ResourceRequirementsFactory = mockk()
    val nodeSelector = KubeNodeSelector(ffClient, listOf())

    val mapper =
      PayloadKubeInputMapper(
        labeler,
        podNameGenerator,
        namespace,
        imageRegistry,
        orchestratorContainerInfo,
        replConfigs,
        checkConfigs,
        discoverConfigs,
        specConfigs,
        resourceReqFactory,
        envVarFactory,
        ffClient,
        nodeSelector,
      )

    val jobId = "415"
    val attemptId = 7654L
    val imageName = "image-name"
    val workspaceId1 = UUID.randomUUID()
    val workloadId = UUID.randomUUID().toString()
    val launcherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns imageName
        every { isCustomConnector } returns customConnector
        every { workspaceId } returns workspaceId1
      }
    val expectedEnv = listOf(EnvVar("key-1", "value-1", null))
    every { envVarFactory.specConnectorEnvVars(launcherConfig, workloadId) } returns expectedEnv
    val jobRunConfig = mockk<JobRunConfig>()

    val input: SpecInput = mockk()
    mockkStatic("io.airbyte.workers.input.SpecInputExtensionsKt")
    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.jobRunConfig } returns jobRunConfig
    every { input.launcherConfig } returns launcherConfig

    val resourceReqs1 =
      ResourceRequirements()
        .withCpuLimit("1")
        .withCpuRequest("1")
        .withMemoryRequest("100Mi")
        .withMemoryLimit("800Mi")
    val resourceReqs2 =
      ResourceRequirements()
        .withCpuLimit("2")
        .withMemoryRequest("300Mi")
    every { resourceReqFactory.specConnector() } returns resourceReqs1
    every { resourceReqFactory.specInit() } returns resourceReqs2

    val connectorLabels = mapOf("connector" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getSpecLabels() } returns connectorLabels
    val result = mapper.toKubeInput(workloadId, input, sharedLabels)

    assertEquals(connectorLabels + sharedLabels, result.connectorLabels)
    assertEquals(if (customConnector) checkCustomSelectors else checkSelectors, result.nodeSelectors)
    assertEquals(namespace, result.kubePodInfo.namespace)
    assertEquals(podName, result.kubePodInfo.name)
    assertEquals(imageName, result.kubePodInfo.mainContainerInfo?.image)
    assertEquals(pullPolicy, result.kubePodInfo.mainContainerInfo?.pullPolicy)
    assertEquals(expectedEnv, result.runtimeEnvVars)
    assertEquals(ResourceConversionUtils.domainToApi(resourceReqs1), result.connectorReqs)
    assertEquals(ResourceConversionUtils.domainToApi(resourceReqs2), result.initReqs)
  }

  @Test
  fun `parses custom node selector strings into a map`() {
    val result = "node-pool=my-env-pool ; other = value".toNodeSelectorMap()
    assertEquals(mapOf("node-pool" to "my-env-pool", "other" to "value"), result)
  }

  @Test
  fun `prefixes images with a custom image registry`() {
    val ffClient = TestClient()
    val envVarFactory: RuntimeEnvVarFactory = mockk()
    val podNetworkSecurityLabeler: PodNetworkSecurityLabeler = mockk()
    val labeler = PodLabeler(podNetworkSecurityLabeler)
    val podNameGenerator = PodNameGenerator("test-ns")
    val orchestratorContainerInfo = KubeContainerInfo("orch-img", "Always")
    val reqs = ResourceRequirements()
    val resourceReqFactory = ResourceRequirementsFactory(reqs, reqs, reqs, reqs, reqs)
    val workerConfigs = WorkerConfigs(reqs, emptyList(), emptyMap(), null, emptyMap(), emptyMap(), emptyList(), "Always")
    val workloadId = "workload-1"
    val jobConfig =
      JobRunConfig().apply {
        jobId = "job-1"
        attemptId = 1
      }

    every { envVarFactory.specConnectorEnvVars(any(), any()) } returns emptyList()
    every { envVarFactory.checkConnectorEnvVars(any(), any(), any()) } returns emptyList()
    every { envVarFactory.discoverConnectorEnvVars(any(), any(), any()) } returns emptyList()
    every { envVarFactory.orchestratorEnvVars(any(), any()) } returns emptyList()
    every { envVarFactory.replicationConnectorEnvVars(any(), any(), any()) } returns emptyList()

    every { podNetworkSecurityLabeler.getLabels(any(), any()) } returns emptyMap()

    val testConfig =
      IntegrationLauncherConfig().apply {
        dockerImage = "test-img"
        this.workspaceId = UUID.randomUUID()
        isCustomConnector = false
      }

    val checkConnectionInput =
      StandardCheckConnectionInput().apply {
        connectionConfiguration = mockk<JsonNode>()
        actorContext = ActorContext().withOrganizationId(UUID.randomUUID())
        resourceRequirements = reqs
      }

    val discoverCatalogInput =
      StandardDiscoverCatalogInput().apply {
        actorContext = ActorContext().withOrganizationId(UUID.randomUUID())
      }

    val specInput: SpecInput = mockk()
    mockkStatic("io.airbyte.workers.input.SpecInputExtensionsKt")
    every { specInput.getJobId() } returns "job-1"
    every { specInput.getAttemptId() } returns 1
    every { specInput.jobRunConfig } returns jobConfig
    every { specInput.launcherConfig } returns testConfig

    val checkInput: CheckConnectionInput = mockk()
    mockkStatic("io.airbyte.workers.input.CheckConnectionInputExtensionsKt")
    every { checkInput.jobRunConfig } returns jobConfig
    every { checkInput.launcherConfig } returns testConfig
    every { checkInput.checkConnectionInput } returns checkConnectionInput

    val discoverInput: DiscoverCatalogInput = mockk()
    mockkStatic("io.airbyte.workers.input.DiscoverCatalogInputExtensionsKt")
    every { discoverInput.getJobId() } returns "job-1"
    every { discoverInput.getAttemptId() } returns 1
    every { discoverInput.jobRunConfig } returns jobConfig
    every { discoverInput.launcherConfig } returns testConfig
    every { discoverInput.discoverCatalogInput } returns discoverCatalogInput

    val replInput: ReplicationInput = mockk()
    mockkStatic("io.airbyte.workers.input.ReplicationInputExtensionsKt")
    every { replInput.connectionId } returns UUID.randomUUID()
    every { replInput.jobRunConfig } returns jobConfig
    every { replInput.sourceLauncherConfig } returns testConfig
    every { replInput.destinationLauncherConfig } returns testConfig
    every { replInput.syncResourceRequirements } returns SyncResourceRequirements()
    every { replInput.useFileTransfer } returns false
    val syncPayload = SyncPayload(replInput)
    val nodeSelector = KubeNodeSelector(ffClient, listOf())

    var mapper =
      PayloadKubeInputMapper(
        labeler,
        podNameGenerator,
        "test-ns",
        "custom-image-registry",
        orchestratorContainerInfo,
        workerConfigs,
        workerConfigs,
        workerConfigs,
        workerConfigs,
        resourceReqFactory,
        envVarFactory,
        ffClient,
        nodeSelector,
      )

    mapper.toKubeInput(workloadId, specInput, emptyMap()).also {
      assertEquals("custom-image-registry/test-img", it.kubePodInfo.mainContainerInfo?.image)
    }
    mapper.toKubeInput(workloadId, checkInput, emptyMap()).also {
      assertEquals("custom-image-registry/test-img", it.kubePodInfo.mainContainerInfo?.image)
    }
    mapper.toKubeInput(workloadId, discoverInput, emptyMap()).also {
      assertEquals("custom-image-registry/test-img", it.kubePodInfo.mainContainerInfo?.image)
    }
    mapper.toKubeInput(workloadId, syncPayload, emptyMap()).also {
      assertEquals("custom-image-registry/test-img", it.sourceImage)
      assertEquals("custom-image-registry/test-img", it.destinationImage)
    }

    // Now test a mapper with an image registry with a trailing slash.
    mapper =
      PayloadKubeInputMapper(
        labeler,
        podNameGenerator,
        "test-ns",
        "custom-image-registry/",
        orchestratorContainerInfo,
        workerConfigs,
        workerConfigs,
        workerConfigs,
        workerConfigs,
        resourceReqFactory,
        envVarFactory,
        ffClient,
        nodeSelector,
      )
    mapper.toKubeInput(workloadId, specInput, emptyMap()).also {
      assertEquals("custom-image-registry/test-img", it.kubePodInfo.mainContainerInfo?.image)
    }
    mapper.toKubeInput(workloadId, checkInput, emptyMap()).also {
      assertEquals("custom-image-registry/test-img", it.kubePodInfo.mainContainerInfo?.image)
    }
    mapper.toKubeInput(workloadId, discoverInput, emptyMap()).also {
      assertEquals("custom-image-registry/test-img", it.kubePodInfo.mainContainerInfo?.image)
    }
    mapper.toKubeInput(workloadId, syncPayload, emptyMap()).also {
      assertEquals("custom-image-registry/test-img", it.sourceImage)
      assertEquals("custom-image-registry/test-img", it.destinationImage)
    }

    // Now test that custom connectors which define a fully-qualified image (i.e. image includes registry domain)
    // will not get the custom registry prefix.
    testConfig.dockerImage = "my.registry.com/test-img"

    mapper.toKubeInput(workloadId, specInput, emptyMap()).also {
      assertEquals("my.registry.com/test-img", it.kubePodInfo.mainContainerInfo?.image)
    }
    mapper.toKubeInput(workloadId, checkInput, emptyMap()).also {
      assertEquals("my.registry.com/test-img", it.kubePodInfo.mainContainerInfo?.image)
    }
    mapper.toKubeInput(workloadId, discoverInput, emptyMap()).also {
      assertEquals("my.registry.com/test-img", it.kubePodInfo.mainContainerInfo?.image)
    }
    mapper.toKubeInput(workloadId, syncPayload, emptyMap()).also {
      assertEquals("my.registry.com/test-img", it.sourceImage)
      assertEquals("my.registry.com/test-img", it.destinationImage)
    }
  }

  companion object {
    @JvmStatic
    private fun replicationFlagsInputMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(true),
        Arguments.of(false),
      )

    @JvmStatic
    private fun connectorInputMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(true, WorkloadPriority.HIGH),
        Arguments.of(false, WorkloadPriority.HIGH),
        Arguments.of(true, WorkloadPriority.HIGH),
        Arguments.of(false, WorkloadPriority.HIGH),
        Arguments.of(false, WorkloadPriority.HIGH),
        Arguments.of(false, WorkloadPriority.DEFAULT),
        Arguments.of(false, WorkloadPriority.DEFAULT),
      )
  }
}
