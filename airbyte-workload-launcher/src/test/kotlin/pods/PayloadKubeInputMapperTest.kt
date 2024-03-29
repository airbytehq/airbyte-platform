package io.airbyte.workload.launcher.pods

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.ActorType
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.orchestrator.PodNameGenerator
import io.airbyte.workers.process.AsyncOrchestratorPodProcess.KUBE_POD_INFO
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.Metadata.AWS_ASSUME_ROLE_EXTERNAL_ID
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workers.sync.ReplicationLauncherWorker
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_SOURCE_LAUNCHER_CONFIG
import io.airbyte.workload.launcher.config.OrchestratorEnvSingleton
import io.airbyte.workload.launcher.model.getActorType
import io.airbyte.workload.launcher.model.getAttemptId
import io.airbyte.workload.launcher.model.getJobId
import io.airbyte.workload.launcher.model.getOrchestratorResourceReqs
import io.airbyte.workload.launcher.model.usesCustomConnector
import io.airbyte.workload.launcher.serde.ObjectSerializer
import io.fabric8.kubernetes.api.model.EnvVar
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream

class PayloadKubeInputMapperTest {
  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `builds a kube input from a replication payload`(customConnector: Boolean) {
    val serializer: ObjectSerializer = mockk()
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val podNameGenerator = PodNameGenerator(namespace = namespace)
    val containerInfo = KubeContainerInfo("img-name", "pull-policy")
    val orchestratorEnvSingleton: OrchestratorEnvSingleton = mockk()
    every { orchestratorEnvSingleton.orchestratorEnvVars(any()) } returns emptyList()
    every { orchestratorEnvSingleton.orchestratorEnvMap(any()) } returns emptyMap()
    val awsAssumedRoleEnv: List<EnvVar> = listOf()
    val replSelectors = mapOf("test-selector" to "normal-repl")
    val replCustomSelectors = mapOf("test-selector" to "custom-repl")
    val checkConfigs: WorkerConfigs = mockk()
    val discoverConfigs: WorkerConfigs = mockk()
    val specConfigs: WorkerConfigs = mockk()
    val replConfigs: WorkerConfigs = mockk()
    every { replConfigs.getworkerKubeNodeSelectors() } returns replSelectors
    every { replConfigs.workerIsolatedKubeNodeSelectors } returns Optional.of(replCustomSelectors)
    every { replConfigs.workerKubeAnnotations } returns mapOf("annotation" to "value2")

    val mapper =
      PayloadKubeInputMapper(
        serializer,
        labeler,
        podNameGenerator,
        orchestratorEnvSingleton,
        namespace,
        containerInfo,
        awsAssumedRoleEnv,
        replConfigs,
        checkConfigs,
        discoverConfigs,
        specConfigs,
        TestClient(emptyMap()),
      )
    val input: ReplicationInput = mockk()

    mockkStatic("io.airbyte.workload.launcher.model.ReplicationInputExtensionsKt")
    val jobId = "415"
    val attemptId = 7654L
    val resourceReqs = ResourceRequirements()

    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.getOrchestratorResourceReqs() } returns resourceReqs
    every { input.usesCustomConnector() } returns customConnector
    every { input.jobRunConfig } returns mockk<JobRunConfig>()
    every { input.sourceLauncherConfig } returns mockk<IntegrationLauncherConfig>()
    every { input.destinationLauncherConfig } returns mockk<IntegrationLauncherConfig>()
    every { input.connectionId } returns mockk<UUID>()

    val mockSerializedOutput = "Serialized Obj."
    every { serializer.serialize<Any>(any()) } returns mockSerializedOutput

    val orchestratorLabels = mapOf("orchestrator" to "labels")
    val sourceLabels = mapOf("source" to "labels")
    val destinationLabels = mapOf("dest" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getReplicationOrchestratorLabels() } returns orchestratorLabels
    every { labeler.getSourceLabels() } returns sourceLabels
    every { labeler.getDestinationLabels() } returns destinationLabels
    val workloadId = UUID.randomUUID().toString()
    val result = mapper.toKubeInput(workloadId, input, sharedLabels)

    assert(result.orchestratorLabels == orchestratorLabels + sharedLabels)
    assert(result.sourceLabels == sourceLabels + sharedLabels)
    assert(result.destinationLabels == destinationLabels + sharedLabels)
    assert(result.nodeSelectors == if (customConnector) replCustomSelectors else replSelectors)
    assert(result.kubePodInfo == KubePodInfo(namespace, "orchestrator-repl-job-415-attempt-7654", containerInfo))
    assert(
      result.fileMap ==
        mapOf(
          OrchestratorConstants.INIT_FILE_ENV_MAP to mockSerializedOutput,
          OrchestratorConstants.INIT_FILE_APPLICATION to ReplicationLauncherWorker.REPLICATION,
          OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to mockSerializedOutput,
          OrchestratorConstants.INIT_FILE_INPUT to mockSerializedOutput,
          INIT_FILE_SOURCE_LAUNCHER_CONFIG to mockSerializedOutput,
          INIT_FILE_DESTINATION_LAUNCHER_CONFIG to mockSerializedOutput,
          KUBE_POD_INFO to mockSerializedOutput,
          OrchestratorConstants.WORKLOAD_ID_FILE to workloadId,
        ),
    )
    assert(result.resourceReqs == resourceReqs)
  }

  @ParameterizedTest
  @MethodSource("connectorInputMatrix")
  fun `builds a kube input from a check payload`(
    customConnector: Boolean,
    assumedRoleEnabled: Boolean,
    workloadPriority: WorkloadPriority,
  ) {
    val ffClient =
      TestClient(
        mapOf(
          InjectAwsSecretsToConnectorPods.key to assumedRoleEnabled,
        ),
      )

    val serializer: ObjectSerializer = mockk()
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val podName = "check-pod"
    val podNameGenerator: PodNameGenerator = mockk()
    every { podNameGenerator.getCheckPodName(any(), any(), any()) } returns podName
    val orchestratorContainerInfo = KubeContainerInfo("img-name", "pull-policy")
    val orchestratorEnvSingleton: OrchestratorEnvSingleton = mockk()
    every { orchestratorEnvSingleton.orchestratorEnvVars(any()) } returns emptyList()
    every { orchestratorEnvSingleton.orchestratorEnvMap(any()) } returns emptyMap()
    val awsAssumedRoleEnv: List<EnvVar> = listOf(EnvVar("aws-assumed-role", "value", null))
    val checkSelectors = mapOf("test-selector" to "normal-check")
    val pullPolicy = "pull-policy"
    val checkCustomSelectors = mapOf("test-selector" to "custom-check")
    val checkConfigs: WorkerConfigs = mockk()
    every { checkConfigs.workerKubeAnnotations } returns mapOf("annotation" to "value1")
    every { checkConfigs.workerIsolatedKubeNodeSelectors } returns Optional.of(checkCustomSelectors)
    every { checkConfigs.getworkerKubeNodeSelectors() } returns checkSelectors
    every { checkConfigs.jobImagePullPolicy } returns pullPolicy
    val discoverConfigs: WorkerConfigs = mockk()
    val specConfigs: WorkerConfigs = mockk()
    val replConfigs: WorkerConfigs = mockk()
    val replSelectors = mapOf("test-selector-repl" to "normal-repl")
    every { replConfigs.getworkerKubeNodeSelectors() } returns replSelectors

    val mapper =
      PayloadKubeInputMapper(
        serializer,
        labeler,
        podNameGenerator,
        orchestratorEnvSingleton,
        namespace,
        orchestratorContainerInfo,
        awsAssumedRoleEnv,
        replConfigs,
        checkConfigs,
        discoverConfigs,
        specConfigs,
        ffClient,
      )
    val input: CheckConnectionInput = mockk()

    mockkStatic("io.airbyte.workload.launcher.model.CheckConnectionInputExtensionsKt")
    val jobId = "415"
    val attemptId = 7654L
    val imageName = "image-name"
    val workspaceId1 = UUID.randomUUID()

    val checkConnectionInput = mockk<StandardCheckConnectionInput>()
    every { checkConnectionInput.connectionConfiguration } returns mockk<JsonNode>()

    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.getActorType() } returns ActorType.SOURCE
    every { input.jobRunConfig } returns mockk<JobRunConfig>()
    every { input.launcherConfig } returns
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns imageName
        every { isCustomConnector } returns customConnector
        every { workspaceId } returns workspaceId1
        every { priority } returns workloadPriority
      }
    every { input.checkConnectionInput } returns checkConnectionInput

    val mockSerializedOutput = "Serialized Obj."
    every { serializer.serialize<Any>(any()) } returns mockSerializedOutput

    val connectorLabels = mapOf("connector" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getCheckConnectorLabels() } returns connectorLabels
    val workloadId = UUID.randomUUID().toString()
    val result = mapper.toKubeInput(workloadId, input, sharedLabels)

    Assertions.assertEquals(connectorLabels + sharedLabels, result.connectorLabels)
    Assertions.assertEquals(
      if (customConnector) {
        checkCustomSelectors
      } else if (WorkloadPriority.DEFAULT.equals(workloadPriority)) {
        replSelectors
      } else {
        checkSelectors
      },
      result.nodeSelectors,
    )
    Assertions.assertEquals(namespace, result.kubePodInfo.namespace)
    Assertions.assertEquals(podName, result.kubePodInfo.name)
    Assertions.assertEquals(imageName, result.kubePodInfo.mainContainerInfo.image)
    Assertions.assertEquals(pullPolicy, result.kubePodInfo.mainContainerInfo.pullPolicy)
    Assertions.assertEquals(
      mapOf(
        OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to mockSerializedOutput,
        OrchestratorConstants.CONNECTION_CONFIGURATION to mockSerializedOutput,
        OrchestratorConstants.SIDECAR_INPUT to mockSerializedOutput,
      ),
      result.fileMap,
    )

    if (!customConnector && assumedRoleEnabled) {
      val externalIdVar = EnvVar(AWS_ASSUME_ROLE_EXTERNAL_ID, workspaceId1.toString(), null)
      val expectedEnvList = awsAssumedRoleEnv + externalIdVar
      Assertions.assertEquals(expectedEnvList, result.extraEnv)
    } else {
      awsAssumedRoleEnv.forEach { assert(!result.extraEnv.contains(it)) }
    }
  }

  @ParameterizedTest
  @MethodSource("connectorInputMatrix")
  fun `builds a kube input from a discover payload`(
    customConnector: Boolean,
    assumedRoleEnabled: Boolean,
    workloadPriority: WorkloadPriority,
  ) {
    val ffClient =
      TestClient(
        mapOf(
          InjectAwsSecretsToConnectorPods.key to assumedRoleEnabled,
        ),
      )

    val serializer: ObjectSerializer = mockk()
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val podName = "check-pod"
    val podNameGenerator: PodNameGenerator = mockk()
    every { podNameGenerator.getDiscoverPodName(any(), any(), any()) } returns podName
    val orchestratorContainerInfo = KubeContainerInfo("img-name", "pull-policy")
    val orchestratorEnvSingleton: OrchestratorEnvSingleton = mockk()
    every { orchestratorEnvSingleton.orchestratorEnvVars(any()) } returns emptyList()
    every { orchestratorEnvSingleton.orchestratorEnvMap(any()) } returns emptyMap()
    val awsAssumedRoleEnv: List<EnvVar> = listOf(EnvVar("aws-assumed-role", "value", null))
    val checkSelectors = mapOf("test-selector" to "normal-check")
    val pullPolicy = "pull-policy"
    val checkCustomSelectors = mapOf("test-selector" to "custom-check")
    val checkConfigs: WorkerConfigs = mockk()
    val discoverConfigs: WorkerConfigs = mockk()
    every { discoverConfigs.workerKubeAnnotations } returns mapOf("annotation" to "value1")
    every { discoverConfigs.workerIsolatedKubeNodeSelectors } returns Optional.of(checkCustomSelectors)
    every { discoverConfigs.getworkerKubeNodeSelectors() } returns checkSelectors
    every { discoverConfigs.jobImagePullPolicy } returns pullPolicy
    val specConfigs: WorkerConfigs = mockk()
    val replConfigs: WorkerConfigs = mockk()
    val replSelectors = mapOf("test-selector-repl" to "normal-repl")
    every { replConfigs.getworkerKubeNodeSelectors() } returns replSelectors

    val mapper =
      PayloadKubeInputMapper(
        serializer,
        labeler,
        podNameGenerator,
        orchestratorEnvSingleton,
        namespace,
        orchestratorContainerInfo,
        awsAssumedRoleEnv,
        replConfigs,
        checkConfigs,
        discoverConfigs,
        specConfigs,
        ffClient,
      )
    val input: DiscoverCatalogInput = mockk()

    mockkStatic("io.airbyte.workload.launcher.model.DiscoverCatalogInputExtensionsKt")
    val jobId = "415"
    val attemptId = 7654L
    val imageName = "image-name"
    val workspaceId1 = UUID.randomUUID()

    val discoverCatalogInput = mockk<StandardDiscoverCatalogInput>()
    every { discoverCatalogInput.connectionConfiguration } returns mockk<JsonNode>()

    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.jobRunConfig } returns mockk<JobRunConfig>()
    every { input.launcherConfig } returns
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns imageName
        every { isCustomConnector } returns customConnector
        every { workspaceId } returns workspaceId1
        every { priority } returns workloadPriority
      }
    every { input.discoverCatalogInput } returns discoverCatalogInput

    val mockSerializedOutput = "Serialized Obj."
    every { serializer.serialize<Any>(any()) } returns mockSerializedOutput

    val connectorLabels = mapOf("connector" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getCheckConnectorLabels() } returns connectorLabels
    val workloadId = UUID.randomUUID().toString()
    val result = mapper.toKubeInput(workloadId, input, sharedLabels)

    Assertions.assertEquals(connectorLabels + sharedLabels, result.connectorLabels)
    Assertions.assertEquals(
      if (customConnector) {
        checkCustomSelectors
      } else if (WorkloadPriority.DEFAULT.equals(workloadPriority)) {
        replSelectors
      } else {
        checkSelectors
      },
      result.nodeSelectors,
    )
    Assertions.assertEquals(namespace, result.kubePodInfo.namespace)
    Assertions.assertEquals(podName, result.kubePodInfo.name)
    Assertions.assertEquals(imageName, result.kubePodInfo.mainContainerInfo.image)
    Assertions.assertEquals(pullPolicy, result.kubePodInfo.mainContainerInfo.pullPolicy)
    Assertions.assertEquals(
      mapOf(
        OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to mockSerializedOutput,
        OrchestratorConstants.CONNECTION_CONFIGURATION to mockSerializedOutput,
        OrchestratorConstants.SIDECAR_INPUT to mockSerializedOutput,
      ),
      result.fileMap,
    )

    if (!customConnector && assumedRoleEnabled) {
      val externalIdVar = EnvVar(AWS_ASSUME_ROLE_EXTERNAL_ID, workspaceId1.toString(), null)
      val expectedEnvList = awsAssumedRoleEnv + externalIdVar
      Assertions.assertEquals(expectedEnvList, result.extraEnv)
    } else {
      awsAssumedRoleEnv.forEach { assert(!result.extraEnv.contains(it)) }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `builds a kube input from a spec payload`(customConnector: Boolean) {
    val ffClient =
      TestClient()

    val serializer: ObjectSerializer = mockk()
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val podName = "check-pod"
    val podNameGenerator: PodNameGenerator = mockk()
    every { podNameGenerator.getSpecPodName(any(), any(), any()) } returns podName
    val orchestratorContainerInfo = KubeContainerInfo("img-name", "pull-policy")
    val orchestratorEnvSingleton: OrchestratorEnvSingleton = mockk()
    every { orchestratorEnvSingleton.orchestratorEnvVars(any()) } returns emptyList()
    every { orchestratorEnvSingleton.orchestratorEnvMap(any()) } returns emptyMap()
    val awsAssumedRoleEnv: List<EnvVar> = listOf(EnvVar("aws-assumed-role", "value", null))
    val checkSelectors = mapOf("test-selector" to "normal-check")
    val pullPolicy = "pull-policy"
    val checkCustomSelectors = mapOf("test-selector" to "custom-check")
    val checkConfigs: WorkerConfigs = mockk()
    val discoverConfigs: WorkerConfigs = mockk()
    val specConfigs: WorkerConfigs = mockk()
    every { specConfigs.workerKubeAnnotations } returns mapOf("annotation" to "value1")
    every { specConfigs.workerIsolatedKubeNodeSelectors } returns Optional.of(checkCustomSelectors)
    every { specConfigs.getworkerKubeNodeSelectors() } returns checkSelectors
    every { specConfigs.jobImagePullPolicy } returns pullPolicy
    val replConfigs: WorkerConfigs = mockk()

    val mapper =
      PayloadKubeInputMapper(
        serializer,
        labeler,
        podNameGenerator,
        orchestratorEnvSingleton,
        namespace,
        orchestratorContainerInfo,
        awsAssumedRoleEnv,
        replConfigs,
        checkConfigs,
        discoverConfigs,
        specConfigs,
        ffClient,
      )
    val input: SpecInput = mockk()

    mockkStatic("io.airbyte.workload.launcher.model.SpecInputExtensionsKt")
    val jobId = "415"
    val attemptId = 7654L
    val imageName = "image-name"
    val workspaceId1 = UUID.randomUUID()

    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.jobRunConfig } returns mockk<JobRunConfig>()
    every { input.launcherConfig } returns
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns imageName
        every { isCustomConnector } returns customConnector
        every { workspaceId } returns workspaceId1
      }

    val mockSerializedOutput = "Serialized Obj."
    every { serializer.serialize<Any>(any()) } returns mockSerializedOutput

    val connectorLabels = mapOf("connector" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getCheckConnectorLabels() } returns connectorLabels
    val workloadId = UUID.randomUUID().toString()
    val result = mapper.toKubeInput(workloadId, input, sharedLabels)

    Assertions.assertEquals(connectorLabels + sharedLabels, result.connectorLabels)
    Assertions.assertEquals(if (customConnector) checkCustomSelectors else checkSelectors, result.nodeSelectors)
    Assertions.assertEquals(namespace, result.kubePodInfo.namespace)
    Assertions.assertEquals(podName, result.kubePodInfo.name)
    Assertions.assertEquals(imageName, result.kubePodInfo.mainContainerInfo.image)
    Assertions.assertEquals(pullPolicy, result.kubePodInfo.mainContainerInfo.pullPolicy)
    Assertions.assertEquals(
      mapOf(
        OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to mockSerializedOutput,
        OrchestratorConstants.SIDECAR_INPUT to mockSerializedOutput,
      ),
      result.fileMap,
    )
  }

  companion object {
    @JvmStatic
    private fun connectorInputMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(true, true, WorkloadPriority.HIGH),
        Arguments.of(false, true, WorkloadPriority.HIGH),
        Arguments.of(true, false, WorkloadPriority.HIGH),
        Arguments.of(false, false, WorkloadPriority.HIGH),
        Arguments.of(false, false, WorkloadPriority.DEFAULT),
      )
    }
  }
}
