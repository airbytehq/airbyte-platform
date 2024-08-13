package io.airbyte.workload.launcher.pods

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.ActorType
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.ConnectorSidecarFetchesInputFromInit
import io.airbyte.featureflag.ContainerOrchestratorDevImage
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.OrchestratorFetchesInputFromInit
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.getAttemptId
import io.airbyte.workers.input.getJobId
import io.airbyte.workers.input.getOrchestratorResourceReqs
import io.airbyte.workers.input.usesCustomConnector
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.pod.PodLabeler
import io.airbyte.workers.pod.PodNameGenerator
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.Metadata.AWS_ASSUME_ROLE_EXTERNAL_ID
import io.airbyte.workers.serde.ObjectSerializer
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workload.launcher.model.getActorType
import io.airbyte.workload.launcher.model.getAttemptId
import io.airbyte.workload.launcher.model.getJobId
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
import io.airbyte.commons.envvar.EnvVar as AirbyteEnvVar

class PayloadKubeInputMapperTest {
  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `builds a kube input from a replication payload`(customConnector: Boolean) {
    // I'm overloading this parameter to exercise the FF. The FF check will be removed shortly
    val shouldKubeCpInput = customConnector

    val serializer: ObjectSerializer = mockk()
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val podNameGenerator = PodNameGenerator(namespace = namespace)
    val containerInfo = KubeContainerInfo("img-name", "pull-policy")
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
    val ffClient: TestClient = mockk()
    every { ffClient.stringVariation(ContainerOrchestratorDevImage, any()) } returns ""
    every { ffClient.boolVariation(OrchestratorFetchesInputFromInit, any()) } returns !shouldKubeCpInput

    val mapper =
      PayloadKubeInputMapper(
        serializer,
        labeler,
        podNameGenerator,
        namespace,
        containerInfo,
        awsAssumedRoleEnv,
        replConfigs,
        checkConfigs,
        discoverConfigs,
        specConfigs,
        ffClient,
      )
    val input: ReplicationInput = mockk()

    mockkStatic("io.airbyte.workers.input.ReplicationInputExtensionsKt")
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
    every { input.workspaceId } returns mockk<UUID>()

    val mockSerializedOutput = "Serialized Obj."
    every { serializer.serialize<Any>(any()) } returns mockSerializedOutput

    val orchestratorLabels = mapOf("orchestrator" to "labels")
    val sourceLabels = mapOf("source" to "labels")
    val destinationLabels = mapOf("dest" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getReplicationOrchestratorLabels(containerInfo.image) } returns orchestratorLabels
    every { labeler.getSourceLabels() } returns sourceLabels
    every { labeler.getDestinationLabels() } returns destinationLabels
    val workloadId = UUID.randomUUID().toString()
    val result = mapper.toKubeInput(workloadId, input, sharedLabels)

    assert(result.orchestratorLabels == orchestratorLabels + sharedLabels)
    assert(result.sourceLabels == sourceLabels + sharedLabels)
    assert(result.destinationLabels == destinationLabels + sharedLabels)
    assert(result.nodeSelectors == if (customConnector) replCustomSelectors else replSelectors)
    assert(result.kubePodInfo == KubePodInfo(namespace, "orchestrator-repl-job-415-attempt-7654", containerInfo))
    val expectedFileMap: Map<String, String> =
      buildMap {
        if (shouldKubeCpInput) {
          put(OrchestratorConstants.INIT_FILE_INPUT, mockSerializedOutput)
        }
      }

    assert(result.fileMap == expectedFileMap)
    assert(result.resourceReqs == resourceReqs)
    assert(
      result.extraEnv ==
        listOf(
          EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.SYNC.toString(), null),
          EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null),
          EnvVar(AirbyteEnvVar.JOB_ID.toString(), jobId, null),
          EnvVar(AirbyteEnvVar.ATTEMPT_ID.toString(), attemptId.toString(), null),
        ),
    )
  }

  @ParameterizedTest
  @MethodSource("connectorInputMatrix")
  fun `builds a kube input from a check payload`(
    customConnector: Boolean,
    assumedRoleEnabled: Boolean,
    workloadPriority: WorkloadPriority,
    useFetchingInit: Boolean,
  ) {
    val ffClient =
      TestClient(
        mapOf(
          InjectAwsSecretsToConnectorPods.key to assumedRoleEnabled,
          ConnectorSidecarFetchesInputFromInit.key to useFetchingInit,
        ),
      )

    val serializer: ObjectSerializer = mockk()
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val podName = "check-pod"
    val podNameGenerator: PodNameGenerator = mockk()
    every { podNameGenerator.getCheckPodName(any(), any(), any()) } returns podName
    val orchestratorContainerInfo = KubeContainerInfo("img-name", "pull-policy")
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
    val workloadId = UUID.randomUUID().toString()
    val logPath = "/log/path"
    val launcherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns imageName
        every { isCustomConnector } returns customConnector
        every { workspaceId } returns workspaceId1
        every { priority } returns workloadPriority
      }
    val jobRunConfig = mockk<JobRunConfig>()
    val checkInputConfig = mockk<JsonNode>()
    val checkConnectionInput = mockk<StandardCheckConnectionInput>()
    every { checkConnectionInput.connectionConfiguration } returns checkInputConfig

    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.getActorType() } returns ActorType.SOURCE
    every { input.jobRunConfig } returns jobRunConfig
    every { input.launcherConfig } returns launcherConfig
    every { input.checkConnectionInput } returns checkConnectionInput

    val mockSerializedOutput = "Serialized Obj."
    every {
      serializer.serialize<Any>(SidecarInput(checkConnectionInput, null, workloadId, launcherConfig, SidecarInput.OperationType.CHECK, logPath))
    } returns mockSerializedOutput
    every { serializer.serialize<Any>(jobRunConfig) } returns mockSerializedOutput
    every { serializer.serialize<Any>(checkInputConfig) } returns mockSerializedOutput

    val connectorLabels = mapOf("connector" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getCheckLabels() } returns connectorLabels
    val result = mapper.toKubeInput(workloadId, input, sharedLabels, logPath)

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
    val expectedFileMap =
      buildMap {
        if (!useFetchingInit) {
          put(OrchestratorConstants.CONNECTION_CONFIGURATION, mockSerializedOutput)
          put(OrchestratorConstants.SIDECAR_INPUT, mockSerializedOutput)
        }
      }
    Assertions.assertEquals(expectedFileMap, result.fileMap)

    val workloadTypeEnvVar = EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.CHECK.toString(), null)
    val workloadIdEnvVar = EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null)
    assert(result.extraEnv.contains(workloadTypeEnvVar))
    assert(result.extraEnv.contains(workloadIdEnvVar))
    if (!customConnector && assumedRoleEnabled) {
      val externalIdVar = EnvVar(AWS_ASSUME_ROLE_EXTERNAL_ID, workspaceId1.toString(), null)
      assert(result.extraEnv.contains(externalIdVar))
      awsAssumedRoleEnv.forEach { assert(result.extraEnv.contains(it)) }
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
    useFetchingInit: Boolean,
  ) {
    val ffClient =
      TestClient(
        mapOf(
          InjectAwsSecretsToConnectorPods.key to assumedRoleEnabled,
          ConnectorSidecarFetchesInputFromInit.key to useFetchingInit,
        ),
      )

    val serializer: ObjectSerializer = mockk()
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val podName = "check-pod"
    val podNameGenerator: PodNameGenerator = mockk()
    every { podNameGenerator.getDiscoverPodName(any(), any(), any()) } returns podName
    val orchestratorContainerInfo = KubeContainerInfo("img-name", "pull-policy")
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
    val workloadId = UUID.randomUUID().toString()
    val logPath = "/log/path"
    val launcherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns imageName
        every { isCustomConnector } returns customConnector
        every { workspaceId } returns workspaceId1
        every { priority } returns workloadPriority
      }
    val jobRunConfig = mockk<JobRunConfig>()
    val catalogInputConfig = mockk<JsonNode>()
    val discoverCatalogInput = mockk<StandardDiscoverCatalogInput>()
    every { discoverCatalogInput.connectionConfiguration } returns catalogInputConfig

    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.jobRunConfig } returns jobRunConfig
    every { input.launcherConfig } returns launcherConfig
    every { input.discoverCatalogInput } returns discoverCatalogInput

    val mockSerializedOutput = "Serialized Obj."
    every {
      serializer.serialize<Any>(
        SidecarInput(
          null,
          discoverCatalogInput,
          workloadId,
          launcherConfig,
          SidecarInput.OperationType.DISCOVER,
          logPath,
        ),
      )
    } returns mockSerializedOutput
    every { serializer.serialize<Any>(jobRunConfig) } returns mockSerializedOutput
    every { serializer.serialize<Any>(catalogInputConfig) } returns mockSerializedOutput

    val connectorLabels = mapOf("connector" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getDiscoverLabels() } returns connectorLabels
    val result = mapper.toKubeInput(workloadId, input, sharedLabels, logPath)

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
    val expectedFileMap =
      buildMap {
        if (!useFetchingInit) {
          put(OrchestratorConstants.CONNECTION_CONFIGURATION, mockSerializedOutput)
          put(OrchestratorConstants.SIDECAR_INPUT, mockSerializedOutput)
        }
      }
    Assertions.assertEquals(expectedFileMap, result.fileMap)

    val workloadTypeEnvVar = EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.DISCOVER.toString(), null)
    val workloadIdEnvVar = EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null)
    assert(result.extraEnv.contains(workloadTypeEnvVar))
    assert(result.extraEnv.contains(workloadIdEnvVar))

    if (!customConnector && assumedRoleEnabled) {
      val externalIdVar = EnvVar(AWS_ASSUME_ROLE_EXTERNAL_ID, workspaceId1.toString(), null)
      assert(result.extraEnv.contains(externalIdVar))
      awsAssumedRoleEnv.forEach { assert(result.extraEnv.contains(it)) }
    } else {
      awsAssumedRoleEnv.forEach { assert(!result.extraEnv.contains(it)) }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `builds a kube input from a spec payload`(customConnector: Boolean) {
    // I'm overloading this parameter to exercise the FF. The FF check will be removed shortly
    val useFetchingInit = customConnector

    val ffClient =
      TestClient(
        mapOf(
          ConnectorSidecarFetchesInputFromInit.key to useFetchingInit,
        ),
      )

    val serializer: ObjectSerializer = mockk()
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val podName = "check-pod"
    val podNameGenerator: PodNameGenerator = mockk()
    every { podNameGenerator.getSpecPodName(any(), any(), any()) } returns podName
    val orchestratorContainerInfo = KubeContainerInfo("img-name", "pull-policy")
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
    val workloadId = UUID.randomUUID().toString()
    val logPath = "/log/path"
    val launcherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns imageName
        every { isCustomConnector } returns customConnector
        every { workspaceId } returns workspaceId1
      }
    val jobRunConfig = mockk<JobRunConfig>()

    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.jobRunConfig } returns jobRunConfig
    every { input.launcherConfig } returns launcherConfig

    val mockSerializedOutput = "Serialized Obj."
    every {
      serializer.serialize<Any>(
        SidecarInput(
          null,
          null,
          workloadId,
          launcherConfig,
          SidecarInput.OperationType.SPEC,
          logPath,
        ),
      )
    } returns mockSerializedOutput
    every { serializer.serialize<Any>(jobRunConfig) } returns mockSerializedOutput

    val connectorLabels = mapOf("connector" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getSpecLabels() } returns connectorLabels
    val result = mapper.toKubeInput(workloadId, input, sharedLabels, logPath)

    Assertions.assertEquals(connectorLabels + sharedLabels, result.connectorLabels)
    Assertions.assertEquals(if (customConnector) checkCustomSelectors else checkSelectors, result.nodeSelectors)
    Assertions.assertEquals(namespace, result.kubePodInfo.namespace)
    Assertions.assertEquals(podName, result.kubePodInfo.name)
    Assertions.assertEquals(imageName, result.kubePodInfo.mainContainerInfo.image)
    Assertions.assertEquals(pullPolicy, result.kubePodInfo.mainContainerInfo.pullPolicy)
    val expectedFileMap =
      buildMap {
        if (!useFetchingInit) {
          put(OrchestratorConstants.SIDECAR_INPUT, mockSerializedOutput)
        }
      }
    Assertions.assertEquals(expectedFileMap, result.fileMap)
  }

  companion object {
    @JvmStatic
    private fun connectorInputMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(true, true, WorkloadPriority.HIGH, true),
        Arguments.of(false, true, WorkloadPriority.HIGH, true),
        Arguments.of(true, false, WorkloadPriority.HIGH, true),
        Arguments.of(false, false, WorkloadPriority.HIGH, true),
        Arguments.of(false, false, WorkloadPriority.HIGH, false),
        Arguments.of(false, false, WorkloadPriority.DEFAULT, false),
        Arguments.of(false, false, WorkloadPriority.DEFAULT, true),
      )
    }
  }
}
