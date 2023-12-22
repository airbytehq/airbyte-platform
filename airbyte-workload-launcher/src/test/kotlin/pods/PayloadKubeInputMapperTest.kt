package io.airbyte.workload.launcher.pods

import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.ResourceRequirements
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.general.DefaultCheckConnectionWorker
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.orchestrator.OrchestratorNameGenerator
import io.airbyte.workers.process.AsyncOrchestratorPodProcess.KUBE_POD_INFO
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workers.sync.ReplicationLauncherWorker
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_SOURCE_LAUNCHER_CONFIG
import io.airbyte.workload.launcher.model.getAttemptId
import io.airbyte.workload.launcher.model.getJobId
import io.airbyte.workload.launcher.model.getOrchestratorResourceReqs
import io.airbyte.workload.launcher.model.usesCustomConnector
import io.airbyte.workload.launcher.serde.ObjectSerializer
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.Optional

class PayloadKubeInputMapperTest {
  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `builds a kube input from a replication input`(customConnector: Boolean) {
    val serializer: ObjectSerializer = mockk()
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val orchestratorNameGenerator = OrchestratorNameGenerator(namespace = namespace)
    val containerInfo = KubeContainerInfo("img-name", "pull-policy")
    val envMap: Map<String, String> = mapOf()
    val replSelectors = mapOf("test-selector" to "normal-repl")
    val replCustomSelectors = mapOf("test-selector" to "custom-repl")
    val checkConfigs: WorkerConfigs = mockk()
    val replConfigs: WorkerConfigs = mockk()
    every { replConfigs.getworkerKubeNodeSelectors() } returns replSelectors
    every { replConfigs.workerIsolatedKubeNodeSelectors } returns Optional.of(replCustomSelectors)
    every { replConfigs.workerKubeAnnotations } returns mapOf("annotation" to "value2")
    val checkResourceReqs = ResourceRequirements().withCpuRequest("1").withMemoryLimit("100MiB")

    val mapper =
      PayloadKubeInputMapper(
        serializer,
        labeler,
        orchestratorNameGenerator,
        namespace,
        containerInfo,
        envMap,
        replConfigs,
        checkConfigs,
        checkResourceReqs,
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

    val mockSerializedOutput = "Serialized Obj."
    every { serializer.serialize<Any>(any()) } returns mockSerializedOutput

    val orchestratorLabels = mapOf("orchestrator" to "labels")
    val sourceLabels = mapOf("source" to "labels")
    val destinationLabels = mapOf("dest" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getReplicationOrchestratorLabels() } returns orchestratorLabels
    every { labeler.getSourceLabels() } returns sourceLabels
    every { labeler.getDestinationLabels() } returns destinationLabels

    val result = mapper.toKubeInput(input, sharedLabels)

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
        ),
    )
    assert(result.resourceReqs == resourceReqs)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `builds a kube input from a check input`(customConnector: Boolean) {
    val serializer: ObjectSerializer = mockk()
    val labeler: PodLabeler = mockk()
    val namespace = "test-namespace"
    val orchestratorNameGenerator = OrchestratorNameGenerator(namespace = namespace)
    val containerInfo = KubeContainerInfo("img-name", "pull-policy")
    val envMap: Map<String, String> = mapOf()
    val checkConfigs: WorkerConfigs = mockk()
    val checkSelectors = mapOf("test-selector" to "normal-check")
    val checkCustomSelectors = mapOf("test-selector" to "custom-check")
    every { checkConfigs.getworkerKubeNodeSelectors() } returns checkSelectors
    every { checkConfigs.workerIsolatedKubeNodeSelectors } returns Optional.of(checkCustomSelectors)
    every { checkConfigs.workerKubeAnnotations } returns mapOf("annotation" to "value1")

    val replConfigs: WorkerConfigs = mockk()
    val checkResourceReqs = ResourceRequirements().withCpuRequest("1").withMemoryLimit("100MiB")

    val mapper =
      PayloadKubeInputMapper(
        serializer,
        labeler,
        orchestratorNameGenerator,
        namespace,
        containerInfo,
        envMap,
        replConfigs,
        checkConfigs,
        checkResourceReqs,
      )
    val input: CheckConnectionInput = mockk()

    mockkStatic("io.airbyte.workload.launcher.model.CheckConnectionInputExtensionsKt")
    val jobId = "415"
    val attemptId = 7654L

    every { input.getJobId() } returns jobId
    every { input.getAttemptId() } returns attemptId
    every { input.usesCustomConnector() } returns customConnector
    every { input.jobRunConfig } returns mockk<JobRunConfig>()
    every { input.launcherConfig } returns mockk<IntegrationLauncherConfig>()

    val mockSerializedOutput = "Serialized Obj."
    every { serializer.serialize<Any>(any()) } returns mockSerializedOutput

    val orchestratorLabels = mapOf("orchestrator" to "labels")
    val connectorLabels = mapOf("connector" to "labels")
    val sharedLabels = mapOf("pass through" to "labels")
    every { labeler.getCheckOrchestratorLabels() } returns orchestratorLabels
    every { labeler.getCheckConnectorLabels() } returns connectorLabels

    val result = mapper.toKubeInput(input, sharedLabels)

    assert(result.orchestratorLabels == orchestratorLabels + sharedLabels)
    assert(result.connectorLabels == connectorLabels + sharedLabels)
    assert(result.nodeSelectors == if (customConnector) checkCustomSelectors else checkSelectors)
    assert(result.kubePodInfo == KubePodInfo(namespace, "orchestrator-check-job-415-attempt-7654", containerInfo))
    assert(
      result.fileMap ==
        mapOf(
          OrchestratorConstants.INIT_FILE_ENV_MAP to mockSerializedOutput,
          OrchestratorConstants.INIT_FILE_APPLICATION to DefaultCheckConnectionWorker.CHECK,
          OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to mockSerializedOutput,
          OrchestratorConstants.INIT_FILE_INPUT to mockSerializedOutput,
          KUBE_POD_INFO to mockSerializedOutput,
        ),
    )
    assert(result.resourceReqs == checkResourceReqs)
  }
}
