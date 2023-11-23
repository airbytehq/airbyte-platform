package io.airbyte.workload.launcher.pods

import io.airbyte.config.ResourceRequirements
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
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
    val selectors = mapOf("test-selector" to "normal")
    val customSelectors = mapOf("test-selector" to "custom")

    val mapper =
      PayloadKubeInputMapper(
        serializer,
        labeler,
        orchestratorNameGenerator,
        namespace,
        containerInfo,
        envMap,
        selectors,
        customSelectors,
      )
    val input: ReplicationInput = mockk()
    val workloadId = "31415"

    mockkStatic("io.airbyte.workload.launcher.model.ReplicationInputExtensionsKt")
    val jobId = "415"
    val attemptId = 7654L
    val resourceReqs = ResourceRequirements().withCpuRequest("test-cpu").withMemoryRequest("test-mem")

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
    val passThroughLabels = mapOf("pass through" to "labels")
    every { labeler.getOrchestratorLabels(input, workloadId, passThroughLabels) } returns orchestratorLabels
    every { labeler.getSourceLabels(input, workloadId, passThroughLabels) } returns sourceLabels
    every { labeler.getDestinationLabels(input, workloadId, passThroughLabels) } returns destinationLabels

    val result = mapper.toKubeInput(input, workloadId, passThroughLabels)

    assert(result.orchestratorLabels == orchestratorLabels)
    assert(result.sourceLabels == sourceLabels)
    assert(result.destinationLabels == destinationLabels)
    assert(result.nodeSelectors == if (customConnector) customSelectors else selectors)
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
}
