package io.airbyte.workload.launcher.pods

import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.orchestrator.OrchestratorNameGenerator
import io.airbyte.workers.process.AsyncOrchestratorPodProcess
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workers.sync.ReplicationLauncherWorker
import io.airbyte.workload.launcher.model.getAttemptId
import io.airbyte.workload.launcher.model.getJobId
import io.airbyte.workload.launcher.model.getOrchestratorResourceReqs
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.serde.ObjectSerializer
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path

/**
 * Interface layer between domain and Docker layers.
 */
@Singleton
@Requires(notEnv = [Environment.KUBERNETES])
class DockerPodClient(
  private val serializer: ObjectSerializer,
  @Named("orchestratorEnvMap") private val orchestratorEnvMap: Map<String, String>,
  private val podLauncher: DockerPodLauncher,
  @Named("orchestratorKubeContainerInfo") private val orchestratorInfo: KubeContainerInfo,
  private val orchestratorNameGenerator: OrchestratorNameGenerator,
) : PodClient {
  override fun podsExistForWorkload(workloadId: String): Boolean = podLauncher.exists(workloadId)

  override fun launchReplication(
    replicationInput: ReplicationInput,
    launcherInput: LauncherInput,
  ) {
    val podConfig =
      DockerPodConfig(
        jobDir = Path.of(replicationInput.getJobId()).resolve(replicationInput.getAttemptId().toString()).resolve("orchestrator"),
        name = launcherInput.workloadId,
        imageName = orchestratorInfo.image,
        mutex = launcherInput.mutexKey,
        envMap = orchestratorEnvMap,
        fileMap = buildFileMap(replicationInput, replicationInput.jobRunConfig),
        orchestratorReqs = replicationInput.getOrchestratorResourceReqs(),
      )
    podLauncher.launch(podConfig)
  }

  override fun launchCheck(
    checkInput: CheckConnectionInput,
    launcherInput: LauncherInput,
  ) {
    TODO("Not yet implemented")
  }

  override fun deleteMutexPods(mutexKey: String): Boolean {
    val count = podLauncher.kill(mutexKey)
    return count > 0
  }

  // TODO: This is the way we pass data into the pods we launch. This should be extracted to
  //  some shared interface between parent / child to make it less brittle.
  private fun buildFileMap(
    input: ReplicationInput,
    jobRunConfig: JobRunConfig,
  ): Map<String, String> {
    return mapOf(
      OrchestratorConstants.INIT_FILE_ENV_MAP to serializer.serialize(orchestratorEnvMap),
      OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to serializer.serialize(jobRunConfig),
      AsyncOrchestratorPodProcess.KUBE_POD_INFO to
        serializer.serialize(
          KubePodInfo(
            orchestratorNameGenerator.namespace,
            orchestratorNameGenerator.getReplicationOrchestratorPodName(jobRunConfig.jobId, jobRunConfig.attemptId),
            orchestratorInfo,
          ),
        ),
      OrchestratorConstants.INIT_FILE_INPUT to serializer.serialize(input),
      OrchestratorConstants.INIT_FILE_APPLICATION to ReplicationLauncherWorker.REPLICATION,
      ReplicationLauncherWorker.INIT_FILE_SOURCE_LAUNCHER_CONFIG to serializer.serialize(input.sourceLauncherConfig),
      ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG to serializer.serialize(input.destinationLauncherConfig),
    )
  }
}
