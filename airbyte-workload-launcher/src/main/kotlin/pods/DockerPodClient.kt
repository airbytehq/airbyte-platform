package io.airbyte.workload.launcher.pods

import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.orchestrator.PodNameGenerator
import io.airbyte.workers.process.AsyncOrchestratorPodProcess
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workers.sync.ReplicationLauncherWorker
import io.airbyte.workload.launcher.config.OrchestratorEnvSingleton
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
import java.util.UUID

/**
 * Interface layer between domain and Docker layers.
 */
@Singleton
@Requires(notEnv = [Environment.KUBERNETES])
class DockerPodClient(
  private val serializer: ObjectSerializer,
  private val orchestratorEnvSingleton: OrchestratorEnvSingleton,
  private val podLauncher: DockerPodLauncher,
  @Named("orchestratorKubeContainerInfo") private val orchestratorInfo: KubeContainerInfo,
  private val podNameGenerator: PodNameGenerator,
) : PodClient {
  override fun podsExistForAutoId(autoId: UUID): Boolean = podLauncher.exists(autoId.toString())

  override fun launchReplication(
    replicationInput: ReplicationInput,
    launcherInput: LauncherInput,
  ) {
    val podConfig =
      DockerPodConfig(
        jobDir = Path.of(replicationInput.getJobId()).resolve(replicationInput.getAttemptId().toString()).resolve("orchestrator"),
        name = launcherInput.autoId.toString(),
        imageName = orchestratorInfo.image,
        mutex = launcherInput.mutexKey,
        envMap = orchestratorEnvSingleton.orchestratorEnvMap(replicationInput.connectionId),
        fileMap = buildFileMap(launcherInput.workloadId, replicationInput, replicationInput.jobRunConfig),
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

  override fun launchDiscover(
    discoverCatalogInput: DiscoverCatalogInput,
    launcherInput: LauncherInput,
  ) {
    TODO("Not yet implemented")
  }

  override fun launchSpec(
    specInput: SpecInput,
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
    workloadId: String,
    input: ReplicationInput,
    jobRunConfig: JobRunConfig,
  ): Map<String, String> {
    return mapOf(
      OrchestratorConstants.INIT_FILE_ENV_MAP to serializer.serialize(orchestratorEnvSingleton.orchestratorEnvMap(input.connectionId)),
      OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG to serializer.serialize(jobRunConfig),
      AsyncOrchestratorPodProcess.KUBE_POD_INFO to
        serializer.serialize(
          KubePodInfo(
            podNameGenerator.namespace,
            podNameGenerator.getReplicationOrchestratorPodName(jobRunConfig.jobId, jobRunConfig.attemptId),
            orchestratorInfo,
          ),
        ),
      OrchestratorConstants.INIT_FILE_INPUT to serializer.serialize(input),
      OrchestratorConstants.INIT_FILE_APPLICATION to ReplicationLauncherWorker.REPLICATION,
      OrchestratorConstants.WORKLOAD_ID_FILE to workloadId,
      ReplicationLauncherWorker.INIT_FILE_SOURCE_LAUNCHER_CONFIG to serializer.serialize(input.sourceLauncherConfig),
      ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG to serializer.serialize(input.destinationLauncherConfig),
    )
  }
}
