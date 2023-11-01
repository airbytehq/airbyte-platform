package io.airbyte.workload.launcher.pods

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ContainerOrchestratorConfig
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG
import io.airbyte.workers.sync.ReplicationLauncherWorker.INIT_FILE_SOURCE_LAUNCHER_CONFIG
import io.airbyte.workers.sync.ReplicationLauncherWorker.POD_NAME_PREFIX
import io.airbyte.workers.sync.ReplicationLauncherWorker.REPLICATION
import io.airbyte.workload.launcher.pods.KubePodClient.Constants.WORKLOAD_ID
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import pods.VendoredLauncher
import java.nio.file.Path

@Singleton
class KubePodClient(
  val launcher: VendoredLauncher,
  @Named("containerOrchestratorConfig") val containerOrchestratorConfig: ContainerOrchestratorConfig,
  @Value("\${micronaut.server.port}") val serverPort: Int,
  @Value("\${airbyte.workspace.root}") val workspaceRoot: String,
  private val featureFlagClient: FeatureFlagClient,
  private val metricClient: MetricClient,
  private val workerConfigs: WorkerConfigs,
) {
  object Constants {
    const val WORKLOAD_ID = "workload_id"
  }

  fun launchReplication(
    input: ReplicationInput,
    workloadId: String,
  ) {
    val jobRoot =
      buildJobRoot(
        workspaceRoot,
        input.jobRunConfig.jobId,
        input.jobRunConfig.attemptId,
      )

    val labels = mapOf(Pair(WORKLOAD_ID, workloadId))

    launcher.launchPod(
      input,
      jobRoot,
      labels,
      input.connectionId,
      input.workspaceId,
      REPLICATION,
      POD_NAME_PREFIX,
      input.jobRunConfig,
      mapOf(
        INIT_FILE_SOURCE_LAUNCHER_CONFIG to Jsons.serialize(input.sourceLauncherConfig),
        INIT_FILE_DESTINATION_LAUNCHER_CONFIG to Jsons.serialize(input.destinationLauncherConfig),
      ),
      containerOrchestratorConfig,
      input.syncResourceRequirements?.orchestrator,
      serverPort,
      workerConfigs,
      featureFlagClient,
      input.sourceLauncherConfig.isCustomConnector || input.destinationLauncherConfig.isCustomConnector,
      metricClient,
    )
  }

  private fun buildJobRoot(
    workspaceRoot: String,
    jobId: String,
    attemptId: Long,
  ): Path {
    return Path.of(workspaceRoot)
      .resolve(jobId)
      .resolve(attemptId.toString())
  }
}
