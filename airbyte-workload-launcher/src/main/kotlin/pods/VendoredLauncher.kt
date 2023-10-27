/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package pods

import io.airbyte.commons.constants.WorkerConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseCustomK8sScheduler
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ContainerOrchestratorConfig
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.process.AsyncOrchestratorPodProcess
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.KubeProcessFactory
import io.airbyte.workers.process.Metadata
import io.airbyte.workers.sync.LauncherWorker
import io.airbyte.workers.sync.OrchestratorConstants
import io.fabric8.kubernetes.client.KubernetesClientException
import jakarta.inject.Singleton
import java.nio.file.Path
import java.util.UUID
import kotlin.collections.HashMap

/**
 * This is the ReplicationLauncher code copied over from worker-commons but streamlined into 1
 * method and auto-kotlin-ed. This exists just to get us merged into master. This will be re-written
 * and simplified for our specific needs.
 */
@Singleton
class VendoredLauncher {
  @Throws(WorkerException::class)
  fun launchPod(
    input: ReplicationInput,
    jobRoot: Path?,
    labelOverrides: Map<String, String>?,
    connectionId: UUID?,
    workspaceId: UUID?,
    application: String,
    podNamePrefix: String,
    jobRunConfig: JobRunConfig,
    additionalFileMap: Map<String, String>?,
    containerOrchestratorConfig: ContainerOrchestratorConfig,
    resourceRequirements: ResourceRequirements?,
    serverPort: Int,
    workerConfigs: WorkerConfigs,
    featureFlagClient: FeatureFlagClient,
    isCustomConnector: Boolean,
    metricClient: MetricClient?,
  ): AsyncOrchestratorPodProcess {
    // Assemble configuration.
    val envMap =
      System.getenv()
        .filter { (key): Map.Entry<String?, String?> -> OrchestratorConstants.ENV_VARS_TO_TRANSFER.contains(key) }.toMutableMap()
    // Manually add the worker environment to the env var map
    envMap[WorkerConstants.WORKER_ENVIRONMENT] = containerOrchestratorConfig.workerEnvironment.name

    // Merge in the env from the ContainerOrchestratorConfig
    containerOrchestratorConfig.environmentVariables.entries.stream().forEach {
        (key, value): Map.Entry<String, String> ->
      envMap.putIfAbsent(key, value)
    }
    val fileMap: MutableMap<String, String> = HashMap(additionalFileMap)
    fileMap.putAll(
      java.util.Map.of(
        OrchestratorConstants.INIT_FILE_APPLICATION,
        application,
        OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG,
        Jsons.serialize(jobRunConfig),
        OrchestratorConstants.INIT_FILE_INPUT,
        Jsons.serialize(input),
        // OrchestratorConstants.INIT_FILE_ENV_MAP might be duplicated since the pod env contains everything
        OrchestratorConstants.INIT_FILE_ENV_MAP,
        Jsons.serialize<Map<String, String>>(envMap),
      ),
    )
    val portMap =
      java.util.Map.of(
        serverPort, serverPort,
        OrchestratorConstants.PORT1, OrchestratorConstants.PORT1,
        OrchestratorConstants.PORT2, OrchestratorConstants.PORT2,
        OrchestratorConstants.PORT3, OrchestratorConstants.PORT3,
        OrchestratorConstants.PORT4, OrchestratorConstants.PORT4,
      )
    val podNameAndJobPrefix = podNamePrefix + "-job-" + jobRunConfig.jobId + "-attempt-"
    val podName = podNameAndJobPrefix + jobRunConfig.attemptId
    val mainContainerInfo =
      KubeContainerInfo(
        containerOrchestratorConfig.containerOrchestratorImage,
        containerOrchestratorConfig.containerOrchestratorImagePullPolicy,
      )
    val kubePodInfo =
      KubePodInfo(
        containerOrchestratorConfig.namespace,
        podName,
        mainContainerInfo,
      )
    ApmTraceUtils.addTagsToTrace(connectionId, jobRunConfig.attemptId, jobRunConfig.jobId, jobRoot)
    val schedulerName = featureFlagClient.stringVariation(UseCustomK8sScheduler, Connection(connectionId!!))
    val allLabels =
      KubeProcessFactory.getLabels(
        jobRunConfig.jobId,
        Math.toIntExact(jobRunConfig.attemptId),
        connectionId,
        workspaceId,
        mainContainerInfo.image,
        generateMetadataLabels(connectionId),
        emptyMap(),
      )
    allLabels.putAll(labelOverrides!!)

    // Use the configuration to create the process.
    val process =
      AsyncOrchestratorPodProcess(
        kubePodInfo,
        containerOrchestratorConfig.documentStoreClient,
        containerOrchestratorConfig.kubernetesClient,
        containerOrchestratorConfig.secretName,
        containerOrchestratorConfig.secretMountPath,
        containerOrchestratorConfig.dataPlaneCredsSecretName,
        containerOrchestratorConfig.dataPlaneCredsSecretMountPath,
        containerOrchestratorConfig.googleApplicationCredentials,
        envMap,
        workerConfigs.workerKubeAnnotations,
        serverPort,
        containerOrchestratorConfig.serviceAccount,
        if (schedulerName.isBlank()) null else schedulerName,
        metricClient,
        launcherType,
      )

    // custom connectors run in an isolated node pool from airbyte-supported connectors
    // to reduce the blast radius of any problems with custom connector code.
    val nodeSelectors =
      if (isCustomConnector) {
        workerConfigs.workerIsolatedKubeNodeSelectors.orElse(
          workerConfigs.getworkerKubeNodeSelectors(),
        )
      } else {
        workerConfigs.getworkerKubeNodeSelectors()
      }
    try {
      process.create(
        allLabels,
        resourceRequirements,
        fileMap,
        portMap,
        nodeSelectors,
      )
    } catch (e: KubernetesClientException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw WorkerException(
        "Failed to create pod $podName, pre-existing pod exists which didn't advance out of the NOT_STARTED state.",
        e,
      )
    }
    return process
  }

  private fun generateMetadataLabels(connectionId: UUID?): Map<String, String> {
    val processId = UUID.randomUUID()
    val metadataLabels: MutableMap<String, String> = HashMap()
    metadataLabels[LauncherWorker.PROCESS_ID_LABEL_KEY] = processId.toString()
    metadataLabels.putAll(generateCustomMetadataLabels())
    if (connectionId != null) {
      metadataLabels[Metadata.CONNECTION_ID_LABEL_KEY] = connectionId.toString()
    }
    return metadataLabels
  }

  protected fun generateCustomMetadataLabels(): Map<String, String> {
    return java.util.Map.of(Metadata.SYNC_STEP_KEY, Metadata.ORCHESTRATOR_REPLICATION_STEP)
  }

  protected val launcherType: String
    protected get() = "Replication"
}
