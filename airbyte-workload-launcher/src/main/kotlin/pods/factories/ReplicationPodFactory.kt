package io.airbyte.workload.launcher.pods.factories

import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.AllowSpotInstances
import io.airbyte.featureflag.Attempt
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.UseCustomK8sScheduler
import io.airbyte.featureflag.Workspace
import io.airbyte.workers.context.WorkloadSecurityContextProvider
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.LocalObjectReference
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.Toleration
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
data class ReplicationPodFactory(
  private val featureFlagClient: FeatureFlagClient,
  private val initContainerFactory: InitContainerFactory,
  private val replContainerFactory: ReplicationContainerFactory,
  private val volumeFactory: VolumeFactory,
  private val workloadSecurityContextProvider: WorkloadSecurityContextProvider,
  @Named("replicationImagePullSecrets") private val imagePullSecrets: List<LocalObjectReference>,
  @Named("replicationPodTolerations") private val tolerations: List<Toleration>,
  @Named("spotTolerations") private val spotTolerations: List<Toleration>,
  @Named("infraFlagContexts") private val infraFlagContexts: List<Context>,
) {
  fun create(
    podName: String,
    allLabels: Map<String, String>,
    annotations: Map<String, String>,
    nodeSelectors: Map<String, String>,
    // TODO: Consider moving container creation to the caller to avoid prop drilling.
    orchImage: String,
    sourceImage: String,
    destImage: String,
    orchResourceReqs: ResourceRequirements?,
    sourceResourceReqs: ResourceRequirements?,
    destResourceReqs: ResourceRequirements?,
    orchRuntimeEnvVars: List<EnvVar>,
    sourceRuntimeEnvVars: List<EnvVar>,
    destRuntimeEnvVars: List<EnvVar>,
    isFileTransfer: Boolean,
    workspaceId: UUID,
  ): Pod {
    // TODO: We should inject the scheduler from the ENV and use this just for overrides
    val schedulerName = featureFlagClient.stringVariation(UseCustomK8sScheduler, Connection(ANONYMOUS))

    val replicationVolumes = volumeFactory.replication(isFileTransfer)
    val initContainer = initContainerFactory.create(orchResourceReqs, replicationVolumes.orchVolumeMounts, orchRuntimeEnvVars, workspaceId)

    val orchContainer =
      replContainerFactory.createOrchestrator(
        orchResourceReqs,
        replicationVolumes.orchVolumeMounts,
        orchRuntimeEnvVars,
        orchImage,
      )

    val sourceContainer =
      replContainerFactory.createSource(
        sourceResourceReqs,
        replicationVolumes.sourceVolumeMounts,
        sourceRuntimeEnvVars,
        sourceImage,
      )

    val destContainer =
      replContainerFactory.createDestination(
        destResourceReqs,
        replicationVolumes.destVolumeMounts,
        destRuntimeEnvVars,
        destImage,
      )

    val tolerationsForReplicationPod = getReplicationTolerations(allLabels)

    return PodBuilder()
      .withApiVersion("v1")
      .withNewMetadata()
      .withName(podName)
      .withLabels<Any, Any>(allLabels)
      .withAnnotations<Any, Any>(annotations)
      .endMetadata()
      .withNewSpec()
      .withSchedulerName(schedulerName)
      .withAutomountServiceAccountToken(true)
      .withRestartPolicy("Never")
      .withInitContainers(initContainer)
      .withContainers(orchContainer, sourceContainer, destContainer)
      .withImagePullSecrets(imagePullSecrets)
      .withVolumes(replicationVolumes.allVolumes)
      .withNodeSelector<Any, Any>(nodeSelectors)
      .withTolerations(tolerationsForReplicationPod)
      .withAutomountServiceAccountToken(false)
      .withSecurityContext(workloadSecurityContextProvider.defaultPodSecurityContext())
      .endSpec()
      .build()
  }

  fun createReset(
    podName: String,
    allLabels: Map<String, String>,
    annotations: Map<String, String>,
    nodeSelectors: Map<String, String>,
    // TODO: Consider moving container creation to the caller to avoid prop drilling.
    orchImage: String,
    destImage: String,
    orchResourceReqs: ResourceRequirements?,
    destResourceReqs: ResourceRequirements?,
    orchRuntimeEnvVars: List<EnvVar>,
    destRuntimeEnvVars: List<EnvVar>,
    isFileTransfer: Boolean,
    workspaceId: UUID,
  ): Pod {
    // TODO: We should inject the scheduler from the ENV and use this just for overrides
    val schedulerName = featureFlagClient.stringVariation(UseCustomK8sScheduler, Connection(ANONYMOUS))

    val replicationVolumes = volumeFactory.replication(isFileTransfer)
    val initContainer = initContainerFactory.create(orchResourceReqs, replicationVolumes.orchVolumeMounts, orchRuntimeEnvVars, workspaceId)

    val orchContainer =
      replContainerFactory.createOrchestrator(
        orchResourceReqs,
        replicationVolumes.orchVolumeMounts,
        orchRuntimeEnvVars,
        orchImage,
      )

    val destContainer =
      replContainerFactory.createDestination(
        destResourceReqs,
        replicationVolumes.destVolumeMounts,
        destRuntimeEnvVars,
        destImage,
      )

    return PodBuilder()
      .withApiVersion("v1")
      .withNewMetadata()
      .withName(podName)
      .withLabels<Any, Any>(allLabels)
      .withAnnotations<Any, Any>(annotations)
      .endMetadata()
      .withNewSpec()
      .withSchedulerName(schedulerName)
      .withAutomountServiceAccountToken(true)
      .withRestartPolicy("Never")
      .withInitContainers(initContainer)
      .withContainers(orchContainer, destContainer)
      .withImagePullSecrets(imagePullSecrets)
      .withVolumes(replicationVolumes.allVolumes)
      .withNodeSelector<Any, Any>(nodeSelectors)
      .withTolerations(tolerations)
      .withAutomountServiceAccountToken(false)
      .withSecurityContext(workloadSecurityContextProvider.defaultPodSecurityContext())
      .endSpec()
      .build()
  }

  private fun getReplicationTolerations(allLabels: Map<String, String>): List<Toleration> {
    val context = buildReplicationTolerationsContext(allLabels)
    if (featureFlagClient.boolVariation(AllowSpotInstances, context)) {
      return tolerations + spotTolerations
    } else {
      return tolerations.toList()
    }
  }

  private fun buildReplicationTolerationsContext(allLabels: Map<String, String>): Context {
    val context = infraFlagContexts.toMutableList()
    allLabels["connection_id"]?.let { connectionId -> context.add(Connection(connectionId)) }
    allLabels["workspace_id"]?.let { workspaceId -> context.add(Workspace(workspaceId)) }
    allLabels["attempt_id"]?.let { attemptNumber -> context.add(Attempt(attemptNumber)) }
    return Multi(context)
  }
}
