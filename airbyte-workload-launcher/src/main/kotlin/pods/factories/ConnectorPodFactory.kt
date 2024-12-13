package io.airbyte.workload.launcher.pods.factories

import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseCustomK8sScheduler
import io.airbyte.workers.context.WorkloadSecurityContextProvider
import io.airbyte.workers.pod.ContainerConstants
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.pod.KubeContainerInfo
import io.airbyte.workers.pod.KubePodInfo
import io.airbyte.workers.pod.ResourceConversionUtils
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.LocalObjectReference
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.Toleration
import io.fabric8.kubernetes.api.model.VolumeMount
import java.util.UUID

data class ConnectorPodFactory(
  private val operationCommand: String,
  private val featureFlagClient: FeatureFlagClient,
  private val tolerations: List<Toleration>,
  private val imagePullSecrets: List<LocalObjectReference>,
  private val connectorEnvVars: List<EnvVar>,
  private val sideCarEnvVars: List<EnvVar>,
  private val sidecarContainerInfo: KubeContainerInfo,
  private val serviceAccount: String?,
  private val volumeFactory: VolumeFactory,
  private val initContainerFactory: InitContainerFactory,
  private val connectorArgs: Map<String, String>,
  private val workloadSecurityContextProvider: WorkloadSecurityContextProvider,
  private val resourceRequirementsFactory: ResourceRequirementsFactory,
) {
  fun create(
    allLabels: Map<String, String>,
    nodeSelectors: Map<String, String>,
    kubePodInfo: KubePodInfo,
    annotations: Map<String, String>,
    connectorContainerReqs: ResourceRequirements,
    initContainerReqs: ResourceRequirements,
    runtimeEnvVars: List<EnvVar>,
    workspaceId: UUID,
  ): Pod {
    val volumeMountPairs = volumeFactory.connector()

    val init: Container = initContainerFactory.create(initContainerReqs, volumeMountPairs.initMounts, runtimeEnvVars, workspaceId)
    val main: Container = buildMainContainer(connectorContainerReqs, volumeMountPairs.mainMounts, kubePodInfo.mainContainerInfo, runtimeEnvVars)
    val sidecar: Container = buildSidecarContainer(volumeMountPairs.sidecarMounts)

    // TODO: We should inject the scheduler from the ENV and use this just for overrides
    val schedulerName = featureFlagClient.stringVariation(UseCustomK8sScheduler, Connection(ANONYMOUS))

    return PodBuilder()
      .withApiVersion("v1")
      .withNewMetadata()
      .withName(kubePodInfo.name)
      .withLabels<String, String>(allLabels)
      .withAnnotations<String, String>(annotations)
      .endMetadata()
      .withNewSpec()
      .withSchedulerName(schedulerName)
      .withServiceAccount(serviceAccount)
      .withAutomountServiceAccountToken(true)
      .withRestartPolicy("Never")
      .withContainers(sidecar, main)
      .withInitContainers(init)
      .withVolumes(volumeMountPairs.volumes)
      .withNodeSelector<String, String>(nodeSelectors)
      .withTolerations(tolerations)
      .withImagePullSecrets(imagePullSecrets) // An empty list or an empty LocalObjectReference turns this into a no-op setting.
      .withSecurityContext(workloadSecurityContextProvider.defaultPodSecurityContext())
      .endSpec()
      .build()
  }

  private fun buildMainContainer(
    resourceReqs: ResourceRequirements,
    volumeMounts: List<VolumeMount>,
    containerInfo: KubeContainerInfo,
    runtimeEnvVars: List<EnvVar>,
  ): Container {
    val configArgs =
      connectorArgs.map {
          (k, v) ->
        "--$k $v"
      }.joinToString(prefix = " ", separator = " ")

    val mainCommand = ContainerCommandFactory.connectorOperation(operationCommand, configArgs)

    return ContainerBuilder()
      .withName(ContainerConstants.MAIN_CONTAINER_NAME)
      .withImage(containerInfo.image)
      .withImagePullPolicy(containerInfo.pullPolicy)
      .withCommand("sh", "-c", mainCommand)
      .withEnv(connectorEnvVars + runtimeEnvVars)
      .withWorkingDir(FileConstants.CONFIG_DIR)
      .withVolumeMounts(volumeMounts)
      .withResources(resourceReqs)
      .withSecurityContext(workloadSecurityContextProvider.rootlessContainerSecurityContext())
      .build()
  }

  private fun buildSidecarContainer(volumeMounts: List<VolumeMount>): Container {
    val mainCommand = ContainerCommandFactory.sidecar()
    val sidecarReqs = resourceRequirementsFactory.sidecar()

    return ContainerBuilder()
      .withName(ContainerConstants.SIDECAR_CONTAINER_NAME)
      .withImage(sidecarContainerInfo.image)
      .withImagePullPolicy(sidecarContainerInfo.pullPolicy)
      .withCommand("sh", "-c", mainCommand)
      .withWorkingDir(FileConstants.CONFIG_DIR)
      .withEnv(sideCarEnvVars)
      .withVolumeMounts(volumeMounts)
      .withResources(ResourceConversionUtils.domainToApi(sidecarReqs))
      .withSecurityContext(workloadSecurityContextProvider.rootlessContainerSecurityContext())
      .build()
  }

  companion object {
    const val CHECK_OPERATION_NAME = "check"
    const val DISCOVER_OPERATION_NAME = "discover"
    const val SPEC_OPERATION_NAME = "spec"
  }
}
