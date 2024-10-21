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
import io.fabric8.kubernetes.api.model.Volume
import io.fabric8.kubernetes.api.model.VolumeMount
import java.util.UUID
import io.airbyte.config.ResourceRequirements as AirbyteResourceRequirements

class ConnectorPodFactory(
  private val operationCommand: String,
  private val featureFlagClient: FeatureFlagClient,
  private val connectorReqs: AirbyteResourceRequirements,
  private val sidecarReqs: AirbyteResourceRequirements,
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
) {
  fun create(
    allLabels: Map<String, String>,
    nodeSelectors: Map<String, String>,
    kubePodInfo: KubePodInfo,
    annotations: Map<String, String>,
    runtimeEnvVars: List<EnvVar>,
    useFetchingInit: Boolean,
    workspaceId: UUID,
  ): Pod {
    val volumes: MutableList<Volume> = ArrayList()
    val volumeMounts: MutableList<VolumeMount> = ArrayList()
    val secretVolumeMounts: MutableList<VolumeMount> = ArrayList()

    val config = volumeFactory.config()
    volumes.add(config.volume)
    volumeMounts.add(config.mount)

    val secrets = volumeFactory.secret()
    if (secrets != null) {
      volumes.add(secrets.volume)
      secretVolumeMounts.add(secrets.mount)
    }

    val dataPlaneCreds = volumeFactory.dataplaneCreds()
    if (dataPlaneCreds != null) {
      volumes.add(dataPlaneCreds.volume)
      secretVolumeMounts.add(dataPlaneCreds.mount)
    }

    val connectorResourceReqs = ResourceConversionUtils.buildResourceRequirements(connectorReqs)
    val internalVolumeMounts = volumeMounts + secretVolumeMounts

    val init: Container =
      if (useFetchingInit) {
        initContainerFactory.createFetching(connectorResourceReqs, internalVolumeMounts, runtimeEnvVars, workspaceId)
      } else {
        initContainerFactory.createWaiting(connectorResourceReqs, internalVolumeMounts)
      }

    val main: Container = buildMainContainer(connectorResourceReqs, volumeMounts, kubePodInfo.mainContainerInfo, runtimeEnvVars)
    val sidecar: Container = buildSidecarContainer(internalVolumeMounts)

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
      .withVolumes(volumes)
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

    return ContainerBuilder()
      .withName(ContainerConstants.SIDECAR_CONTAINER_NAME)
      .withImage(sidecarContainerInfo.image)
      .withImagePullPolicy(sidecarContainerInfo.pullPolicy)
      .withCommand("sh", "-c", mainCommand)
      .withWorkingDir(FileConstants.CONFIG_DIR)
      .withEnv(sideCarEnvVars)
      .withVolumeMounts(volumeMounts)
      .withResources(ResourceConversionUtils.buildResourceRequirements(sidecarReqs))
      .withSecurityContext(workloadSecurityContextProvider.rootlessContainerSecurityContext())
      .build()
  }

  companion object {
    const val CHECK_OPERATION_NAME = "check"
    const val DISCOVER_OPERATION_NAME = "discover"
    const val SPEC_OPERATION_NAME = "spec"
  }
}
