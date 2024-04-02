package io.airbyte.workload.launcher.pods.factories

import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseCustomK8sScheduler
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.KubePodProcess
import io.airbyte.workers.sync.OrchestratorConstants
import io.fabric8.kubernetes.api.model.CapabilitiesBuilder
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.LocalObjectReference
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.PodSecurityContext
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.SeccompProfileBuilder
import io.fabric8.kubernetes.api.model.SecurityContext
import io.fabric8.kubernetes.api.model.SecurityContextBuilder
import io.fabric8.kubernetes.api.model.Toleration
import io.fabric8.kubernetes.api.model.Volume
import io.fabric8.kubernetes.api.model.VolumeMount
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
) {
  fun create(
    allLabels: Map<String, String>,
    nodeSelectors: Map<String, String>,
    kubePodInfo: KubePodInfo,
    annotations: Map<String, String>,
    extraEnvVars: List<EnvVar>,
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

    val connectorResourceReqs = KubePodProcess.getResourceRequirementsBuilder(connectorReqs).build()

    val init: Container = initContainerFactory.create(connectorResourceReqs, volumeMounts)
    val main: Container = buildMainContainer(connectorResourceReqs, volumeMounts, kubePodInfo.mainContainerInfo, extraEnvVars)
    val sidecar: Container = buildSidecarContainer(volumeMounts + secretVolumeMounts)

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
      .withSecurityContext(podSecurityContext())
      .endSpec()
      .build()
  }

  private fun buildMainContainer(
    resourceReqs: ResourceRequirements,
    volumeMounts: List<VolumeMount>,
    containerInfo: KubeContainerInfo,
    extraEnvVars: List<EnvVar>,
  ): Container {
    val configArg =
      connectorArgs.map {
          (k, v) ->
        "--$k $v"
      }.joinToString(prefix = " ", separator = " ")

    val mainCommand =
      """
      pwd

      eval "${'$'}AIRBYTE_ENTRYPOINT $operationCommand $configArg" > ${KubePodProcess.CONFIG_DIR}/${OrchestratorConstants.JOB_OUTPUT_FILENAME}
      
      cat ${KubePodProcess.CONFIG_DIR}/${OrchestratorConstants.JOB_OUTPUT_FILENAME}

      echo $? > ${KubePodProcess.CONFIG_DIR}/${OrchestratorConstants.EXIT_CODE_FILE}
      """.trimIndent()

    return ContainerBuilder()
      .withName(KubePodProcess.MAIN_CONTAINER_NAME)
      .withImage(containerInfo.image)
      .withImagePullPolicy(containerInfo.pullPolicy)
      .withCommand("sh", "-c", mainCommand)
      .withEnv(connectorEnvVars + extraEnvVars)
      .withWorkingDir(KubePodProcess.CONFIG_DIR)
      .withVolumeMounts(volumeMounts)
      .withResources(resourceReqs)
      .withSecurityContext(containerSecurityContext())
      .build()
  }

  private fun buildSidecarContainer(volumeMounts: List<VolumeMount>): Container {
    return ContainerBuilder()
      .withName(KubePodProcess.SIDECAR_CONTAINER_NAME)
      .withImage(sidecarContainerInfo.image)
      .withImagePullPolicy(sidecarContainerInfo.pullPolicy)
      .withWorkingDir(KubePodProcess.CONFIG_DIR)
      .withEnv(sideCarEnvVars)
      .withVolumeMounts(volumeMounts)
      .withResources(KubePodProcess.getResourceRequirementsBuilder(sidecarReqs).build())
      .withSecurityContext(containerSecurityContext())
      .build()
  }

  companion object {
    val CHECK_OPERATION_NAME = "check"
    val DISCOVER_OPERATION_NAME = "discover"
    val SPEC_OPERATION_NAME = "spec"
  }
}

private fun containerSecurityContext(): SecurityContext? =
  when (io.airbyte.commons.envvar.EnvVar.ROOTLESS_WORKLOAD.fetch(default = "false").toBoolean()) {
    true ->
      SecurityContextBuilder()
        .withAllowPrivilegeEscalation(false)
        .withRunAsUser(1000L)
        .withRunAsGroup(1000L)
        .withReadOnlyRootFilesystem(false)
        .withRunAsNonRoot(true)
        .withCapabilities(CapabilitiesBuilder().addAllToDrop(listOf("ALL")).build())
        .withSeccompProfile(SeccompProfileBuilder().withType("RuntimeDefault").build())
        .build()
    false -> null
  }

private fun podSecurityContext(): PodSecurityContext? =
  when (io.airbyte.commons.envvar.EnvVar.ROOTLESS_WORKLOAD.fetch(default = "false").toBoolean()) {
    true -> PodSecurityContextBuilder().withFsGroup(1000L).build()
    false -> null
  }
