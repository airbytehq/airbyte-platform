package io.airbyte.workload.launcher.pods.factories

import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseCustomK8sScheduler
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.KubePodProcess
import io.airbyte.workers.sync.OrchestratorConstants
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.LocalObjectReference
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.Toleration
import io.fabric8.kubernetes.api.model.Volume
import io.fabric8.kubernetes.api.model.VolumeMount

class ConnectorPodFactory(
  private val operationCommand: String,
  private val featureFlagClient: FeatureFlagClient,
  private val connectorReqs: ResourceRequirements,
  private val sidecarReqs: ResourceRequirements,
  private val tolerations: List<Toleration>,
  private val imagePullSecrets: List<LocalObjectReference>,
  private val connectorEnvVars: List<EnvVar>,
  private val sideCarEnvVars: List<EnvVar>,
  private val sidecarContainerInfo: KubeContainerInfo,
  private val serviceAccount: String?,
  private val volumeFactory: VolumeFactory,
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

    val init: Container = buildInitContainer(volumeMounts)
    val main: Container = buildMainContainer(volumeMounts, kubePodInfo.mainContainerInfo, extraEnvVars)
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
      .endSpec()
      .build()
  }

  private fun buildInitContainer(volumeMounts: List<VolumeMount>): Container {
    return ContainerBuilder()
      .withName(KubePodProcess.INIT_CONTAINER_NAME)
      .withImage("busybox:1.35")
      .withWorkingDir(KubePodProcess.CONFIG_DIR)
      .withCommand(
        listOf(
          "sh",
          "-c",
          String.format(
            """
            i=0
            until [ ${'$'}i -gt 60 ]
            do
              echo "${'$'}i - waiting for config file transfer to complete..."
              # check if the upload-complete file exists, if so exit without error
              if [ -f "%s/%s" ]; then
                # Wait 50ms for the incoming kubectl cp call to cleanly exit
                sleep .05
                exit 0
              fi
              i=${'$'}((i+1))
              sleep 1
            done
            echo "config files did not transfer in time"
            # no upload-complete file was created in time, exit with error
            exit 1
            """.trimIndent(),
            KubePodProcess.CONFIG_DIR,
            KubePodProcess.SUCCESS_FILE_NAME,
          ),
        ),
      )
      .withResources(KubePodProcess.getResourceRequirementsBuilder(connectorReqs).build())
      .withVolumeMounts(volumeMounts)
      .build()
  }

  private fun buildMainContainer(
    volumeMounts: List<VolumeMount>,
    containerInfo: KubeContainerInfo,
    extraEnvVars: List<EnvVar>,
  ): Container {
    val mainCommand =
      """
      pwd

      eval "${'$'}AIRBYTE_ENTRYPOINT $operationCommand --config ${KubePodProcess.CONFIG_DIR}/connectionConfiguration.json" > ${KubePodProcess.CONFIG_DIR}/${OrchestratorConstants.JOB_OUTPUT_FILENAME}
      
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
      .withResources(KubePodProcess.getResourceRequirementsBuilder(connectorReqs).build())
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
      .build()
  }
}
