package io.airbyte.workload.launcher.pods

import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.TolerationPOJO
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
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder
import io.fabric8.kubernetes.api.model.Toleration
import io.fabric8.kubernetes.api.model.TolerationBuilder
import io.fabric8.kubernetes.api.model.Volume
import io.fabric8.kubernetes.api.model.VolumeBuilder
import io.fabric8.kubernetes.api.model.VolumeMount
import io.fabric8.kubernetes.api.model.VolumeMountBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.micronaut.context.annotation.Value
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton

// TODO: make sure these beans are wired up
// TODO: Use this in KubePodClient to launch
@Singleton
class ConnectorPodLauncher(
  private val kubernetesClient: KubernetesClient,
  private val featureFlagClient: FeatureFlagClient,
  @Named("checkWorkerConfigs") private val checkWorkerConfigs: WorkerConfigs,
  @Named("checkConnectorReqs") private val checkReqs: ResourceRequirements,
  @Named("checkSidecarReqs") private val sidecarReqs: ResourceRequirements,
  @Named("checkEnvVars") private val envVars: List<EnvVar>,
  @Named("checkSideCarEnvVars") private val checkSideCarEnvVars: List<EnvVar>,
  @Named("sidecarKubeContainerInfo") private val sidecarContainerInfo: KubeContainerInfo,
  @Value("\${airbyte.worker.job.kube.serviceAccount}") private val serviceAccount: String?,
  @Value("\${google.application.credentials}") private val googleApplicationCredentials: String?,
  @Value("\${airbyte.container.orchestrator.secret-name}") private val secretName: String?,
  @Value("\${airbyte.container.orchestrator.secret-mount-path}") private val secretMountPath: String?,
  @Value("\${airbyte.container.orchestrator.data-plane-creds.secret-name}") private val dataPlaneCredsSecretName: String?,
  @Value("\${airbyte.container.orchestrator.data-plane-creds.secret-mount-path}") private val dataPlaneCredsSecretMountPath: String?,
) {
  fun create(
    allLabels: Map<String, String>,
    nodeSelectors: Map<String, String>,
    kubePodInfo: KubePodInfo,
    annotations: Map<String, String>,
  ): Pod {
    val volumes: MutableList<Volume> = ArrayList()
    val volumeMounts: MutableList<VolumeMount> = ArrayList()
    val sidecarVolumeMounts: MutableList<VolumeMount> = ArrayList()

    volumes.add(
      VolumeBuilder()
        .withName("airbyte-config")
        .withNewEmptyDir()
        .withMedium("Memory")
        .endEmptyDir()
        .build(),
    )

    volumeMounts.add(
      VolumeMountBuilder()
        .withName("airbyte-config")
        .withMountPath(KubePodProcess.CONFIG_DIR)
        .build(),
    )

    if (StringUtils.isNotEmpty(secretName) && StringUtils.isNotEmpty(secretMountPath) && StringUtils.isNotEmpty(googleApplicationCredentials)) {
      volumes.add(
        VolumeBuilder()
          .withName("airbyte-secret")
          .withSecret(
            SecretVolumeSourceBuilder()
              .withSecretName(secretName)
              .withDefaultMode(420)
              .build(),
          )
          .build(),
      )
      sidecarVolumeMounts.add(
        VolumeMountBuilder()
          .withName("airbyte-secret")
          .withMountPath(secretMountPath)
          .build(),
      )
    }

    if (StringUtils.isNotEmpty(dataPlaneCredsSecretName) && StringUtils.isNotEmpty(dataPlaneCredsSecretMountPath)) {
      volumes.add(
        VolumeBuilder()
          .withName("airbyte-dataplane-creds")
          .withSecret(
            SecretVolumeSourceBuilder()
              .withSecretName(dataPlaneCredsSecretName)
              .withDefaultMode(420)
              .build(),
          )
          .build(),
      )
      sidecarVolumeMounts.add(
        VolumeMountBuilder()
          .withName("airbyte-dataplane-creds")
          .withMountPath(dataPlaneCredsSecretMountPath)
          .build(),
      )
    }

    val pullSecrets: List<LocalObjectReference> =
      checkWorkerConfigs.jobImagePullSecrets
        .map { imagePullSecret -> LocalObjectReference(imagePullSecret) }
        .toList()

    val init: Container = buildInitContainer(volumeMounts)
    val main: Container = buildMainContainer(volumeMounts, kubePodInfo.mainContainerInfo)
    val sidecar: Container = buildSidecarContainer(volumeMounts + sidecarVolumeMounts)

    // TODO: We should inject the scheduler from the ENV and use this just for overrides
    val schedulerName = featureFlagClient.stringVariation(UseCustomK8sScheduler, Connection(ANONYMOUS))

    val podToCreate =
      PodBuilder()
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
        .withTolerations(buildPodTolerations(checkWorkerConfigs.workerKubeTolerations))
        .withImagePullSecrets(pullSecrets) // An empty list or an empty LocalObjectReference turns this into a no-op setting.
        .endSpec()
        .build()

    return kubernetesClient.pods()
      .inNamespace(kubePodInfo.namespace)
      .resource(podToCreate)
      .serverSideApply()
  }

  // TODO (after spike): inject this
  private fun buildPodTolerations(tolerations: List<TolerationPOJO>?): List<Toleration>? {
    if (tolerations.isNullOrEmpty()) {
      return null
    }
    return tolerations
      .map { t ->
        TolerationBuilder()
          .withKey(t.key)
          .withEffect(t.effect)
          .withOperator(t.operator)
          .withValue(t.value)
          .build()
      }
      .toList()
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
      .withResources(KubePodProcess.getResourceRequirementsBuilder(checkReqs).build()) // // TODO (after spike): inject this
      .withVolumeMounts(volumeMounts)
      .build()
  }

  private fun buildMainContainer(
    volumeMounts: List<VolumeMount>,
    containerInfo: KubeContainerInfo,
  ): Container {
    /**
     * TODO:
     * - create output file(s)
     * - run executable and pipe to file
     * - write finished file with exit code
     * - Override entry point and other stuff that main.sh is doing?
     * - Make this script generic?
     */
    val mainCommand =
      """
      pwd
      
      eval "${'$'}AIRBYTE_ENTRYPOINT check --config ${KubePodProcess.CONFIG_DIR}/connectionConfiguration.json" > ${KubePodProcess.CONFIG_DIR}/${OrchestratorConstants.JOB_OUTPUT_FILENAME}
      
      cat ${KubePodProcess.CONFIG_DIR}/${OrchestratorConstants.JOB_OUTPUT_FILENAME}
      
      echo $? > ${KubePodProcess.CONFIG_DIR}/${OrchestratorConstants.EXIT_CODE_FILE}
      """.trimIndent()

    return ContainerBuilder()
      .withName(KubePodProcess.MAIN_CONTAINER_NAME)
      .withImage(containerInfo.image)
      .withImagePullPolicy(checkWorkerConfigs.jobImagePullPolicy) // TODO: this should be properly set on the kubePodInfo
      .withCommand("sh", "-c", mainCommand)
      .withEnv(envVars)
      .withWorkingDir(KubePodProcess.CONFIG_DIR)
      .withVolumeMounts(volumeMounts)
      .withResources(KubePodProcess.getResourceRequirementsBuilder(checkReqs).build())
      .build()
  }

  private fun buildSidecarContainer(volumeMounts: List<VolumeMount>): Container {
    return ContainerBuilder()
      .withName(KubePodProcess.SIDECAR_CONTAINER_NAME)
      .withImage(sidecarContainerInfo.image)
      .withImagePullPolicy(sidecarContainerInfo.pullPolicy)
      .withWorkingDir(KubePodProcess.CONFIG_DIR)
      .withEnv(checkSideCarEnvVars)
      .withVolumeMounts(volumeMounts)
      .withResources(KubePodProcess.getResourceRequirementsBuilder(sidecarReqs).build())
      .build()
  }
}
