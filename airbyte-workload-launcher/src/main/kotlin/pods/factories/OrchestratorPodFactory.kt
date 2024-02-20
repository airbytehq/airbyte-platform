package io.airbyte.workload.launcher.pods.factories

import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseCustomK8sScheduler
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.KubePodProcess
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.ContainerPort
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.Volume
import io.fabric8.kubernetes.api.model.VolumeMount
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
class OrchestratorPodFactory(
  private val featureFlagClient: FeatureFlagClient,
  @Value("\${airbyte.worker.job.kube.serviceAccount}") private val serviceAccount: String?,
  @Named("orchestratorEnvVars") private val sharedEnvVars: List<EnvVar>,
  @Named("orchestratorContainerPorts") private val containerPorts: List<ContainerPort>,
  private val volumeFactory: VolumeFactory,
) {
  fun create(
    allLabels: Map<String, String>,
    resourceRequirements: ResourceRequirements?,
    nodeSelectors: Map<String, String>,
    kubePodInfo: KubePodInfo,
    annotations: Map<String, String>,
    additionalEnvVars: Map<String, String>,
  ): Pod {
    val volumes: MutableList<Volume> = ArrayList()
    val volumeMounts: MutableList<VolumeMount> = ArrayList()

    val config = volumeFactory.config()
    volumes.add(config.volume)
    volumeMounts.add(config.mount)

    val secrets = volumeFactory.secret()
    if (secrets != null) {
      volumes.add(secrets.volume)
      volumeMounts.add(secrets.mount)
    }

    val dataPlaneCreds = volumeFactory.dataplaneCreds()
    if (dataPlaneCreds != null) {
      volumes.add(dataPlaneCreds.volume)
      volumeMounts.add(dataPlaneCreds.mount)
    }

    val containerResources = KubePodProcess.getResourceRequirementsBuilder(resourceRequirements).build()

    val initContainer =
      ContainerBuilder()
        .withName(KubePodProcess.INIT_CONTAINER_NAME)
        .withImage("busybox:1.35")
        .withVolumeMounts(volumeMounts)
        .withResources(containerResources)
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
        .build()

    val extraKubeEnv = additionalEnvVars.map { (k, v) -> EnvVar(k, v, null) }

    val mainContainer =
      ContainerBuilder()
        .withName(KubePodProcess.MAIN_CONTAINER_NAME)
        .withImage(kubePodInfo.mainContainerInfo.image)
        .withImagePullPolicy(kubePodInfo.mainContainerInfo.pullPolicy)
        .withResources(containerResources)
        .withEnv(sharedEnvVars + extraKubeEnv)
        .withPorts(containerPorts)
        .withVolumeMounts(volumeMounts)
        .build()

    // TODO: We should inject the scheduler from the ENV and use this just for overrides
    val schedulerName = featureFlagClient.stringVariation(UseCustomK8sScheduler, Connection(ANONYMOUS))

    return PodBuilder()
      .withApiVersion("v1")
      .withNewMetadata()
      .withName(kubePodInfo.name)
      .withNamespace(kubePodInfo.namespace)
      .withLabels<Any, Any>(allLabels)
      .withAnnotations<Any, Any>(annotations)
      .endMetadata()
      .withNewSpec()
      .withSchedulerName(schedulerName)
      .withServiceAccount(serviceAccount)
      .withAutomountServiceAccountToken(true)
      .withRestartPolicy("Never")
      .withContainers(mainContainer)
      .withInitContainers(initContainer)
      .withVolumes(volumes)
      .withNodeSelector<Any, Any>(nodeSelectors)
      .endSpec()
      .build()
  }
}
