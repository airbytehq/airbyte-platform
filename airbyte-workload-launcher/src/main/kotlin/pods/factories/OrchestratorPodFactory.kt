package io.airbyte.workload.launcher.pods.factories

import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseCustomK8sScheduler
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.KubePodProcess
import io.airbyte.workload.launcher.config.OrchestratorEnvSingleton
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
import java.util.UUID

@Singleton
class OrchestratorPodFactory(
  private val featureFlagClient: FeatureFlagClient,
  private val orchestratorEnvSingleton: OrchestratorEnvSingleton,
  @Value("\${airbyte.worker.job.kube.serviceAccount}") private val serviceAccount: String?,
  @Named("orchestratorContainerPorts") private val containerPorts: List<ContainerPort>,
  private val volumeFactory: VolumeFactory,
  private val initContainerFactory: InitContainerFactory,
) {
  fun create(
    connectionId: UUID,
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

    val initContainer = initContainerFactory.create(containerResources, volumeMounts)

    val extraKubeEnv = additionalEnvVars.map { (k, v) -> EnvVar(k, v, null) }

    val mainContainer =
      ContainerBuilder()
        .withName(KubePodProcess.MAIN_CONTAINER_NAME)
        .withImage(kubePodInfo.mainContainerInfo.image)
        .withImagePullPolicy(kubePodInfo.mainContainerInfo.pullPolicy)
        .withResources(containerResources)
        .withEnv(orchestratorEnvSingleton.orchestratorEnvVars(connectionId) + extraKubeEnv)
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
