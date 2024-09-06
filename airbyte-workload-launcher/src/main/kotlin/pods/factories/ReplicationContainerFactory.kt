package io.airbyte.workload.launcher.pods.factories

import io.airbyte.workers.context.WorkloadSecurityContextProvider
import io.airbyte.workers.pod.ContainerConstants.DESTINATION_CONTAINER_NAME
import io.airbyte.workers.pod.ContainerConstants.ORCHESTRATOR_CONTAINER_NAME
import io.airbyte.workers.pod.ContainerConstants.SOURCE_CONTAINER_NAME
import io.airbyte.workers.pod.FileConstants.DEST_DIR
import io.airbyte.workers.pod.FileConstants.SOURCE_DIR
import io.airbyte.workload.launcher.config.OrchestratorEnvSingleton
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.VolumeMount
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class ReplicationContainerFactory(
  private val orchestratorEnvFactory: OrchestratorEnvSingleton,
  private val workloadSecurityContextProvider: WorkloadSecurityContextProvider,
  @Named("readEnvVars") private val sourceEnvVars: List<EnvVar>,
  @Named("writeEnvVars") private val destinationEnvVars: List<EnvVar>,
  @Value("\${airbyte.worker.job.kube.main.container.image-pull-policy}") private val imagePullPolicy: String,
) {
  fun createOrchestrator(
    resourceReqs: ResourceRequirements?,
    volumeMounts: List<VolumeMount>,
    runtimeEnvVars: List<EnvVar>,
    image: String,
    connectionId: UUID,
  ): Container {
    val mainCommand = ContainerCommandFactory.orchestrator()
    val envVars = orchestratorEnvFactory.orchestratorEnvVars(connectionId) + runtimeEnvVars

    return ContainerBuilder()
      .withName(ORCHESTRATOR_CONTAINER_NAME)
      .withImage(image)
      .withImagePullPolicy(imagePullPolicy)
      .withCommand("sh", "-c", mainCommand)
      .withResources(resourceReqs)
      .withEnv(envVars)
      .withVolumeMounts(volumeMounts)
      .withSecurityContext(workloadSecurityContextProvider.rootlessContainerSecurityContext())
      .build()
  }

  fun createSource(
    resourceReqs: ResourceRequirements?,
    volumeMounts: List<VolumeMount>,
    runtimeEnvVars: List<EnvVar>,
    image: String,
  ): Container {
    val mainCommand = ContainerCommandFactory.source()

    return ContainerBuilder()
      .withName(SOURCE_CONTAINER_NAME)
      .withImage(image)
      .withImagePullPolicy(imagePullPolicy)
      .withCommand("sh", "-c", mainCommand)
      .withEnv(sourceEnvVars + runtimeEnvVars)
      .withWorkingDir(SOURCE_DIR)
      .withVolumeMounts(volumeMounts)
      .withSecurityContext(workloadSecurityContextProvider.rootlessContainerSecurityContext())
      .withResources(resourceReqs)
      .build()
  }

  fun createDestination(
    resourceReqs: ResourceRequirements?,
    volumeMounts: List<VolumeMount>,
    runtimeEnvVars: List<EnvVar>,
    image: String,
  ): Container {
    val mainCommand = ContainerCommandFactory.destination()

    return ContainerBuilder()
      .withName(DESTINATION_CONTAINER_NAME)
      .withImage(image)
      .withImagePullPolicy(imagePullPolicy)
      .withCommand("sh", "-c", mainCommand)
      .withEnv(destinationEnvVars + runtimeEnvVars)
      .withWorkingDir(DEST_DIR)
      .withVolumeMounts(volumeMounts)
      .withSecurityContext(workloadSecurityContextProvider.rootlessContainerSecurityContext())
      .withResources(resourceReqs)
      .build()
  }
}
