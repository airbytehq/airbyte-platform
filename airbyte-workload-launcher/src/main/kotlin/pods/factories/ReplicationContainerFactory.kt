package io.airbyte.workload.launcher.pods.factories

import io.airbyte.workers.pod.ContainerConstants.DESTINATION_CONTAINER_NAME
import io.airbyte.workers.pod.ContainerConstants.ORCHESTRATOR_CONTAINER_NAME
import io.airbyte.workers.pod.ContainerConstants.SOURCE_CONTAINER_NAME
import io.airbyte.workers.pod.FileConstants.CATALOG_FILE
import io.airbyte.workers.pod.FileConstants.CONNECTOR_CONFIG_FILE
import io.airbyte.workers.pod.FileConstants.DEST_DIR
import io.airbyte.workers.pod.FileConstants.INPUT_STATE_FILE
import io.airbyte.workers.pod.FileConstants.SOURCE_DIR
import io.airbyte.workload.launcher.config.OrchestratorEnvSingleton
import io.fabric8.kubernetes.api.model.CapabilitiesBuilder
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.SeccompProfileBuilder
import io.fabric8.kubernetes.api.model.SecurityContext
import io.fabric8.kubernetes.api.model.SecurityContextBuilder
import io.fabric8.kubernetes.api.model.VolumeMount
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.commons.envvar.EnvVar as AirbyteEnvVar

@Singleton
class ReplicationContainerFactory(
  private val orchestratorEnvFactory: OrchestratorEnvSingleton,
  @Value("\${airbyte.worker.job.kube.main.container.image-pull-policy}") private val imagePullPolicy: String,
) {
  fun createOrchestrator(
    resourceReqs: ResourceRequirements?,
    volumeMounts: List<VolumeMount>,
    runtimeEnvVars: List<EnvVar>,
    image: String,
    connectionId: UUID,
  ): Container {
    val envVars = orchestratorEnvFactory.orchestratorEnvVars(connectionId) + runtimeEnvVars

    return ContainerBuilder()
      .withName(ORCHESTRATOR_CONTAINER_NAME)
      .withImage(image)
      .withImagePullPolicy(imagePullPolicy)
      .withResources(resourceReqs)
      .withEnv(envVars)
      .withVolumeMounts(volumeMounts)
      .withSecurityContext(containerSecurityContext())
      .build()
  }

  fun createSource(
    resourceReqs: ResourceRequirements?,
    volumeMounts: List<VolumeMount>,
    runtimeEnvVars: List<EnvVar>,
    image: String,
  ): Container {
    val mainCommand =
      ContainerCommandFactory.replConnector(
        "read",
        "--config $SOURCE_DIR/${CONNECTOR_CONFIG_FILE} " +
          "--catalog $SOURCE_DIR/${CATALOG_FILE} " +
          "--state $SOURCE_DIR/${INPUT_STATE_FILE}",
        "/dev/null",
      )

    return ContainerBuilder()
      .withName(SOURCE_CONTAINER_NAME)
      .withImage(image)
      .withImagePullPolicy(imagePullPolicy)
      .withCommand("sh", "-c", mainCommand)
      .withEnv(runtimeEnvVars)
      .withWorkingDir(SOURCE_DIR)
      .withVolumeMounts(volumeMounts)
      .withSecurityContext(containerSecurityContext())
      .withResources(resourceReqs)
      .build()
  }

  fun createDestination(
    resourceReqs: ResourceRequirements?,
    volumeMounts: List<VolumeMount>,
    runtimeEnvVars: List<EnvVar>,
    image: String,
  ): Container {
    val mainCommand =
      ContainerCommandFactory.replConnector(
        "write",
        "--config $DEST_DIR/${CONNECTOR_CONFIG_FILE} " +
          "--catalog $DEST_DIR/${CATALOG_FILE} ",
      )

    return ContainerBuilder()
      .withName(DESTINATION_CONTAINER_NAME)
      .withImage(image)
      .withImagePullPolicy(imagePullPolicy)
      .withCommand("sh", "-c", mainCommand)
      .withEnv(runtimeEnvVars)
      .withWorkingDir(DEST_DIR)
      .withVolumeMounts(volumeMounts)
      .withSecurityContext(containerSecurityContext())
      .withResources(resourceReqs)
      .build()
  }
}

// TODO: make this a proper singleton
private fun containerSecurityContext(): SecurityContext? =
  when (AirbyteEnvVar.ROOTLESS_WORKLOAD.fetch(default = "false").toBoolean()) {
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
