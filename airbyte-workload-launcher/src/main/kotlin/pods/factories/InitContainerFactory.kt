/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods.factories

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.PlatformInitContainerImage
import io.airbyte.featureflag.Workspace
import io.airbyte.workers.context.WorkloadSecurityContextProvider
import io.airbyte.workers.pod.ContainerConstants
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.pod.KubeContainerInfo
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.VolumeMount
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID

val logger = KotlinLogging.logger {}

@Singleton
class InitContainerFactory(
  private val workloadSecurityContextProvider: WorkloadSecurityContextProvider,
  @Named("initEnvVars") private val envVars: List<EnvVar>,
  @Value("\${airbyte.worker.job.kube.images.busybox}") private val imageName: String,
  @Named("initContainerInfo") private val initContainerInfo: KubeContainerInfo,
  private val featureFlagClient: FeatureFlagClient,
) {
  fun createWaiting(
    resourceReqs: ResourceRequirements,
    volumeMounts: List<VolumeMount>,
  ): Container =
    ContainerBuilder()
      .withName(ContainerConstants.INIT_CONTAINER_NAME)
      .withImage(imageName)
      .withWorkingDir(FileConstants.CONFIG_DIR)
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
            FileConstants.CONFIG_DIR,
            FileConstants.KUBE_CP_SUCCESS_MARKER_FILE,
          ),
        ),
      ).withResources(resourceReqs)
      .withVolumeMounts(volumeMounts)
      .withSecurityContext(workloadSecurityContextProvider.rootlessContainerSecurityContext())
      .build()

  fun createFetching(
    resourceReqs: ResourceRequirements?,
    volumeMounts: List<VolumeMount>,
    runtimeEnvVars: List<EnvVar>,
    workspaceId: UUID,
  ): Container {
    val initContainerImageOverride = featureFlagClient.stringVariation(PlatformInitContainerImage, Workspace(workspaceId))

    logger.info { "[initContainer] image: ${initContainerImageOverride.ifEmpty { initContainerInfo.image }}" }

    return ContainerBuilder()
      .withName(ContainerConstants.INIT_CONTAINER_NAME)
      .withImage(initContainerImageOverride.ifEmpty { initContainerInfo.image })
      .withImagePullPolicy(initContainerInfo.pullPolicy)
      .withWorkingDir(FileConstants.CONFIG_DIR)
      .withResources(resourceReqs)
      .withVolumeMounts(volumeMounts)
      .withSecurityContext(workloadSecurityContextProvider.rootlessContainerSecurityContext())
      .withEnv(envVars + runtimeEnvVars)
      .build()
  }
}
