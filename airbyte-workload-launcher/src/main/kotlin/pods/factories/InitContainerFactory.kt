/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods.factories

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.PlatformInitContainerImage
import io.airbyte.featureflag.Workspace
import io.airbyte.micronaut.runtime.AirbyteConnectorConfig
import io.airbyte.workload.launcher.constants.ContainerConstants
import io.airbyte.workload.launcher.context.WorkloadSecurityContextProvider
import io.airbyte.workload.launcher.pods.KubeContainerInfo
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.VolumeMount
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID

val logger = KotlinLogging.logger {}

@Singleton
class InitContainerFactory(
  private val workloadSecurityContextProvider: WorkloadSecurityContextProvider,
  @Named("initEnvVars") private val envVars: List<EnvVar>,
  @Named("initContainerInfo") private val initContainerInfo: KubeContainerInfo,
  private val featureFlagClient: FeatureFlagClient,
  private val airbyteConnectorConfig: AirbyteConnectorConfig,
) {
  internal fun create(
    resourceReqs: ResourceRequirements?,
    volumeMounts: List<VolumeMount>,
    runtimeEnvVars: List<EnvVar>,
    workspaceId: UUID,
  ): Container {
    val initContainerImageOverride = featureFlagClient.stringVariation(PlatformInitContainerImage, Workspace(workspaceId))
    val resolvedImage = initContainerImageOverride.ifEmpty { initContainerInfo.image }

    logger.info { "[initContainer] image: $resolvedImage resources: $resourceReqs" }

    return ContainerBuilder()
      .withName(ContainerConstants.INIT_CONTAINER_NAME)
      .withImage(resolvedImage)
      .withImagePullPolicy(initContainerInfo.pullPolicy)
      .withWorkingDir(airbyteConnectorConfig.configDir)
      .withResources(resourceReqs)
      .withVolumeMounts(volumeMounts)
      .withSecurityContext(workloadSecurityContextProvider.rootlessContainerSecurityContext())
      .withEnv(envVars + runtimeEnvVars)
      .build()
  }
}
