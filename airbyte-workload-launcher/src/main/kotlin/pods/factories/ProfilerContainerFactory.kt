/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods.factories

import io.airbyte.config.ResourceRequirements
import io.airbyte.workload.launcher.constants.ContainerConstants
import io.airbyte.workload.launcher.pods.KubeContainerInfo
import io.airbyte.workload.launcher.pods.ResourceConversionUtils
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.VolumeMount
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
class ProfilerContainerFactory(
  @Named("orchestratorEnvVars") private val orchestratorEnvVars: List<EnvVar>,
  @Named("profilerKubeContainerInfo") private val profilerContainerInfo: KubeContainerInfo,
  @Named("profilerReqs") private val profilerReqs: ResourceRequirements,
) {
  private val profilerEnvVars =
    setOf(
      "WORKLOAD_ID",
      "CONNECTION_ID",
      "JOB_ID",
      "ATTEMPT_ID",
      "STORAGE_TYPE",
      "STORAGE_BUCKET_LOG",
      "STORAGE_BUCKET_STATE",
      "STORAGE_BUCKET_WORKLOAD_OUTPUT",
      "STORAGE_BUCKET_ACTIVITY_PAYLOAD",
      "STORAGE_BUCKET_AUDIT_LOGGING",
      "STORAGE_BUCKET_WORKLOAD_OUTPUT",
      "AZURE_STORAGE_CONNECTION_STRING",
      "GOOGLE_APPLICATION_CREDENTIALS",
      "AWS_ACCESS_KEY_ID",
      "MINIO_ENDPOINT",
      "AWS_SECRET_ACCESS_KEY",
      "AWS_DEFAULT_REGION",
    )

  internal fun create(
    runtimeEnvVars: List<EnvVar>,
    profilerVolumeMounts: List<VolumeMount>,
  ): Container {
    val mainCommand = ContainerCommandFactory.profiler()

    return ContainerBuilder()
      .withName(ContainerConstants.PROFILER_CONTAINER_NAME)
      .withCommand("sh", "-c", mainCommand)
      .withImage(profilerContainerInfo.image)
      .withImagePullPolicy(profilerContainerInfo.pullPolicy)
      .withResources(ResourceConversionUtils.domainToApi(profilerReqs))
      .withVolumeMounts(profilerVolumeMounts)
      .withEnv(filterProfilerEnvVars(orchestratorEnvVars + runtimeEnvVars))
      .build()
  }

  private fun filterProfilerEnvVars(envVars: List<EnvVar>): List<EnvVar> = envVars.filter { env -> profilerEnvVars.contains(env.name) }
}
