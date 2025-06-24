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
  companion object {
    private const val DEFAULT_PROFILING_MODE = "cpu"
    private const val PROFILING_MODE_ENV_VAR = "PROFILING_MODE"
    private const val SHELL_COMMAND = "sh"
    private const val SHELL_FLAG = "-c"

    private val VALID_PROFILING_MODES =
      setOf(
        "cpu",
        "wall",
        "lock",
      )

    private val PROFILER_ENV_VARS =
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
  }

  internal fun create(
    runtimeEnvVars: List<EnvVar>,
    profilerVolumeMounts: List<VolumeMount>,
    profilingMode: String = DEFAULT_PROFILING_MODE,
  ): Container {
    val validatedProfilingMode = validateProfilingMode(profilingMode)
    val filteredEnvVars = filterProfilerEnvVars(orchestratorEnvVars + runtimeEnvVars)
    val allEnvVars = filteredEnvVars + createProfilingModeEnvVar(validatedProfilingMode)

    return ContainerBuilder()
      .withName(ContainerConstants.PROFILER_CONTAINER_NAME)
      .withCommand(SHELL_COMMAND, SHELL_FLAG, ContainerCommandFactory.profiler())
      .withImage(profilerContainerInfo.image)
      .withImagePullPolicy(profilerContainerInfo.pullPolicy)
      .withResources(ResourceConversionUtils.domainToApi(profilerReqs))
      .withVolumeMounts(profilerVolumeMounts)
      .withEnv(allEnvVars)
      .build()
  }

  private fun validateProfilingMode(profilingMode: String): String =
    if (profilingMode in VALID_PROFILING_MODES) profilingMode else DEFAULT_PROFILING_MODE

  private fun filterProfilerEnvVars(envVars: List<EnvVar>): List<EnvVar> = envVars.filter { it.name in PROFILER_ENV_VARS }

  private fun createProfilingModeEnvVar(mode: String): EnvVar = EnvVar(PROFILING_MODE_ENV_VAR, mode, null)
}
