/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.commons.storage.StorageConfig
import io.airbyte.workload.launcher.constants.EnvVarConstants
import io.airbyte.workload.launcher.constants.OrchestratorConstants
import io.airbyte.workload.launcher.model.toEnvVarList
import io.airbyte.workload.launcher.model.toRefEnvVarList
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.EnvVarSource
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import io.airbyte.commons.envvar.EnvVar as AbEnvVar

@Factory
class OrchestratorEnvVarFactory(
  private val storageConfig: StorageConfig,
  @Named("workloadApiEnvMap") private val workloadApiEnvMap: Map<String, String>,
  @Named("metricsEnvMap") private val metricsEnvMap: Map<String, String>,
  @Named("micronautEnvMap") private val micronautEnvMap: Map<String, String>,
  @Named("apiClientEnvMap") private val apiClientEnvMap: Map<String, String>,
  @Value("\${airbyte.connector.source.credentials.aws.assumed-role.secret-name}") private val awsAssumedRoleSecretName: String,
  @Named("orchestratorSecretsEnvMap") private val secretsEnvMap: Map<String, EnvVarSource>,
  @Named("airbyteMetadataEnvMap") private val airbyteMetadataEnvMap: Map<String, String>,
  @Named("featureFlagEnvMap") private val ffEnvVars: Map<String, String>,
  @Named("dataplaneCredentialsSecretsEnvMap") private val dataplaneCredentialsSecretsEnvMap: Map<String, EnvVarSource>,
) {
  /**
   * The list of environment variables to be passed to the orchestrator.
   * The created list contains both regular environment variables and environment variables that
   * are sourced from Kubernetes secrets.
   */
  @Singleton
  @Named("orchestratorEnvVars")
  fun orchestratorEnvVars(): List<EnvVar> {
    // Build the map of additional environment variables to be passed to the container orchestrator
    val envMap: MutableMap<String, String> = HashMap()

    envMap[AbEnvVar.CLOUD_STORAGE_APPENDER_THREADS.name] = "1"
    envMap[EnvVarConstants.DD_SERVICE_ENV_VAR] = "airbyte-container-orchestrator"

    // secret name used by orchestrator for assumed role look-ups
    envMap[AbEnvVar.AWS_ASSUME_ROLE_SECRET_NAME.name] = awsAssumedRoleSecretName

    // Cloud storage config
    envMap.putAll(storageConfig.toEnvVarMap())

    // Workload Api configuration
    envMap.putAll(workloadApiEnvMap)

    // Api client configuration
    envMap.putAll(apiClientEnvMap)

    // FF client configuration
    envMap.putAll(ffEnvVars)

    // Metrics configuration
    envMap.putAll(metricsEnvMap)

    // Airbyte specific metadata
    envMap.putAll(airbyteMetadataEnvMap)

    // TODO: Don't do this. Be explicit about what env vars we pass.
    // Copy over all local values
    val localEnvMap =
      System
        .getenv()
        .filter { OrchestratorConstants.ENV_VARS_TO_TRANSFER.contains(it.key) }
    envMap.putAll(localEnvMap)

    // Micronaut environment -- this needs to be last to ensure that it is included.
    envMap.putAll(micronautEnvMap)

    val secretEnvVars =
      (secretsEnvMap + dataplaneCredentialsSecretsEnvMap).toRefEnvVarList()

    val envVars =
      envMap
        .filterNot { env ->
          secretsEnvMap.containsKey(env.key)
        }.toEnvVarList()

    return envVars + secretEnvVars
  }
}
