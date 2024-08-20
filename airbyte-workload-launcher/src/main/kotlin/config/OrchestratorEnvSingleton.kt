package io.airbyte.workload.launcher.config

import io.airbyte.commons.constants.WorkerConstants
import io.airbyte.commons.logging.StorageConfig
import io.airbyte.config.Configs
import io.airbyte.config.EnvConfigs
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ContainerOrchestratorJavaOpts
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workers.sync.OrchestratorConstants
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.EnvVarSource
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.commons.envvar.EnvVar as AbEnvVar

@Singleton
class OrchestratorEnvSingleton(
  private val featureFlagClient: FeatureFlagClient,
  private val workerEnv: Configs.WorkerEnvironment,
  private val storageConfig: StorageConfig,
  @Named("workloadApiEnvMap") private val workloadApiEnvMap: Map<String, String>,
  @Named("metricsEnvMap") private val metricsEnvMap: Map<String, String>,
  @Named("micronautEnvMap") private val micronautEnvMap: Map<String, String>,
  @Named("apiClientEnvMap") private val apiClientEnvMap: Map<String, String>,
  @Value("\${airbyte.container.orchestrator.java-opts}") private val containerOrchestratorJavaOpts: String,
  @Value("\${airbyte.connector.source.credentials.aws.assumed-role.secret-name}") private val awsAssumedRoleSecretName: String,
  @Named("orchestratorSecretsEnvMap") private val secretsEnvMap: Map<String, EnvVarSource>,
) {
  /**
   * Map of env vars to be passed to the orchestrator container.
   */
  fun orchestratorEnvMap(connectionId: UUID): Map<String, String> {
    // Build the map of additional environment variables to be passed to the container orchestrator
    val envMap: MutableMap<String, String> = HashMap()
    overrideOrchestratorJavaOpts(envMap, connectionId)
    val configs: Configs = EnvConfigs()
    envMap[AbEnvVar.FEATURE_FLAG_CLIENT.name] = AbEnvVar.FEATURE_FLAG_CLIENT.fetch() ?: ""
    envMap[AbEnvVar.LAUNCHDARKLY_KEY.name] = AbEnvVar.LAUNCHDARKLY_KEY.fetch() ?: ""
    envMap[AbEnvVar.OTEL_COLLECTOR_ENDPOINT.name] = AbEnvVar.OTEL_COLLECTOR_ENDPOINT.fetch() ?: ""
    envMap[AbEnvVar.SOCAT_KUBE_CPU_LIMIT.name] = configs.socatSidecarKubeCpuLimit
    envMap[AbEnvVar.SOCAT_KUBE_CPU_REQUEST.name] = configs.socatSidecarKubeCpuRequest

    // secret name used by orchestrator for assumed role look-ups
    envMap[AbEnvVar.AWS_ASSUME_ROLE_SECRET_NAME.name] = awsAssumedRoleSecretName

    // Manually add the worker environment
    envMap[WorkerConstants.WORKER_ENVIRONMENT] = workerEnv.name

    // Cloud storage config
    envMap.putAll(storageConfig.toEnvVarMap())

    // Workload Api configuration
    envMap.putAll(workloadApiEnvMap)

    // Api client configuration
    envMap.putAll(apiClientEnvMap)

    // Metrics configuration
    envMap.putAll(metricsEnvMap)

    // TODO: Don't do this. Be explicit about what env vars we pass.
    // Copy over all local values
    val localEnvMap =
      System.getenv()
        .filter { OrchestratorConstants.ENV_VARS_TO_TRANSFER.contains(it.key) }
    envMap.putAll(localEnvMap)

    // Micronaut environment -- this needs to be last to ensure that it is included.
    envMap.putAll(micronautEnvMap)

    return envMap
  }

  private fun overrideOrchestratorJavaOpts(
    envMap: MutableMap<String, String>,
    connectionId: UUID,
  ) {
    val injectedJavaOpts: String = featureFlagClient.stringVariation(ContainerOrchestratorJavaOpts, Connection(connectionId))
    if (injectedJavaOpts.isNotEmpty()) {
      envMap[EnvVarConfigBeanFactory.JAVA_OPTS_ENV_VAR] = injectedJavaOpts.trim()
    } else {
      envMap[EnvVarConfigBeanFactory.JAVA_OPTS_ENV_VAR] = containerOrchestratorJavaOpts
    }
  }

  /**
   * The list of environment variables to be passed to the orchestrator.
   * The created list contains both regular environment variables and environment variables that
   * are sourced from Kubernetes secrets.
   */
  fun orchestratorEnvVars(connectionId: UUID): List<EnvVar> {
    val secretEnvVars =
      secretEnvMap()
        .map { EnvVar(it.key, null, it.value) }
        .toList()

    val envVars =
      orchestratorEnvMap(connectionId)
        .filterNot { env ->
          secretEnvMap().containsKey(env.key)
        }
        .map { EnvVar(it.key, it.value, null) }
        .toList()

    return envVars + secretEnvVars
  }

  fun secretEnvMap(): Map<String, EnvVarSource> {
    return secretsEnvMap
  }
}
