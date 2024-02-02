/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.commons.constants.WorkerConstants
import io.airbyte.commons.features.EnvVariableFeatureFlags
import io.airbyte.commons.features.FeatureFlags
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.Configs
import io.airbyte.config.EnvConfigs
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workload.launcher.config.cloud.CloudLoggingConfig
import io.airbyte.workload.launcher.config.cloud.CloudStateConfig
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.EnvVarSource
import io.fabric8.kubernetes.api.model.SecretKeySelector
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.context.env.Environment
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.function.Consumer

/**
 * Provides and configures the environment variables for the containers we launch.
 */
@Factory
class EnvVarConfigBeanFactory {
  /**
   * Map of env vars to be passed to the orchestrator container.
   */
  @Singleton
  @Named("orchestratorEnvMap")
  fun orchestratorEnvMap(
    featureFlags: FeatureFlags,
    @Value("\${airbyte.container.orchestrator.java-opts}") containerOrchestratorJavaOpts: String,
    workerEnv: Configs.WorkerEnvironment,
    cloudLoggingConfig: CloudLoggingConfig,
    cloudStateConfig: CloudStateConfig,
    @Named("workloadApiEnvMap") workloadApiEnvMap: Map<String, String>,
    @Named("metricsEnvMap") metricsEnvMap: Map<String, String>,
    @Named("micronautEnvMap") micronautEnvMap: Map<String, String>,
    @Named("apiClientEnvMap") apiClientEnvMap: Map<String, String>,
  ): Map<String, String> {
    // Build the map of additional environment variables to be passed to the container orchestrator
    val envMap: MutableMap<String, String> = HashMap()
    envMap[EnvVariableFeatureFlags.AUTO_DETECT_SCHEMA] = java.lang.Boolean.toString(featureFlags.autoDetectSchema())
    envMap[EnvVariableFeatureFlags.APPLY_FIELD_SELECTION] = java.lang.Boolean.toString(featureFlags.applyFieldSelection())
    envMap[EnvVariableFeatureFlags.FIELD_SELECTION_WORKSPACES] = featureFlags.fieldSelectionWorkspaces()
    envMap[JAVA_OPTS_ENV_VAR] = containerOrchestratorJavaOpts

    val configs: Configs = EnvConfigs()
    envMap[EnvConfigs.FEATURE_FLAG_CLIENT] = configs.featureFlagClient
    envMap[EnvConfigs.LAUNCHDARKLY_KEY] = configs.launchDarklyKey
    envMap[EnvConfigs.OTEL_COLLECTOR_ENDPOINT] = configs.otelCollectorEndpoint
    envMap[EnvConfigs.SOCAT_KUBE_CPU_LIMIT] = configs.socatSidecarKubeCpuLimit
    envMap[EnvConfigs.SOCAT_KUBE_CPU_REQUEST] = configs.socatSidecarKubeCpuRequest

    // Manually add the worker environment
    envMap[WorkerConstants.WORKER_ENVIRONMENT] = workerEnv.name

    // Cloud logging configuration
    envMap.putAll(cloudLoggingConfig.toEnvVarMap())

    // Cloud state configuration
    envMap.putAll(cloudStateConfig.toEnvVarMap())

    // Workload Api configuration
    envMap.putAll(workloadApiEnvMap)

    // Api client configuration
    envMap.putAll(apiClientEnvMap)

    // Metrics configuration
    envMap.putAll(metricsEnvMap)

    // Micronaut environment
    envMap.putAll(micronautEnvMap)

    // TODO: Don't do this. Be explicit about what env vars we pass.
    // Copy over all local values
    val localEnvMap =
      System.getenv()
        .filter { OrchestratorConstants.ENV_VARS_TO_TRANSFER.contains(it.key) }

    envMap.putAll(localEnvMap)

    return envMap
  }

  /**
   * The list of environment variables to be passed to the orchestrator.
   * The created list contains both regular environment variables and environment variables that
   * are sourced from Kubernetes secrets.
   */
  @Singleton
  @Named("orchestratorEnvVars")
  fun orchestratorEnvVars(
    @Named("orchestratorEnvMap") envMap: Map<String, String>,
    @Named("orchestratorSecretsEnvMap") secretsEnvMap: Map<String, EnvVarSource>,
  ): List<EnvVar> {
    val envVars =
      envMap
        .map { EnvVar(it.key, it.value, null) }
        .toList()

    val secretEnvVars =
      secretsEnvMap
        .map { EnvVar(it.key, null, it.value) }
        .toList()

    return envVars + secretEnvVars
  }

  /**
   * The list of env vars to be passed to the check sidecar container.
   */
  @Singleton
  @Named("checkSideCarEnvVars")
  fun checkSideCarEnvVars(
    cloudLoggingConfig: CloudLoggingConfig,
    cloudStateConfig: CloudStateConfig,
    @Named("workloadApiEnvMap") workloadApiEnvMap: Map<String, String>,
    @Named("apiClientEnvMap") apiClientEnvMap: Map<String, String>,
    @Named("micronautEnvMap") micronautEnvMap: Map<String, String>,
    @Named("orchestratorSecretsEnvMap") secretsEnvMap: Map<String, EnvVarSource>,
  ): List<EnvVar> {
    val envMap: MutableMap<String, String> = HashMap()
    // Cloud logging configuration
    envMap.putAll(cloudLoggingConfig.toEnvVarMap())

    // Cloud state configuration
    envMap.putAll(cloudStateConfig.toEnvVarMap())

    // Workload Api configuration
    envMap.putAll(workloadApiEnvMap)

    // Api client configuration
    envMap.putAll(apiClientEnvMap)

    // Micronaut environment
    envMap.putAll(micronautEnvMap)

    val envVars =
      envMap
        .map { EnvVar(it.key, it.value, null) }
        .toList()

    val secretEnvVars =
      secretsEnvMap
        .map { EnvVar(it.key, null, it.value) }
        .toList()

    return envVars + secretEnvVars
  }

  /**
   * The list of env vars to be passed to the connector container we are checking.
   */
  @Singleton
  @Named("checkEnvVars")
  fun checkEnvVars(
    @Named("checkWorkerConfigs") checkWorkerConfigs: WorkerConfigs,
  ): List<EnvVar> {
    return checkWorkerConfigs.envMap
      .map { EnvVar(it.key, it.value, null) }
      .toList()
  }

  /**
   * Creates a map that represents environment variables that will be used by the orchestrator that are sourced from kubernetes secrets.
   * The map key is the environment variable name and the map value contains the kubernetes secret name and key
   */
  @Singleton
  @Named("orchestratorSecretsEnvMap")
  fun orchestratorSecretsEnvMap(
    @Value("\${airbyte.workload-api.bearer-token-secret-name}") bearerTokenSecretName: String,
    @Value("\${airbyte.workload-api.bearer-token-secret-key}") bearerTokenSecretKey: String,
  ): Map<String, EnvVarSource> {
    return mapOf(WORKLOAD_API_BEARER_TOKEN_ENV_VAR to createEnvVarSource(bearerTokenSecretName, bearerTokenSecretKey))
  }

  private fun createEnvVarSource(
    secretName: String,
    secretKey: String,
  ): EnvVarSource {
    val secretKeySelector =
      SecretKeySelector().apply {
        name = secretName
        key = secretKey
      }

    val envVarSource =
      EnvVarSource().apply {
        secretKeyRef = secretKeySelector
      }

    return envVarSource
  }

  /**
   * Map of env vars for AirbyteApiClient
   */
  @Singleton
  @Named("apiClientEnvMap")
  fun apiClientEnvMap(
    @Value("\${airbyte.internal.api.host}") apiHost: String,
    @Value("\${airbyte.internal.api.auth-header.name}") apiAuthHeaderName: String,
    @Value("\${airbyte.internal.api.auth-header.value}") apiAuthHeaderValue: String,
    @Value("\${airbyte.control.plane.auth-endpoint}") controlPlaneAuthEndpoint: String,
    @Value("\${airbyte.data.plane.service-account.email}") dataPlaneServiceAccountEmail: String,
    @Value("\${airbyte.data.plane.service-account.credentials-path}") dataPlaneServiceAccountCredentialsPath: String,
    @Value("\${airbyte.acceptance.test.enabled}") isInTestMode: Boolean,
  ): Map<String, String> {
    val envMap: MutableMap<String, String> = HashMap()

    envMap[INTERNAL_API_HOST_ENV_VAR] = apiHost
    envMap[AIRBYTE_API_AUTH_HEADER_NAME_ENV_VAR] = apiAuthHeaderName
    envMap[AIRBYTE_API_AUTH_HEADER_VALUE_ENV_VAR] = apiAuthHeaderValue
    envMap[CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR] = controlPlaneAuthEndpoint
    envMap[DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR] = dataPlaneServiceAccountEmail
    envMap[DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR] = dataPlaneServiceAccountCredentialsPath
    envMap[ACCEPTANCE_TEST_ENABLED_VAR] = java.lang.Boolean.toString(isInTestMode)

    return envMap
  }

  /**
   * Map of env vars for specifying the Micronaut environment.
   */
  @Singleton
  @Named("micronautEnvMap")
  fun micronautEnvMap(): Map<String, String> {
    val envMap: MutableMap<String, String> = HashMap()

    if (System.getenv(Environment.ENVIRONMENTS_ENV) != null) {
      envMap[Environment.ENVIRONMENTS_ENV] = "$WORKER_V2_MICRONAUT_ENV,${System.getenv(Environment.ENVIRONMENTS_ENV)}"
    } else {
      envMap[Environment.ENVIRONMENTS_ENV] = WORKER_V2_MICRONAUT_ENV
    }

    return envMap
  }

  /**
   * Map of env vars for configuring datadog and metrics publishing.
   */
  @Singleton
  @Named("metricsEnvMap")
  fun metricsEnvMap(
    @Value("\${datadog.agent.host}") dataDogAgentHost: String,
    @Value("\${datadog.agent.port}") dataDogStatsdPort: String,
    @Value("\${airbyte.metric.client}") metricClient: String,
    @Value("\${airbyte.metric.should-publish}") shouldPublishMetrics: String,
    @Value("\${datadog.orchestrator.disabled.integrations}") disabledIntegrations: String,
  ): Map<String, String> {
    val envMap: MutableMap<String, String> = HashMap()
    envMap[METRIC_CLIENT_ENV_VAR] = metricClient
    envMap[DD_AGENT_HOST_ENV_VAR] = dataDogAgentHost
    envMap[DD_SERVICE_ENV_VAR] = "airbyte-container-orchestrator"
    envMap[DD_DOGSTATSD_PORT_ENV_VAR] = dataDogStatsdPort
    envMap[PUBLISH_METRICS_ENV_VAR] = shouldPublishMetrics
    if (System.getenv(DD_ENV_ENV_VAR) != null) {
      envMap[DD_ENV_ENV_VAR] = System.getenv(DD_ENV_ENV_VAR)
    }
    if (System.getenv(DD_VERSION_ENV_VAR) != null) {
      envMap[DD_VERSION_ENV_VAR] = System.getenv(DD_VERSION_ENV_VAR)
    }

    // Disable DD agent integrations based on the configuration
    if (StringUtils.isNotEmpty(disabledIntegrations)) {
      listOf(*disabledIntegrations.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
        .forEach(
          Consumer { e: String ->
            envMap[String.format(DD_INTEGRATION_ENV_VAR_FORMAT, e.trim { it <= ' ' })] = java.lang.Boolean.FALSE.toString()
          },
        )
    }

    return envMap
  }

  /**
   * Map of env vars for configuring the WorkloadApiClient (separate from ApiClient).
   */
  @Singleton
  @Named("workloadApiEnvMap")
  fun workloadApiEnvVars(
    @Value("\${airbyte.workload-api.connect-timeout-seconds}") workloadApiConnectTimeoutSeconds: String,
    @Value("\${airbyte.workload-api.read-timeout-seconds}") workloadApiReadTimeoutSeconds: String,
    @Value("\${airbyte.workload-api.retries.delay-seconds}") workloadApiRetriesDelaySeconds: String,
    @Value("\${airbyte.workload-api.retries.max}") workloadApiRetriesMax: String,
    @Value("\${airbyte.workload-api.base-path}") workloadApiBasePath: String,
  ): Map<String, String> {
    val envMap: MutableMap<String, String> = HashMap()
    envMap[WORKLOAD_API_HOST_ENV_VAR] = workloadApiBasePath
    envMap[WORKLOAD_API_CONNECT_TIMEOUT_SECONDS_ENV_VAR] = workloadApiConnectTimeoutSeconds
    envMap[WORKLOAD_API_READ_TIMEOUT_SECONDS_ENV_VAR] = workloadApiReadTimeoutSeconds
    envMap[WORKLOAD_API_RETRY_DELAY_SECONDS_ENV_VAR] = workloadApiRetriesDelaySeconds
    envMap[WORKLOAD_API_MAX_RETRIES_ENV_VAR] = workloadApiRetriesMax

    return envMap
  }

  companion object {
    private const val METRIC_CLIENT_ENV_VAR = "METRIC_CLIENT"
    private const val DD_AGENT_HOST_ENV_VAR = "DD_AGENT_HOST"
    private const val DD_DOGSTATSD_PORT_ENV_VAR = "DD_DOGSTATSD_PORT"
    private const val DD_ENV_ENV_VAR = "DD_ENV"
    private const val DD_SERVICE_ENV_VAR = "DD_SERVICE"
    private const val DD_VERSION_ENV_VAR = "DD_VERSION"
    private const val JAVA_OPTS_ENV_VAR = "JAVA_OPTS"
    private const val PUBLISH_METRICS_ENV_VAR = "PUBLISH_METRICS"
    private const val CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR = "CONTROL_PLANE_AUTH_ENDPOINT"
    private const val DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR = "DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH"
    private const val DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR = "DATA_PLANE_SERVICE_ACCOUNT_EMAIL"
    private const val AIRBYTE_API_AUTH_HEADER_NAME_ENV_VAR = "AIRBYTE_API_AUTH_HEADER_NAME"
    private const val AIRBYTE_API_AUTH_HEADER_VALUE_ENV_VAR = "AIRBYTE_API_AUTH_HEADER_VALUE"
    private const val INTERNAL_API_HOST_ENV_VAR = "INTERNAL_API_HOST"
    private const val ACCEPTANCE_TEST_ENABLED_VAR = "ACCEPTANCE_TEST_ENABLED"
    private const val DD_INTEGRATION_ENV_VAR_FORMAT = "DD_INTEGRATION_%s_ENABLED"
    private const val WORKER_V2_MICRONAUT_ENV = "worker-v2"
    private const val WORKLOAD_API_HOST_ENV_VAR = "WORKLOAD_API_HOST"
    private const val WORKLOAD_API_CONNECT_TIMEOUT_SECONDS_ENV_VAR = "WORKLOAD_API_CONNECT_TIMEOUT_SECONDS"
    private const val WORKLOAD_API_READ_TIMEOUT_SECONDS_ENV_VAR = "WORKLOAD_API_READ_TIMEOUT_SECONDS"
    private const val WORKLOAD_API_RETRY_DELAY_SECONDS_ENV_VAR = "WORKLOAD_API_RETRY_DELAY_SECONDS"
    private const val WORKLOAD_API_MAX_RETRIES_ENV_VAR = "WORKLOAD_API_MAX_RETRIES"

    // secrets
    private const val WORKLOAD_API_BEARER_TOKEN_ENV_VAR = "WORKLOAD_API_BEARER_TOKEN"
  }
}
