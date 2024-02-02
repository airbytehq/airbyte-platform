/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.commons.constants.WorkerConstants
import io.airbyte.commons.features.EnvVariableFeatureFlags
import io.airbyte.commons.features.FeatureFlags
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.commons.workers.config.WorkerConfigsProvider
import io.airbyte.config.Configs
import io.airbyte.config.EnvConfigs
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.helpers.LogClientSingleton
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workload.launcher.config.cloud.CloudLoggingConfig
import io.airbyte.workload.launcher.config.cloud.CloudStateConfig
import io.fabric8.kubernetes.api.model.ContainerPort
import io.fabric8.kubernetes.api.model.ContainerPortBuilder
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.EnvVarSource
import io.fabric8.kubernetes.api.model.SecretKeySelector
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.context.env.Environment
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.function.Consumer

/**
 * Micronaut bean factory for container orchestrator configuration-related singletons.
 */
@Factory
class ContainerOrchestratorConfigBeanFactory {
  @Singleton
  fun kubeClient(customOkHttpClientFactory: CustomOkHttpClientFactory): KubernetesClient {
    return KubernetesClientBuilder()
      .withHttpClientFactory(customOkHttpClientFactory)
      .build()
  }

  @Singleton
  @Named("containerOrchestratorImage")
  fun containerOrchestratorImage(
    @Value("\${airbyte.version}") airbyteVersion: String,
    @Value("\${airbyte.container.orchestrator.image}") injectedImage: String?,
  ): String {
    if (injectedImage != null && StringUtils.isNotEmpty(injectedImage)) {
      return injectedImage
    }

    return "airbyte/container-orchestrator:$airbyteVersion"
  }

  @Singleton
  @Named("containerOrchestratorSidecarImage")
  fun containerOrchestratorSidecarImage(
    @Value("\${airbyte.version}") airbyteVersion: String,
    @Value("\${airbyte.container.orchestrator.sidecar.image}") injectedImage: String?,
  ): String {
    if (injectedImage != null && StringUtils.isNotEmpty(injectedImage)) {
      return injectedImage
    }

    if (airbyteVersion.endsWith("-cloud")) {
      return "airbyte/connector-sidecar:${airbyteVersion.dropLast(6)}"
    } else {
      return "airbyte/connector-sidecar:$airbyteVersion"
    }
  }

  @Singleton
  @Named("orchestratorKubeContainerInfo")
  fun orchestratorKubeContainerInfo(
    @Named("containerOrchestratorImage") containerOrchestratorImage: String,
    @Value("\${airbyte.worker.job.kube.main.container.image-pull-policy}") containerOrchestratorImagePullPolicy: String,
  ): KubeContainerInfo {
    return KubeContainerInfo(
      containerOrchestratorImage,
      containerOrchestratorImagePullPolicy,
    )
  }

  @Singleton
  @Named("orchestratorContainerPorts")
  fun orchestratorContainerPorts(
    @Value("\${micronaut.server.port}") serverPort: Int,
  ): List<ContainerPort> {
    return listOf(
      ContainerPortBuilder().withContainerPort(serverPort).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT1).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT2).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT3).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT4).build(),
    )
  }

  @Singleton
  @Named("checkContainerPorts")
  fun checkContainerPorts(
    @Value("\${micronaut.server.port}") serverPort: Int,
  ): List<ContainerPort> {
    return listOf(
      ContainerPortBuilder().withContainerPort(serverPort).build(),
    )
  }

  @Singleton
  @Named("sidecarKubeContainerInfo")
  fun sidecarKubeContainerInfo(
    @Named("containerOrchestratorSidecarImage") containerOrchestratorImage: String,
    @Value("\${airbyte.worker.job.kube.main.container.image-pull-policy}") containerOrchestratorImagePullPolicy: String,
  ): KubeContainerInfo {
    return KubeContainerInfo(containerOrchestratorImage, containerOrchestratorImagePullPolicy)
  }

  @Singleton
  @Named("replicationWorkerConfigs")
  fun replicationWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs {
    return workerConfigsProvider.getConfig(WorkerConfigsProvider.ResourceType.REPLICATION)
  }

  @Singleton
  @Named("checkWorkerConfigs")
  fun checkWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs {
    return workerConfigsProvider.getConfig(WorkerConfigsProvider.ResourceType.CHECK)
  }

  @Singleton
  @Named("checkSidecarWorkerConfigs")
  fun checkSidecarWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs {
    return workerConfigsProvider.getConfig(WorkerConfigsProvider.ResourceType.ORCHESTRATOR)
  }

  @Singleton
  @Named("orchestratorEnvMap")
  fun orchestratorEnvMap(
    featureFlags: FeatureFlags,
    @Value("\${datadog.agent.host}") dataDogAgentHost: String,
    @Value("\${datadog.agent.port}") dataDogStatsdPort: String,
    @Value("\${airbyte.metric.client}") metricClient: String,
    @Value("\${airbyte.metric.should-publish}") shouldPublishMetrics: String,
    @Value("\${airbyte.container.orchestrator.java-opts}") containerOrchestratorJavaOpts: String,
    @Value("\${airbyte.internal.api.host}") containerOrchestratorApiHost: String,
    @Value("\${airbyte.internal.api.auth-header.name}") containerOrchestratorApiAuthHeaderName: String,
    @Value("\${airbyte.internal.api.auth-header.value}") containerOrchestratorApiAuthHeaderValue: String,
    @Value("\${airbyte.control.plane.auth-endpoint}") controlPlaneAuthEndpoint: String,
    @Value("\${airbyte.data.plane.service-account.email}") dataPlaneServiceAccountEmail: String,
    @Value("\${airbyte.data.plane.service-account.credentials-path}") dataPlaneServiceAccountCredentialsPath: String,
    @Value("\${airbyte.acceptance.test.enabled}") isInTestMode: Boolean,
    @Value("\${datadog.orchestrator.disabled.integrations}") disabledIntegrations: String,
    @Value("\${google.application.credentials}") googleApplicationCredentials: String,
    @Value("\${airbyte.workload-api.base-path}") workloadApiBasePath: String,
    @Value("\${airbyte.workload-api.connect-timeout-seconds}") workloadApiConnectTimeoutSeconds: String,
    @Value("\${airbyte.workload-api.read-timeout-seconds}") workloadApiReadTimeoutSeconds: String,
    @Value("\${airbyte.workload-api.retries.delay-seconds}") workloadApiRetriesDelaySeconds: String,
    @Value("\${airbyte.workload-api.retries.max}") workloadApiRetriesMax: String,
    @Value("\${airbyte.cloud.storage.logs.type}") logsStorageType: String,
    @Value("\${airbyte.cloud.storage.state.type}") stateStorageType: String,
    cloudLoggingConfig: CloudLoggingConfig,
    cloudStateConfig: CloudStateConfig,
    workerEnv: Configs.WorkerEnvironment,
  ): Map<String, String> {
    // Build the map of additional environment variables to be passed to the container orchestrator
    val envMap: MutableMap<String, String> = HashMap()
    envMap[METRIC_CLIENT_ENV_VAR] = metricClient
    envMap[DD_AGENT_HOST_ENV_VAR] = dataDogAgentHost
    envMap[DD_SERVICE_ENV_VAR] = "airbyte-container-orchestrator"
    envMap[DD_DOGSTATSD_PORT_ENV_VAR] = dataDogStatsdPort
    envMap[PUBLISH_METRICS_ENV_VAR] = shouldPublishMetrics
    envMap[EnvVariableFeatureFlags.AUTO_DETECT_SCHEMA] = java.lang.Boolean.toString(featureFlags.autoDetectSchema())
    envMap[EnvVariableFeatureFlags.APPLY_FIELD_SELECTION] = java.lang.Boolean.toString(featureFlags.applyFieldSelection())
    envMap[EnvVariableFeatureFlags.FIELD_SELECTION_WORKSPACES] = featureFlags.fieldSelectionWorkspaces()
    envMap[JAVA_OPTS_ENV_VAR] = containerOrchestratorJavaOpts
    envMap[CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR] = controlPlaneAuthEndpoint
    envMap[DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR] = dataPlaneServiceAccountCredentialsPath
    envMap[DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR] = dataPlaneServiceAccountEmail

    // Disable DD agent integrations based on the configuration
    if (StringUtils.isNotEmpty(disabledIntegrations)) {
      listOf(*disabledIntegrations.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
        .forEach(
          Consumer { e: String ->
            envMap[String.format(DD_INTEGRATION_ENV_VAR_FORMAT, e.trim { it <= ' ' })] = java.lang.Boolean.FALSE.toString()
          },
        )
    }
    val configs: Configs = EnvConfigs()
    envMap[EnvConfigs.FEATURE_FLAG_CLIENT] = configs.featureFlagClient
    envMap[EnvConfigs.LAUNCHDARKLY_KEY] = configs.launchDarklyKey
    envMap[EnvConfigs.OTEL_COLLECTOR_ENDPOINT] = configs.otelCollectorEndpoint
    envMap[EnvConfigs.SOCAT_KUBE_CPU_LIMIT] = configs.socatSidecarKubeCpuLimit
    envMap[EnvConfigs.SOCAT_KUBE_CPU_REQUEST] = configs.socatSidecarKubeCpuRequest
    if (System.getenv(DD_ENV_ENV_VAR) != null) {
      envMap[DD_ENV_ENV_VAR] = System.getenv(DD_ENV_ENV_VAR)
    }
    if (System.getenv(DD_VERSION_ENV_VAR) != null) {
      envMap[DD_VERSION_ENV_VAR] = System.getenv(DD_VERSION_ENV_VAR)
    }

    // Environment variables for Workload API
    envMap[WORKLOAD_API_HOST_ENV_VAR] = workloadApiBasePath
    envMap[WORKLOAD_API_CONNECT_TIMEOUT_SECONDS_ENV_VAR] = workloadApiConnectTimeoutSeconds
    envMap[WORKLOAD_API_READ_TIMEOUT_SECONDS_ENV_VAR] = workloadApiReadTimeoutSeconds
    envMap[WORKLOAD_API_RETRY_DELAY_SECONDS_ENV_VAR] = workloadApiRetriesDelaySeconds
    envMap[WORKLOAD_API_MAX_RETRIES_ENV_VAR] = workloadApiRetriesMax

    // Environment variables for ApiClientBeanFactory
    envMap[CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR] = controlPlaneAuthEndpoint
    envMap[DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR] = dataPlaneServiceAccountCredentialsPath
    envMap[DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR] = dataPlaneServiceAccountEmail
    envMap[AIRBYTE_API_AUTH_HEADER_NAME_ENV_VAR] = containerOrchestratorApiAuthHeaderName
    envMap[AIRBYTE_API_AUTH_HEADER_VALUE_ENV_VAR] = containerOrchestratorApiAuthHeaderValue
    envMap[INTERNAL_API_HOST_ENV_VAR] = containerOrchestratorApiHost
    envMap[Environment.ENVIRONMENTS_ENV] = WORKER_V2_MICRONAUT_ENV
    if (System.getenv(Environment.ENVIRONMENTS_ENV) != null) {
      envMap[Environment.ENVIRONMENTS_ENV] = "${envMap[Environment.ENVIRONMENTS_ENV]},${System.getenv(Environment.ENVIRONMENTS_ENV)}"
    }
    envMap[ACCEPTANCE_TEST_ENABLED_VAR] = java.lang.Boolean.toString(isInTestMode)

    // Environment variables for object storage
    envMap[WORKER_LOGS_STORAGE_TYPE_VAR] = logsStorageType
    envMap[WORKER_STATE_STORAGE_TYPE_VAR] = stateStorageType

    // Manually add the worker environment
    envMap[WorkerConstants.WORKER_ENVIRONMENT] = workerEnv.name
    envMap[LogClientSingleton.GOOGLE_APPLICATION_CREDENTIALS] = googleApplicationCredentials

    // Cloud logging configuration
    envMap.putAll(cloudLoggingConfig.toEnvVarMap())

    // Cloud state configuration
    envMap.putAll(cloudStateConfig.toEnvVarMap())

    // copy over all local values
    val localEnvMap =
      System.getenv()
        .filter { OrchestratorConstants.ENV_VARS_TO_TRANSFER.contains(it.key) }

    envMap.putAll(localEnvMap)

    return envMap
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
   * Creates a list of environment variables to be passed to the orchestrator.
   * The created list contains both regular environment variables and environment variables that
   * are sourced from Kubernetes secrets.
   */
  @Singleton
  @Named("orchestratorEnvVars")
  fun orchestratorEnvVars(
    @Named("orchestratorEnvMap") envMap: Map<String, String>,
    @Named("orchestratorSecretsEnvMap") secretsEnvMap: Map<String, EnvVarSource> = emptyMap(),
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

  @Singleton
  @Named("checkEnvVars")
  fun checkEnvVars(
    @Named("checkWorkerConfigs") checkWorkerConfigs: WorkerConfigs,
  ): List<EnvVar> {
    return checkWorkerConfigs.envMap
      .map { EnvVar(it.key, it.value, null) }
      .toList()
  }

  @Singleton
  @Named("checkSideCarEnvVars")
  fun checkSideCarEnvVars(
    @Named("orchestratorEnvVars") envList: List<EnvVar>,
  ): List<EnvVar> {
    return envList
      .filter { it.name != JAVA_OPTS_ENV_VAR }
      .toList()
  }

  @Singleton
  @Named("checkOrchestratorReqs")
  fun check(): ResourceRequirements {
    // TODO: Make these env-driven
    return ResourceRequirements()
      .withMemoryRequest("500Mi")
      .withMemoryLimit("500Mi")
      .withCpuRequest("2")
      .withCpuLimit("2")
  }

  @Singleton
  @Named("sidecarReqs")
  fun sidecar(): ResourceRequirements {
    // TODO: Make these env-driven
    return ResourceRequirements()
      .withMemoryRequest("500Mi")
      .withMemoryLimit("500Mi")
      .withCpuRequest("2")
      .withCpuLimit("2")
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
    private const val WORKER_LOGS_STORAGE_TYPE_VAR = "WORKER_LOGS_STORAGE_TYPE"
    private const val WORKER_STATE_STORAGE_TYPE_VAR = "WORKER_STATE_STORAGE_TYPE"
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
