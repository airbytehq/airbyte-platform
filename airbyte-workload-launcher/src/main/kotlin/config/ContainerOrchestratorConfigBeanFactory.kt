package io.airbyte.workload.launcher.config

import io.airbyte.commons.features.EnvVariableFeatureFlags
import io.airbyte.commons.features.FeatureFlags
import io.airbyte.config.Configs
import io.airbyte.config.EnvConfigs
import io.airbyte.config.storage.CloudStorageConfigs
import io.airbyte.workers.ContainerOrchestratorConfig
import io.airbyte.workers.storage.DocumentStoreClient
import io.airbyte.workers.storage.StateClients
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.context.env.Environment
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path
import java.util.Optional
import java.util.function.Consumer

/**
 * Micronaut bean factory for container orchestrator configuration-related singletons.
 */
@Factory
class ContainerOrchestratorConfigBeanFactory {
  @Singleton
  @Named("containerOrchestratorConfig")
  fun kubernetesContainerOrchestratorConfig(
    @Named("stateStorageConfigs") cloudStateStorageConfiguration: Optional<CloudStorageConfigs?>,
    @Value("\${airbyte.version}") airbyteVersion: String,
    @Value("\${airbyte.container.orchestrator.image}") containerOrchestratorImage: String?,
    @Value("\${airbyte.worker.job.kube.main.container.image-pull-policy}") containerOrchestratorImagePullPolicy: String?,
    @Value("\${airbyte.container.orchestrator.secret-mount-path}") containerOrchestratorSecretMountPath: String?,
    @Value("\${airbyte.container.orchestrator.secret-name}") containerOrchestratorSecretName: String?,
    @Value("\${google.application.credentials}") googleApplicationCredentials: String?,
    @Value("\${airbyte.worker.job.kube.namespace}") namespace: String?,
    @Value("\${airbyte.metric.client}") metricClient: String,
    @Value("\${datadog.agent.host}") dataDogAgentHost: String,
    @Value("\${datadog.agent.port}") dataDogStatsdPort: String,
    @Value("\${airbyte.metric.should-publish}") shouldPublishMetrics: String,
    featureFlags: FeatureFlags,
    @Value("\${airbyte.container.orchestrator.java-opts}") containerOrchestratorJavaOpts: String,
    workerEnvironment: Configs.WorkerEnvironment?,
    @Value("\${airbyte.internal.api.host}") containerOrchestratorApiHost: String,
    @Value("\${airbyte.internal.api.auth-header.name}") containerOrchestratorApiAuthHeaderName: String,
    @Value("\${airbyte.internal.api.auth-header.value}") containerOrchestratorApiAuthHeaderValue: String,
    @Value("\${airbyte.control.plane.auth-endpoint}") controlPlaneAuthEndpoint: String,
    @Value("\${airbyte.data.plane.service-account.email}") dataPlaneServiceAccountEmail: String,
    @Value("\${airbyte.data.plane.service-account.credentials-path}") dataPlaneServiceAccountCredentialsPath: String,
    @Value("\${airbyte.container.orchestrator.data-plane-creds.secret-mount-path}") containerOrchestratorDataPlaneCredsSecretMountPath: String?,
    @Value("\${airbyte.container.orchestrator.data-plane-creds.secret-name}") containerOrchestratorDataPlaneCredsSecretName: String?,
    @Value("\${airbyte.acceptance.test.enabled}") isInTestMode: Boolean,
    @Value("\${datadog.orchestrator.disabled.integrations}") disabledIntegrations: String,
    @Value("\${airbyte.worker.job.kube.serviceAccount}") serviceAccount: String?,
  ): ContainerOrchestratorConfig {
    val kubernetesClient = DefaultKubernetesClient()
    val documentStoreClient: DocumentStoreClient =
      StateClients.create(
        cloudStateStorageConfiguration.orElse(null),
        STATE_STORAGE_PREFIX,
      )

    // Build the map of additional environment variables to be passed to the container orchestrator
    val environmentVariables: MutableMap<String, String> = HashMap()
    environmentVariables[METRIC_CLIENT_ENV_VAR] = metricClient
    environmentVariables[DD_AGENT_HOST_ENV_VAR] = dataDogAgentHost
    environmentVariables[DD_SERVICE_ENV_VAR] = "airbyte-container-orchestrator"
    environmentVariables[DD_DOGSTATSD_PORT_ENV_VAR] = dataDogStatsdPort
    environmentVariables[PUBLISH_METRICS_ENV_VAR] = shouldPublishMetrics
    environmentVariables[EnvVariableFeatureFlags.AUTO_DETECT_SCHEMA] = java.lang.Boolean.toString(featureFlags.autoDetectSchema())
    environmentVariables[EnvVariableFeatureFlags.APPLY_FIELD_SELECTION] = java.lang.Boolean.toString(featureFlags.applyFieldSelection())
    environmentVariables[EnvVariableFeatureFlags.FIELD_SELECTION_WORKSPACES] = featureFlags.fieldSelectionWorkspaces()
    environmentVariables[JAVA_OPTS_ENV_VAR] = containerOrchestratorJavaOpts
    environmentVariables[CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR] = controlPlaneAuthEndpoint
    environmentVariables[DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR] = dataPlaneServiceAccountCredentialsPath
    environmentVariables[DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR] = dataPlaneServiceAccountEmail

    // Disable DD agent integrations based on the configuration
    if (StringUtils.isNotEmpty(disabledIntegrations)) {
      listOf(*disabledIntegrations.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
        .forEach(
          Consumer { e: String ->
            environmentVariables[String.format(DD_INTEGRATION_ENV_VAR_FORMAT, e.trim { it <= ' ' })] = java.lang.Boolean.FALSE.toString()
          },
        )
    }
    val configs: Configs = EnvConfigs()
    environmentVariables[EnvConfigs.FEATURE_FLAG_CLIENT] = configs.getFeatureFlagClient()
    environmentVariables[EnvConfigs.LAUNCHDARKLY_KEY] = configs.getLaunchDarklyKey()
    environmentVariables[EnvConfigs.OTEL_COLLECTOR_ENDPOINT] = configs.getOtelCollectorEndpoint()
    environmentVariables[EnvConfigs.SOCAT_KUBE_CPU_LIMIT] = configs.getSocatSidecarKubeCpuLimit()
    environmentVariables[EnvConfigs.SOCAT_KUBE_CPU_REQUEST] = configs.getSocatSidecarKubeCpuRequest()
    if (System.getenv(DD_ENV_ENV_VAR) != null) {
      environmentVariables[DD_ENV_ENV_VAR] = System.getenv(DD_ENV_ENV_VAR)
    }
    if (System.getenv(DD_VERSION_ENV_VAR) != null) {
      environmentVariables[DD_VERSION_ENV_VAR] = System.getenv(DD_VERSION_ENV_VAR)
    }

    // Environment variables for ApiClientBeanFactory
    environmentVariables[CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR] = controlPlaneAuthEndpoint
    environmentVariables[DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR] = dataPlaneServiceAccountCredentialsPath
    environmentVariables[DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR] = dataPlaneServiceAccountEmail
    environmentVariables[AIRBYTE_API_AUTH_HEADER_NAME_ENV_VAR] = containerOrchestratorApiAuthHeaderName
    environmentVariables[AIRBYTE_API_AUTH_HEADER_VALUE_ENV_VAR] = containerOrchestratorApiAuthHeaderValue
    environmentVariables[INTERNAL_API_HOST_ENV_VAR] = containerOrchestratorApiHost
    if (System.getenv(Environment.ENVIRONMENTS_ENV) != null) {
      environmentVariables[Environment.ENVIRONMENTS_ENV] = System.getenv(Environment.ENVIRONMENTS_ENV)
    }
    environmentVariables[ACCEPTANCE_TEST_ENABLED_VAR] = java.lang.Boolean.toString(isInTestMode)
    return ContainerOrchestratorConfig(
      namespace,
      documentStoreClient,
      environmentVariables,
      kubernetesClient,
      containerOrchestratorSecretName,
      containerOrchestratorSecretMountPath,
      containerOrchestratorDataPlaneCredsSecretName,
      containerOrchestratorDataPlaneCredsSecretMountPath,
      if (StringUtils.isNotEmpty(containerOrchestratorImage)) containerOrchestratorImage else "airbyte/container-orchestrator:$airbyteVersion",
      containerOrchestratorImagePullPolicy,
      googleApplicationCredentials,
      workerEnvironment,
      serviceAccount,
    )
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

    // IMPORTANT: Changing the storage location will orphan already existing kube pods when the new
    // version is deployed!
    private val STATE_STORAGE_PREFIX = Path.of("/state")
  }
}
