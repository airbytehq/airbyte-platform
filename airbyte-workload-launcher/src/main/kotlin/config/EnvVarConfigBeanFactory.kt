/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.commons.envvar.EnvVar.CLOUD_STORAGE_APPENDER_THREADS
import io.airbyte.commons.envvar.EnvVar.S3_PATH_STYLE_ACCESS
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.micronaut.runtime.AirbyteAnalyticsConfig
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteConnectorConfig
import io.airbyte.micronaut.runtime.AirbyteControlPlaneConfig
import io.airbyte.micronaut.runtime.AirbyteDataDogConfig
import io.airbyte.micronaut.runtime.AirbyteDataPlaneConfig
import io.airbyte.micronaut.runtime.AirbyteFeatureFlagConfig
import io.airbyte.micronaut.runtime.AirbyteInternalApiClientConfig
import io.airbyte.micronaut.runtime.AirbyteLoggingConfig
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig
import io.airbyte.micronaut.runtime.AirbyteWorkloadApiClientConfig
import io.airbyte.micronaut.runtime.SecretPersistenceType
import io.airbyte.micronaut.runtime.StorageEnvironmentVariableProvider
import io.airbyte.workers.pod.Metadata.AWS_ACCESS_KEY_ID
import io.airbyte.workers.pod.Metadata.AWS_SECRET_ACCESS_KEY
import io.airbyte.workload.launcher.constants.EnvVarConstants
import io.airbyte.workload.launcher.model.toEnvVarList
import io.airbyte.workload.launcher.model.toRefEnvVarList
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.EnvVarSource
import io.fabric8.kubernetes.api.model.SecretKeySelector
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.context.env.Environment
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.function.Consumer
import io.airbyte.commons.envvar.EnvVar as AirbyteEnvVar

/** Base config path for workload-api. */
private const val WORKLOAD_API_PREFIX = "airbyte.workload-api"

/**
 * Provides and configures the static environment variables for the containers we launch.
 * For dynamic configuration, see RuntimeEnvVarFactory.
 */
@Factory
class EnvVarConfigBeanFactory {
  /**
   * The list of env vars to be passed to the init container.
   */
  @Singleton
  @Named("initEnvVars")
  fun initEnvVars(
    @Named("apiClientEnvMap") apiClientEnvMap: Map<String, String>,
    @Named("featureFlagEnvMap") ffEnvVars: Map<String, String>,
    @Named("micronautEnvMap") micronautEnvMap: Map<String, String>,
    @Named("loggingEnvVars") loggingEnvMap: Map<String, String>,
    @Named("secretPersistenceSecretsEnvMap") secretPersistenceSecretsEnvMap: Map<String, EnvVarSource>,
    @Named("secretPersistenceEnvMap") secretPersistenceEnvMap: Map<String, String>,
    @Named("workloadApiEnvMap") workloadApiEnvMap: Map<String, String>,
    @Named("databaseEnvMap") dbEnvMap: Map<String, String>,
    @Named("awsAssumedRoleSecretEnv") awsAssumedRoleSecretEnv: Map<String, EnvVarSource>,
    @Named("metricsEnvMap") metricsEnvMap: Map<String, String>,
    @Named("trackingClientEnvMap") trackingClientEnvMap: Map<String, String>,
    @Named("airbyteMetadataEnvMap") airbyteMetadataEnvMap: Map<String, String>,
    @Named("dataplaneCredentialsSecretsEnvMap") dataplaneCredentialsSecretsEnvMap: Map<String, EnvVarSource>,
  ): List<EnvVar> {
    val envMap: MutableMap<String, String> = HashMap()

    envMap.putAll(loggingEnvMap)

    // Workload Api configuration
    envMap.putAll(workloadApiEnvMap)

    // Api client configuration
    envMap.putAll(apiClientEnvMap)

    // FF client configuration
    envMap.putAll(ffEnvVars)

    // Micronaut environment (secretly necessary for configuring API client auth)
    envMap.putAll(micronautEnvMap)

    // Direct env vars for secret persistence
    envMap.putAll(secretPersistenceEnvMap)

    // Add db env vars for local deployments if applicable
    envMap.putAll(dbEnvMap)

    // Metrics configuration
    envMap.putAll(metricsEnvMap)
    envMap[EnvVarConstants.DD_SERVICE_ENV_VAR] = "airbyte-workload-init-container"

    // Tracking configuration
    envMap.putAll(trackingClientEnvMap)

    // Airbyte metadata
    envMap.putAll(airbyteMetadataEnvMap)

    val envVars = envMap.toEnvVarList()

    val secretEnvVars =
      (secretPersistenceSecretsEnvMap + awsAssumedRoleSecretEnv + dataplaneCredentialsSecretsEnvMap)
        .toRefEnvVarList()

    return envVars + secretEnvVars
  }

  /**
   * The list of env vars to be passed to the check sidecar container.
   */
  @Singleton
  @Named("sideCarEnvVars")
  fun sideCarEnvVars(
    storageEnvironmentVariableProvider: StorageEnvironmentVariableProvider,
    @Named("apiClientEnvMap") apiClientEnvMap: Map<String, String>,
    @Named("loggingEnvVars") loggingEnvMap: Map<String, String>,
    @Named("micronautEnvMap") micronautEnvMap: Map<String, String>,
    @Named("workloadApiEnvMap") workloadApiEnvMap: Map<String, String>,
    @Named("trackingClientEnvMap") trackingClientEnvMap: Map<String, String>,
    @Named("airbyteMetadataEnvMap") airbyteMetadataEnvMap: Map<String, String>,
    @Named("dataplaneCredentialsSecretsEnvMap") dataplaneCredentialsSecretsEnvMap: Map<String, EnvVarSource>,
  ): List<EnvVar> {
    val envMap: MutableMap<String, String> = HashMap()

    envMap.putAll(loggingEnvMap)

    // Cloud storage configuration
    envMap.putAll(storageEnvironmentVariableProvider.toEnvVarMap())

    // Workload Api configuration
    envMap.putAll(workloadApiEnvMap)

    // Api client configuration
    envMap.putAll(apiClientEnvMap)

    // Micronaut environment (secretly necessary for configuring API client auth)
    envMap.putAll(micronautEnvMap)

    // Tracking configuration
    envMap.putAll(trackingClientEnvMap)

    // Airbyte metadata
    envMap.putAll(airbyteMetadataEnvMap)

    val envVars = envMap.toEnvVarList()

    val secretEnvVars = dataplaneCredentialsSecretsEnvMap.toRefEnvVarList()

    return envVars + secretEnvVars
  }

  @Singleton
  @Named("featureFlagEnvMap")
  fun featureFlagEnvMap(airbyteFeatureFlagConfig: AirbyteFeatureFlagConfig): Map<String, String> {
    val envMap: MutableMap<String, String> = HashMap()
    envMap[AirbyteEnvVar.FEATURE_FLAG_CLIENT.toString()] = airbyteFeatureFlagConfig.client.name.lowercase()
    envMap[AirbyteEnvVar.FEATURE_FLAG_PATH.toString()] = airbyteFeatureFlagConfig.path.toString()
    envMap[AirbyteEnvVar.LAUNCHDARKLY_KEY.toString()] = airbyteFeatureFlagConfig.apiKey
    envMap[AirbyteEnvVar.FEATURE_FLAG_BASEURL.toString()] = airbyteFeatureFlagConfig.baseUrl

    return envMap
  }

  @Singleton
  @Named("loggingEnvVars")
  fun loggingEnvVars(airbyteLoggingConfig: AirbyteLoggingConfig): Map<String, String> =
    mapOf(
      CLOUD_STORAGE_APPENDER_THREADS.name to "1",
      // We specifically do not set the log level here anymore since this would prevent us from
      // overriding it later in RuntimeEnvVarFactory. We need to be able to set it there to ensure that
      // we are able to dynamically change the level based on a feature flag
      S3_PATH_STYLE_ACCESS.name to airbyteLoggingConfig.s3PathStyleAccess,
    )

  /**
   * The list of env vars to be passed to the connector container we are checking.
   */
  @Singleton
  @Named("checkEnvVars")
  fun checkEnvVars(
    @Named("airbyteMetadataEnvMap") metadataEnvMap: Map<String, String>,
  ): List<EnvVar> = metadataEnvMap.toEnvVarList()

  /**
   * The list of env vars to be passed to the connector container we are discovering.
   */
  @Singleton
  @Named("discoverEnvVars")
  fun discoverEnvVars(
    @Named("airbyteMetadataEnvMap") metadataEnvMap: Map<String, String>,
  ): List<EnvVar> = metadataEnvMap.toEnvVarList()

  /**
   * The list of env vars to be passed to the connector container we are specifying.
   */
  @Singleton
  @Named("specEnvVars")
  fun specEnvVars(
    @Named("airbyteMetadataEnvMap") metadataEnvMap: Map<String, String>,
  ): List<EnvVar> = metadataEnvMap.toEnvVarList()

  /**
   * The list of env vars to be passed to the connector container we are reading from (the source).
   */
  @Singleton
  @Named("readEnvVars")
  fun readEnvVars(
    @Named("airbyteMetadataEnvMap") metadataEnvMap: Map<String, String>,
    @Named("featureFlagEnvMap") ffEnvVars: Map<String, String>,
  ): List<EnvVar> = metadataEnvMap.toEnvVarList() + ffEnvVars.toEnvVarList()

  /**
   * The list of env vars to be passed to the connector container we are writing to (the destination).
   */
  @Singleton
  @Named("writeEnvVars")
  fun writeEnvVars(
    @Named("airbyteMetadataEnvMap") metadataEnvMap: Map<String, String>,
    @Named("featureFlagEnvMap") ffEnvVars: Map<String, String>,
  ): List<EnvVar> = metadataEnvMap.toEnvVarList() + ffEnvVars.toEnvVarList()

  /**
   * To be injected into the replication pod, for the connectors that use assumed role access.
   */
  @Singleton
  @Named("awsAssumedRoleSecretEnv")
  fun awsAssumedRoleSecretEnv(airbyteConnectorConfig: AirbyteConnectorConfig): Map<String, EnvVarSource> =
    buildMap {
      if (airbyteConnectorConfig.source.credentials.aws.assumedRole.secretName
          .isNotBlank()
      ) {
        put(
          EnvVarConstants.AWS_ASSUME_ROLE_ACCESS_KEY_ID_ENV_VAR,
          createEnvVarSource(
            airbyteConnectorConfig.source.credentials.aws.assumedRole.secretName,
            airbyteConnectorConfig.source.credentials.aws.assumedRole.accessKey,
          ),
        )
        put(
          EnvVarConstants.AWS_ASSUME_ROLE_SECRET_ACCESS_KEY_ENV_VAR,
          createEnvVarSource(
            airbyteConnectorConfig.source.credentials.aws.assumedRole.secretName,
            airbyteConnectorConfig.source.credentials.aws.assumedRole.secretKey,
          ),
        )
      }
    }

  /**
   * To be injected into AWS connector pods that use assumed role access.
   */
  @Singleton
  @Named("connectorAwsAssumedRoleSecretEnv")
  fun connectorAwsAssumedRoleSecretEnv(airbyteConnectorConfig: AirbyteConnectorConfig): List<EnvVar> =
    buildList {
      if (airbyteConnectorConfig.source.credentials.aws.assumedRole.secretName
          .isNotBlank()
      ) {
        add(
          EnvVar(
            AWS_ACCESS_KEY_ID,
            null,
            createEnvVarSource(
              airbyteConnectorConfig.source.credentials.aws.assumedRole.secretName,
              airbyteConnectorConfig.source.credentials.aws.assumedRole.accessKey,
            ),
          ),
        )
        add(
          EnvVar(
            AWS_SECRET_ACCESS_KEY,
            null,
            createEnvVarSource(
              airbyteConnectorConfig.source.credentials.aws.assumedRole.secretName,
              airbyteConnectorConfig.source.credentials.aws.assumedRole.secretKey,
            ),
          ),
        )
      }
    }

  /**
   * Creates a map that represents environment variables that will be used by the orchestrator that are sourced from kubernetes secrets.
   * The map key is the environment variable name and the map value contains the kubernetes secret name and key
   */
  @Singleton
  @Named("orchestratorSecretsEnvMap")
  fun orchestratorSecretsEnvMap(
    @Named("awsAssumedRoleSecretEnv") awsAssumedRoleSecretEnv: Map<String, EnvVarSource>,
  ): Map<String, EnvVarSource> = awsAssumedRoleSecretEnv

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

  @Singleton
  @Named("dataplaneCredentialsSecretsEnvMap")
  fun dataplaneCredentialsSecretsEnvMap(airbyteDataPlaneConfig: AirbyteDataPlaneConfig): Map<String, EnvVarSource> =
    buildMap {
      if (airbyteDataPlaneConfig.credentials.clientIdSecretName.isNotBlank()) {
        put(
          EnvVarConstants.DATAPLANE_CLIENT_ID,
          createEnvVarSource(airbyteDataPlaneConfig.credentials.clientIdSecretName, airbyteDataPlaneConfig.credentials.clientIdSecretKey),
        )
      }
      if (airbyteDataPlaneConfig.credentials.clientSecretSecretName.isNotBlank()) {
        put(
          EnvVarConstants.DATAPLANE_CLIENT_SECRET,
          createEnvVarSource(airbyteDataPlaneConfig.credentials.clientSecretSecretName, airbyteDataPlaneConfig.credentials.clientSecretSecretKey),
        )
      }
    }

  /**
   * Map of env vars for AirbyteApiClient
   */
  @Singleton
  @Named("apiClientEnvMap")
  fun apiClientEnvMap(
    /*
     * Reference the environment variable instead of the resolved property, so that
     * the entry in the orchestrator's application.yml is consistent with all other
     * services that use the Airbyte API client.
     */
    @Value("\${INTERNAL_API_HOST}") internalApiHost: String,
    airbyteConfig: AirbyteConfig,
    airbyteInternalApiConfig: AirbyteInternalApiClientConfig,
    airbyteControlPlaneConfig: AirbyteControlPlaneConfig,
    airbyteDataPlaneConfig: AirbyteDataPlaneConfig,
  ): Map<String, String> =
    buildMap {
      put(EnvVarConstants.AIRBYTE_URL, airbyteConfig.airbyteUrl)
      put(EnvVarConstants.INTERNAL_API_HOST_ENV_VAR, internalApiHost)
      put(EnvVarConstants.CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR, airbyteControlPlaneConfig.authEndpoint)
      put(EnvVarConstants.DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR, airbyteDataPlaneConfig.serviceAccount.email)
      put(EnvVarConstants.DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR, airbyteDataPlaneConfig.serviceAccount.credentialsPath)
      put(EnvVarConstants.ACCEPTANCE_TEST_ENABLED_VAR, airbyteConfig.acceptanceTestEnabled.toString())
      // Expected to be present in dataplane for fetching token from control plane
      put(EnvVarConstants.CONTROL_PLANE_TOKEN_ENDPOINT, airbyteInternalApiConfig.auth.tokenEndpoint)
    }

  /**
   * Map of env vars for specifying the Micronaut environment.
   * Indirectly necessary for configuring API client auth and the local test secrets db
   */
  @Singleton
  @Named("micronautEnvMap")
  fun micronautEnvMap(
    airbyteSecretsManagerConfig: AirbyteSecretsManagerConfig,
    @Value("\${micronaut.env.additional-envs}") additionalMicronautEnv: String,
    edition: AirbyteEdition,
  ): Map<String, String> {
    val envs = mutableListOf(EnvConstants.WORKER_V2)

    // inherit from the parent env
    if (additionalMicronautEnv.isNotBlank()) {
      envs.add(additionalMicronautEnv)
    }

    // add this conditionally to trigger datasource bean creation via application.yaml
    if (SecretPersistenceType.TESTING_CONFIG_DB_TABLE == airbyteSecretsManagerConfig.persistence) {
      envs.add(EnvConstants.LOCAL_SECRETS)
    }

    if (edition == AirbyteEdition.CLOUD) {
      envs.add(Environment.CLOUD)
    }

    val commaSeparatedEnvString = envs.joinToString(separator = ",")

    return mapOf(Environment.ENVIRONMENTS_ENV to commaSeparatedEnvString)
  }

  /**
   * Map of env vars for configuring datadog and metrics publishing.
   */
  @Singleton
  @Named("metricsEnvMap")
  fun metricsEnvMap(airbyteDataDogConfig: AirbyteDataDogConfig): Map<String, String> {
    val envMap: MutableMap<String, String> = HashMap()
    envMap[EnvVarConstants.DD_AGENT_HOST_ENV_VAR] = airbyteDataDogConfig.agent.host
    envMap[EnvVarConstants.DD_DOGSTATSD_PORT_ENV_VAR] = airbyteDataDogConfig.agent.port
    if (airbyteDataDogConfig.env.isNotBlank()) {
      envMap[EnvVarConstants.DD_ENV_ENV_VAR] = airbyteDataDogConfig.env
    }
    if (airbyteDataDogConfig.version.isNotBlank()) {
      envMap[EnvVarConstants.DD_VERSION_ENV_VAR] = airbyteDataDogConfig.version
    }
    // Copy all Micrometer environment variables
    envMap.putAll(
      System.getenv().filter {
        it.key.startsWith(EnvVarConstants.MICROMETER_ENV_VAR_PREFIX) ||
          it.key == EnvVarConstants.OTEL_COLLECTOR_ENDPOINT ||
          it.key.startsWith(EnvVarConstants.STATSD_ENV_VAR_PREFIX)
      },
    )

    // Disable DD agent integrations based on the configuration
    if (airbyteDataDogConfig.orchestratorDisabledIntegrations.isNotBlank()) {
      listOf(
        *airbyteDataDogConfig.orchestratorDisabledIntegrations
          .split(",".toRegex())
          .dropLastWhile { it.isEmpty() }
          .toTypedArray(),
      ).forEach(
        Consumer { e: String ->
          envMap[String.format(EnvVarConstants.DD_INTEGRATION_ENV_VAR_FORMAT, e.trim { it <= ' ' })] =
            java.lang.Boolean.FALSE
              .toString()
        },
      )
    }

    return envMap
  }

  /**
   * Environment variables for secret persistence configuration.
   * These values are non-sensitive and passed directly.
   */
  @Singleton
  @Named("secretPersistenceEnvMap")
  fun secretPersistenceEnvMap(airbyteSecretsManagerConfig: AirbyteSecretsManagerConfig): Map<String, String> =
    buildMap {
      put(EnvVarConstants.SECRET_PERSISTENCE, airbyteSecretsManagerConfig.persistence.name)
      put(EnvVarConstants.SECRET_STORE_GCP_PROJECT_ID, airbyteSecretsManagerConfig.store.gcp.projectId)
      if (!airbyteSecretsManagerConfig.store.gcp.region
          .isNullOrBlank()
      ) {
        put(
          EnvVarConstants.SECRET_STORE_GCP_REGION,
          airbyteSecretsManagerConfig.store.gcp.region!!,
        )
      }
      put(EnvVarConstants.AWS_SECRET_MANAGER_REGION, airbyteSecretsManagerConfig.store.aws.region)
      put(EnvVarConstants.AWS_KMS_KEY_ARN, airbyteSecretsManagerConfig.store.aws.kmsKeyArn)
      put(EnvVarConstants.AWS_SECRET_MANAGER_SECRET_TAGS, airbyteSecretsManagerConfig.store.aws.tags)
      put(EnvVarConstants.AZURE_KEY_VAULT_VAULT_URL, airbyteSecretsManagerConfig.store.azure.vaultUrl)
      put(EnvVarConstants.AZURE_KEY_VAULT_TENANT_ID, airbyteSecretsManagerConfig.store.azure.tenantId)
      put(EnvVarConstants.AZURE_KEY_VAULT_SECRET_TAGS, airbyteSecretsManagerConfig.store.azure.tags)
      put(EnvVarConstants.VAULT_ADDRESS, airbyteSecretsManagerConfig.store.vault.address)
      put(EnvVarConstants.VAULT_PREFIX, airbyteSecretsManagerConfig.store.vault.prefix)
    }

  /**
   * Environment variables for secret persistence configuration.
   * These values are themselves sourced from kubernetes secrets.
   * The map key is the environment variable name and the map value
   * contains the kubernetes secret name and key for lookup.
   */
  @Singleton
  @Named("secretPersistenceSecretsEnvMap")
  fun secretPersistenceSecretsEnvMap(airbyteSecretsManagerConfig: AirbyteSecretsManagerConfig): Map<String, EnvVarSource> =
    buildMap {
      // Note: If any of the secret ref names or keys are blank kube will fail to create the pod, so we have to manually exclude empties
      if (airbyteSecretsManagerConfig.store.gcp.credentialsRefName
          .isNotBlank() &&
        airbyteSecretsManagerConfig.store.gcp.credentialsRefKey
          .isNotBlank()
      ) {
        put(
          EnvVarConstants.SECRET_STORE_GCP_CREDENTIALS,
          createEnvVarSource(
            secretName = airbyteSecretsManagerConfig.store.gcp.credentialsRefName,
            secretKey = airbyteSecretsManagerConfig.store.gcp.credentialsRefKey,
          ),
        )
      }
      if (airbyteSecretsManagerConfig.store.aws.accessKeyRefName
          .isNotBlank() &&
        airbyteSecretsManagerConfig.store.aws.accessKeyRefKey
          .isNotBlank()
      ) {
        put(
          EnvVarConstants.AWS_SECRET_MANAGER_ACCESS_KEY_ID,
          createEnvVarSource(
            secretName = airbyteSecretsManagerConfig.store.aws.accessKeyRefName,
            secretKey = airbyteSecretsManagerConfig.store.aws.accessKeyRefKey,
          ),
        )
      }
      if (airbyteSecretsManagerConfig.store.aws.secretKeyRefName
          .isNotBlank() &&
        airbyteSecretsManagerConfig.store.aws.secretKeyRefKey
          .isNotBlank()
      ) {
        put(
          EnvVarConstants.AWS_SECRET_MANAGER_SECRET_ACCESS_KEY,
          createEnvVarSource(
            secretName = airbyteSecretsManagerConfig.store.aws.secretKeyRefName,
            secretKey = airbyteSecretsManagerConfig.store.aws.secretKeyRefKey,
          ),
        )
      }
      // Azure
      if (airbyteSecretsManagerConfig.store.azure.clientIdRefName
          .isNotBlank() &&
        airbyteSecretsManagerConfig.store.azure.clientIdRefKey
          .isNotBlank()
      ) {
        put(
          EnvVarConstants.AZURE_KEY_VAULT_CLIENT_ID,
          createEnvVarSource(
            secretName = airbyteSecretsManagerConfig.store.azure.clientIdRefName,
            secretKey = airbyteSecretsManagerConfig.store.azure.clientIdRefKey,
          ),
        )
      }
      if (airbyteSecretsManagerConfig.store.azure.clientSecretRefName
          .isNotBlank() &&
        airbyteSecretsManagerConfig.store.azure.clientSecretRefKey
          .isNotBlank()
      ) {
        put(
          EnvVarConstants.AZURE_KEY_VAULT_CLIENT_SECRET,
          createEnvVarSource(
            secretName = airbyteSecretsManagerConfig.store.azure.clientSecretRefName,
            secretKey = airbyteSecretsManagerConfig.store.azure.clientSecretRefKey,
          ),
        )
      }
      if (airbyteSecretsManagerConfig.store.vault.tokenRefName
          .isNotBlank() &&
        airbyteSecretsManagerConfig.store.vault.tokenRefKey
          .isNotBlank()
      ) {
        put(
          EnvVarConstants.VAULT_AUTH_TOKEN,
          createEnvVarSource(
            secretName = airbyteSecretsManagerConfig.store.vault.tokenRefName,
            secretKey = airbyteSecretsManagerConfig.store.vault.tokenRefKey,
          ),
        )
      }
    }

  /**
   * Map of env vars for configuring the WorkloadApiClient (separate from ApiClient).
   */
  @Singleton
  @Named("workloadApiEnvMap")
  fun workloadApiEnvVars(airbyteWorkloadApiClientConfig: AirbyteWorkloadApiClientConfig): Map<String, String> {
    val envMap: MutableMap<String, String> = HashMap()
    envMap[EnvVarConstants.WORKLOAD_API_HOST_ENV_VAR] = airbyteWorkloadApiClientConfig.basePath
    envMap[EnvVarConstants.WORKLOAD_API_CONNECT_TIMEOUT_SECONDS_ENV_VAR] = airbyteWorkloadApiClientConfig.connectTimeoutSeconds.toString()
    envMap[EnvVarConstants.WORKLOAD_API_READ_TIMEOUT_SECONDS_ENV_VAR] = airbyteWorkloadApiClientConfig.readTimeoutSeconds.toString()
    envMap[EnvVarConstants.WORKLOAD_API_RETRY_DELAY_SECONDS_ENV_VAR] = airbyteWorkloadApiClientConfig.retries.delaySeconds.toString()
    envMap[EnvVarConstants.WORKLOAD_API_MAX_RETRIES_ENV_VAR] = airbyteWorkloadApiClientConfig.retries.max.toString()

    return envMap
  }

  @Singleton
  @Named("databaseEnvMap")
  fun databaseEnvMap(
    airbyteSecretsManagerConfig: AirbyteSecretsManagerConfig,
    @Value("\${datasources.local-secrets.url:}") dbUrl: String,
    @Value("\${datasources.local-secrets.username:}") dbUsername: String,
    @Value("\${datasources.local-secrets.password:}") dbPassword: String,
  ): Map<String, String> {
    // Only pass through DB env vars if configured for local storage of secrets
    if (SecretPersistenceType.TESTING_CONFIG_DB_TABLE != airbyteSecretsManagerConfig.persistence) {
      return mapOf()
    }

    return mapOf(
      AirbyteEnvVar.DATABASE_URL.toString() to dbUrl,
      AirbyteEnvVar.DATABASE_USER.toString() to dbUsername,
      AirbyteEnvVar.DATABASE_PASSWORD.toString() to dbPassword,
    )
  }

  @Singleton
  @Named("airbyteMetadataEnvMap")
  fun airbyteMetadataEnvMap(
    version: AirbyteVersion,
    airbyteConfig: AirbyteConfig,
  ): Map<String, String> =
    mapOf(
      EnvVarConstants.AIRBYTE_EDITION_ENV_VAR to airbyteConfig.edition.name,
      EnvVarConstants.AIRBYTE_VERSION_ENV_VAR to version.serialize(),
    )

  @Singleton
  @Named("trackingClientEnvMap")
  fun trackingClientEnvMap(airbyteAnalyticsConfig: AirbyteAnalyticsConfig) =
    mapOf(
      EnvVarConstants.TRACKING_STRATEGY to airbyteAnalyticsConfig.strategy.name,
      EnvVarConstants.SEGMENT_WRITE_KEY to airbyteAnalyticsConfig.writeKey,
    )
}
