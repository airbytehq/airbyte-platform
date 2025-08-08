/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.commons.envvar.EnvVar.CLOUD_STORAGE_APPENDER_THREADS
import io.airbyte.commons.envvar.EnvVar.S3_PATH_STYLE_ACCESS
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.commons.storage.StorageConfig
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs
import io.airbyte.config.Configs.AirbyteEdition
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
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.function.Consumer
import io.airbyte.commons.envvar.EnvVar as AirbyteEnvVar

/** Base config path for workload-api. */
private const val WORKLOAD_API_PREFIX = "airbyte.workload-api"

/**
 * Provides and configures the static environment variables for the containers we launch.
 * For dynamic configuration see RuntimeEnvVarFactory.
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
    storageConfig: StorageConfig,
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
    envMap.putAll(storageConfig.toEnvVarMap())

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
  fun featureFlagEnvMap(
    @Value("\${airbyte.feature-flag.client}") client: String,
    @Value("\${airbyte.feature-flag.path}") path: String,
    @Value("\${airbyte.feature-flag.api-key}") apiKey: String,
    @Value("\${airbyte.feature-flag.base-url}") baseUrl: String,
  ): Map<String, String> {
    val envMap: MutableMap<String, String> = HashMap()
    envMap[AirbyteEnvVar.FEATURE_FLAG_CLIENT.toString()] = client
    envMap[AirbyteEnvVar.FEATURE_FLAG_PATH.toString()] = path
    envMap[AirbyteEnvVar.LAUNCHDARKLY_KEY.toString()] = apiKey
    envMap[AirbyteEnvVar.FEATURE_FLAG_BASEURL.toString()] = baseUrl

    return envMap
  }

  @Singleton
  @Named("loggingEnvVars")
  fun loggingEnvVars(
    @Value("\${airbyte.logging.s3-path-style-access}") s3PathStyleAccess: String,
  ): Map<String, String> =
    mapOf(
      CLOUD_STORAGE_APPENDER_THREADS.name to "1",
      // We specifically do not set the log level here anymore since this would prevent us from
      // overriding it later in RuntimeEnvVarFactory. We need to be able to set it there to ensure that
      // we are able to dynamically change the level based on a feature flag
      S3_PATH_STYLE_ACCESS.name to s3PathStyleAccess,
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
  fun awsAssumedRoleSecretEnv(
    @Value("\${airbyte.connector.source.credentials.aws.assumed-role.access-key}") awsAssumedRoleAccessKey: String,
    @Value("\${airbyte.connector.source.credentials.aws.assumed-role.secret-key}") awsAssumedRoleSecretKey: String,
    @Value("\${airbyte.connector.source.credentials.aws.assumed-role.secret-name}") awsAssumedRoleSecretName: String,
  ): Map<String, EnvVarSource> =
    buildMap {
      if (awsAssumedRoleSecretName.isNotBlank()) {
        put(EnvVarConstants.AWS_ASSUME_ROLE_ACCESS_KEY_ID_ENV_VAR, createEnvVarSource(awsAssumedRoleSecretName, awsAssumedRoleAccessKey))
        put(EnvVarConstants.AWS_ASSUME_ROLE_SECRET_ACCESS_KEY_ENV_VAR, createEnvVarSource(awsAssumedRoleSecretName, awsAssumedRoleSecretKey))
      }
    }

  /**
   * To be injected into AWS connector pods that use assumed role access.
   */
  @Singleton
  @Named("connectorAwsAssumedRoleSecretEnv")
  fun connectorAwsAssumedRoleSecretEnv(
    @Value("\${airbyte.connector.source.credentials.aws.assumed-role.access-key}") accessKey: String,
    @Value("\${airbyte.connector.source.credentials.aws.assumed-role.secret-key}") secretKey: String,
    @Value("\${airbyte.connector.source.credentials.aws.assumed-role.secret-name}") secretName: String,
  ): List<EnvVar> =
    buildList {
      if (secretName.isNotBlank()) {
        add(EnvVar(AWS_ACCESS_KEY_ID, null, createEnvVarSource(secretName, accessKey)))
        add(EnvVar(AWS_SECRET_ACCESS_KEY, null, createEnvVarSource(secretName, secretKey)))
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
  fun dataplaneCredentialsSecretsEnvMap(
    @Value("\${airbyte.data-plane-credentials.client-id-secret-name}") clientIdSecretName: String,
    @Value("\${airbyte.data-plane-credentials.client-id-secret-key}") clientIdSecretKey: String,
    @Value("\${airbyte.data-plane-credentials.client-secret-secret-name}") clientSecretSecretName: String,
    @Value("\${airbyte.data-plane-credentials.client-secret-secret-key}") clientSecretSecretKey: String,
  ): Map<String, EnvVarSource> =
    buildMap {
      if (clientIdSecretName.isNotBlank()) {
        put(EnvVarConstants.DATAPLANE_CLIENT_ID, createEnvVarSource(clientIdSecretName, clientIdSecretKey))
      }
      if (clientSecretSecretName.isNotBlank()) {
        put(EnvVarConstants.DATAPLANE_CLIENT_SECRET, createEnvVarSource(clientSecretSecretName, clientSecretSecretKey))
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
    @Value("\${airbyte.control.plane.auth-endpoint}") controlPlaneAuthEndpoint: String,
    @Value("\${airbyte.data.plane.service-account.email}") dataPlaneServiceAccountEmail: String,
    @Value("\${airbyte.data.plane.service-account.credentials-path}") dataPlaneServiceAccountCredentialsPath: String,
    @Value("\${airbyte.acceptance.test.enabled}") isInTestMode: Boolean,
    @Value("\${airbyte.internal-api.auth.token-endpoint}") controlPlaneTokenEndpoint: String,
    @Value("\${airbyte.airbyte-url}") airbyteUrl: String,
  ): Map<String, String> =
    buildMap {
      put(EnvVarConstants.AIRBYTE_URL, airbyteUrl)
      put(EnvVarConstants.INTERNAL_API_HOST_ENV_VAR, internalApiHost)
      put(EnvVarConstants.CONTROL_PLANE_AUTH_ENDPOINT_ENV_VAR, controlPlaneAuthEndpoint)
      put(EnvVarConstants.DATA_PLANE_SERVICE_ACCOUNT_EMAIL_ENV_VAR, dataPlaneServiceAccountEmail)
      put(EnvVarConstants.DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH_ENV_VAR, dataPlaneServiceAccountCredentialsPath)
      put(EnvVarConstants.ACCEPTANCE_TEST_ENABLED_VAR, isInTestMode.toString())
      // Expected to be present in dataplane for fetching token from control plane
      put(EnvVarConstants.CONTROL_PLANE_TOKEN_ENDPOINT, controlPlaneTokenEndpoint)
    }

  /**
   * Map of env vars for specifying the Micronaut environment.
   * Indirectly necessary for configuring API client auth and the local test secrets db
   */
  @Singleton
  @Named("micronautEnvMap")
  fun micronautEnvMap(
    @Value("\${airbyte.secret.persistence}") secretPersistenceType: String,
    @Value("\${micronaut.env.additional-envs}") additionalMicronautEnv: String,
    edition: AirbyteEdition,
  ): Map<String, String> {
    val envs = mutableListOf(EnvConstants.WORKER_V2)

    // inherit from the parent env
    if (additionalMicronautEnv.isNotBlank()) {
      envs.add(additionalMicronautEnv)
    }

    // add this conditionally to trigger datasource bean creation via application.yaml
    if (Configs.SecretPersistenceType.TESTING_CONFIG_DB_TABLE
        .toString()
        .equals(secretPersistenceType, ignoreCase = true)
    ) {
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
  fun metricsEnvMap(
    @Value("\${datadog.agent.host}") dataDogAgentHost: String,
    @Value("\${datadog.agent.port}") dataDogStatsdPort: String,
    @Value("\${datadog.orchestrator.disabled.integrations}") disabledIntegrations: String,
    @Value("\${datadog.env}") ddEnv: String,
    @Value("\${datadog.version}") ddVersion: String,
  ): Map<String, String> {
    val envMap: MutableMap<String, String> = HashMap()
    envMap[EnvVarConstants.DD_AGENT_HOST_ENV_VAR] = dataDogAgentHost
    envMap[EnvVarConstants.DD_DOGSTATSD_PORT_ENV_VAR] = dataDogStatsdPort
    if (ddEnv.isNotBlank()) {
      envMap[EnvVarConstants.DD_ENV_ENV_VAR] = ddEnv
    }
    if (ddVersion.isNotBlank()) {
      envMap[EnvVarConstants.DD_VERSION_ENV_VAR] = ddVersion
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
    if (StringUtils.isNotEmpty(disabledIntegrations)) {
      listOf(*disabledIntegrations.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
        .forEach(
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
  fun secretPersistenceEnvMap(
    @Value("\${airbyte.secret.persistence}") persistenceType: String,
    @Value("\${airbyte.secret.store.gcp.project-id}") gcpProjectId: String,
    @Value("\${airbyte.secret.store.gcp.region:}") gcpRegion: String?,
    @Value("\${airbyte.secret.store.aws.region}") awsRegion: String,
    @Value("\${airbyte.secret.store.aws.kms-key-arn}") awsKmsKeyArn: String,
    @Value("\${airbyte.secret.store.aws.tags}") awsTags: String,
    @Value("\${airbyte.secret.store.azure.vault-url}") azureVaultUrl: String,
    @Value("\${airbyte.secret.store.azure.tenant-id}") azureTenantId: String,
    @Value("\${airbyte.secret.store.azure.tags}") azureTags: String,
    @Value("\${airbyte.secret.store.vault.address}") vaultAddress: String,
    @Value("\${airbyte.secret.store.vault.prefix}") vaultPrefix: String,
  ): Map<String, String> =
    buildMap {
      put(EnvVarConstants.SECRET_PERSISTENCE, persistenceType)
      put(EnvVarConstants.SECRET_STORE_GCP_PROJECT_ID, gcpProjectId)
      if (!gcpRegion.isNullOrBlank()) put(EnvVarConstants.SECRET_STORE_GCP_REGION, gcpRegion)
      put(EnvVarConstants.AWS_SECRET_MANAGER_REGION, awsRegion)
      put(EnvVarConstants.AWS_KMS_KEY_ARN, awsKmsKeyArn)
      put(EnvVarConstants.AWS_SECRET_MANAGER_SECRET_TAGS, awsTags)
      put(EnvVarConstants.AZURE_KEY_VAULT_VAULT_URL, azureVaultUrl)
      put(EnvVarConstants.AZURE_KEY_VAULT_TENANT_ID, azureTenantId)
      put(EnvVarConstants.AZURE_KEY_VAULT_SECRET_TAGS, azureTags)
      put(EnvVarConstants.VAULT_ADDRESS, vaultAddress)
      put(EnvVarConstants.VAULT_PREFIX, vaultPrefix)
    }

  /**
   * Environment variables for secret persistence configuration.
   * These values are themselves sourced from kubernetes secrets.
   * The map key is the environment variable name and the map value
   * contains the kubernetes secret name and key for lookup.
   */
  @Singleton
  @Named("secretPersistenceSecretsEnvMap")
  fun secretPersistenceSecretsEnvMap(
    @Value("\${airbyte.secret.store.gcp.credentials-ref-name}") gcpCredsRefName: String,
    @Value("\${airbyte.secret.store.gcp.credentials-ref-key}") gcpCredsRefKey: String,
    @Value("\${airbyte.secret.store.aws.access-key-ref-name}") awsAccessKeyRefName: String,
    @Value("\${airbyte.secret.store.aws.access-key-ref-key}") awsAccessKeyRefKey: String,
    @Value("\${airbyte.secret.store.aws.secret-key-ref-name}") awsSecretKeyRefName: String,
    @Value("\${airbyte.secret.store.aws.secret-key-ref-key}") awsSecretKeyRefKey: String,
    @Value("\${airbyte.secret.store.azure.client-id-ref-name}") azureClientKeyRefName: String,
    @Value("\${airbyte.secret.store.azure.client-id-ref-key}") azureClientKeyRefKey: String,
    @Value("\${airbyte.secret.store.azure.client-secret-ref-name}") azureSecretKeyRefName: String,
    @Value("\${airbyte.secret.store.azure.client-secret-ref-key}") azureSecretKeyRefKey: String,
    @Value("\${airbyte.secret.store.vault.token-ref-name}") vaultTokenRefName: String,
    @Value("\${airbyte.secret.store.vault.token-ref-key}") vaultTokenRefKey: String,
  ): Map<String, EnvVarSource> =
    buildMap {
      // Note: If any of the secret ref names or keys are blank kube will fail to create the pod, so we have to manually exclude empties
      if (gcpCredsRefName.isNotBlank() && gcpCredsRefKey.isNotBlank()) {
        put(EnvVarConstants.SECRET_STORE_GCP_CREDENTIALS, createEnvVarSource(gcpCredsRefName, gcpCredsRefKey))
      }
      if (awsAccessKeyRefName.isNotBlank() && awsAccessKeyRefKey.isNotBlank()) {
        put(EnvVarConstants.AWS_SECRET_MANAGER_ACCESS_KEY_ID, createEnvVarSource(awsAccessKeyRefName, awsAccessKeyRefKey))
      }
      if (awsSecretKeyRefName.isNotBlank() && awsSecretKeyRefKey.isNotBlank()) {
        put(EnvVarConstants.AWS_SECRET_MANAGER_SECRET_ACCESS_KEY, createEnvVarSource(awsSecretKeyRefName, awsSecretKeyRefKey))
      }
      // Azure
      if (azureClientKeyRefName.isNotBlank() && azureClientKeyRefKey.isNotBlank()) {
        put(EnvVarConstants.AZURE_KEY_VAULT_CLIENT_ID, createEnvVarSource(azureClientKeyRefName, azureClientKeyRefKey))
      }
      if (azureSecretKeyRefName.isNotBlank() && azureSecretKeyRefKey.isNotBlank()) {
        put(EnvVarConstants.AZURE_KEY_VAULT_CLIENT_SECRET, createEnvVarSource(azureSecretKeyRefName, azureSecretKeyRefKey))
      }
      if (vaultTokenRefName.isNotBlank() && vaultTokenRefKey.isNotBlank()) {
        put(EnvVarConstants.VAULT_AUTH_TOKEN, createEnvVarSource(vaultTokenRefName, vaultTokenRefKey))
      }
    }

  /**
   * Map of env vars for configuring the WorkloadApiClient (separate from ApiClient).
   */
  @Singleton
  @Named("workloadApiEnvMap")
  fun workloadApiEnvVars(
    @Value("\${$WORKLOAD_API_PREFIX.connect-timeout-seconds}") workloadApiConnectTimeoutSeconds: String,
    @Value("\${$WORKLOAD_API_PREFIX.read-timeout-seconds}") workloadApiReadTimeoutSeconds: String,
    @Value("\${$WORKLOAD_API_PREFIX.retries.delay-seconds}") workloadApiRetriesDelaySeconds: String,
    @Value("\${$WORKLOAD_API_PREFIX.retries.max}") workloadApiRetriesMax: String,
    @Value("\${$WORKLOAD_API_PREFIX.base-path}") workloadApiBasePath: String,
  ): Map<String, String> {
    val envMap: MutableMap<String, String> = HashMap()
    envMap[EnvVarConstants.WORKLOAD_API_HOST_ENV_VAR] = workloadApiBasePath
    envMap[EnvVarConstants.WORKLOAD_API_CONNECT_TIMEOUT_SECONDS_ENV_VAR] = workloadApiConnectTimeoutSeconds
    envMap[EnvVarConstants.WORKLOAD_API_READ_TIMEOUT_SECONDS_ENV_VAR] = workloadApiReadTimeoutSeconds
    envMap[EnvVarConstants.WORKLOAD_API_RETRY_DELAY_SECONDS_ENV_VAR] = workloadApiRetriesDelaySeconds
    envMap[EnvVarConstants.WORKLOAD_API_MAX_RETRIES_ENV_VAR] = workloadApiRetriesMax

    return envMap
  }

  @Singleton
  @Named("databaseEnvMap")
  fun databaseEnvMap(
    @Value("\${airbyte.secret.persistence}") secretPersistenceType: String,
    @Value("\${datasources.local-secrets.url:}") dbUrl: String,
    @Value("\${datasources.local-secrets.username:}") dbUsername: String,
    @Value("\${datasources.local-secrets.password:}") dbPassword: String,
  ): Map<String, String> {
    // Only pass through DB env vars if configured for local storage of secrets
    if (!Configs.SecretPersistenceType.TESTING_CONFIG_DB_TABLE
        .toString()
        .equals(secretPersistenceType, ignoreCase = true)
    ) {
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
    edition: AirbyteEdition,
    version: AirbyteVersion,
    @Value("\${airbyte.role}") role: String,
  ): Map<String, String> =
    mapOf(
      EnvVarConstants.AIRBYTE_EDITION_ENV_VAR to edition.name,
      EnvVarConstants.AIRBYTE_VERSION_ENV_VAR to version.serialize(),
      EnvVarConstants.AIRBYTE_ROLE_ENV_VAR to role,
    )

  @Singleton
  @Named("trackingClientEnvMap")
  fun trackingClientEnvMap(
    @Value("\${airbyte.tracking.strategy}") trackingStrategy: String,
    @Value("\${airbyte.tracking.write-key}") trackingWriteKey: String,
  ) = mapOf(
    EnvVarConstants.TRACKING_STRATEGY to trackingStrategy,
    EnvVarConstants.SEGMENT_WRITE_KEY to trackingWriteKey,
  )
}
