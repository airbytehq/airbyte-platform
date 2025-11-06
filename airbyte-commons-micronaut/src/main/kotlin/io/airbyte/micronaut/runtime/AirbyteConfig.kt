/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.airbyte.commons.envvar.EnvVar
import io.airbyte.config.Configs
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.EachProperty
import io.micronaut.context.annotation.Parameter
import io.micronaut.context.annotation.Requires
import io.micronaut.logging.LogLevel
import jakarta.inject.Singleton
import java.nio.file.Path
import java.time.Duration
import java.util.UUID

// Configuration properties prefixes
const val AIRBYTE_PREFIX = "airbyte"
const val ANALYTICS_PREFIX = "$AIRBYTE_PREFIX.tracking"
const val API_PREFIX = "$AIRBYTE_PREFIX.api"
const val AUDIT_LOGGING_PREFIX = "$AIRBYTE_PREFIX.audit.logging"
const val AUTH_PREFIX = "$AIRBYTE_PREFIX.auth"
const val CLOUD_PUBSUB_PREFIX = "$AIRBYTE_PREFIX.cloud.pubsub"
const val CONNECTOR_BUILDER_PREFIX = "$AIRBYTE_PREFIX.connector-builder-server"
const val CONNECTOR_PREFIX = "$AIRBYTE_PREFIX.connector"
const val CONNECTOR_REGISTRY_PREFIX = "$AIRBYTE_PREFIX.connector-registry"
const val CONNECTOR_ROLLOUT_PREFIX = "$AIRBYTE_PREFIX.connector-rollout"
const val CONTAINER_PREFIX = "$AIRBYTE_PREFIX.container"
const val CONTROL_PLANE_PREFIX = "$AIRBYTE_PREFIX.control-plane"
const val DATA_DOG_PREFIX = "$AIRBYTE_PREFIX.datadog"
const val DATA_PLANE_PREFIX = "$AIRBYTE_PREFIX.data-plane"
const val DATAPLANE_GROUPS_PREFIX = "$AIRBYTE_PREFIX.dataplane-groups"
const val DB_PREFIX = "$AIRBYTE_PREFIX.db"
const val DB_PATH_PREFIX = "$DB_PREFIX.path"
const val DB_PASSWORD_PREFIX = "$DB_PREFIX.password"
const val DB_USERNAME_PREFIX = "$DB_PREFIX.username"
const val DB_URL_PREFIX = "$DB_PREFIX.url"
const val DB_DRIVER_CLASS_PREFIX = "$DB_PREFIX.driver-class-name"
const val DB_MIGRATION_PREFIX = "$DB_PREFIX.migration"
const val DB_SSL_PREFIX = "$DB_PREFIX.ssl"
const val STIGG_PREFIX = "$AIRBYTE_PREFIX.stigg"
const val FEATURE_FLAG_PREFIX = "$AIRBYTE_PREFIX.feature-flag"
const val FLYWAY_PREFIX = "$AIRBYTE_PREFIX.flyway"
const val INTERNAL_API_PREFIX = "$AIRBYTE_PREFIX.internal-api"
const val INTERNAL_DOCUMENTATION_PREFIX = "$AIRBYTE_PREFIX.internal.documentation"
const val KEYCLOAK_PREFIX = "$AIRBYTE_PREFIX.keycloak"
const val KUBERNETES_PREFIX = "$AIRBYTE_PREFIX.kubernetes"
const val LOGGING_PREFIX = "$AIRBYTE_PREFIX.logging"
const val MANIFEST_SERVER_API_PREFIX = "$AIRBYTE_PREFIX.manifest-server-api"
const val NOTIFICATION_PREFIX = "$AIRBYTE_PREFIX.notification"
const val OPENAI_PREFIX = "$AIRBYTE_PREFIX.openai"
const val ORB_PREFIX = "$AIRBYTE_PREFIX.orb"
const val ORCHESTRATION_PREFIX = "$AIRBYTE_PREFIX.worker"
const val PLATFORM_COMPATIBILITY_PREFIX = "$AIRBYTE_PREFIX.platform-compatibility"
const val POD_SWEEPER_PREFIX = "$AIRBYTE_PREFIX.pod-sweeper"
const val SECRET_PREFIX = "$AIRBYTE_PREFIX.secret"
const val SHUTDOWN_PREFIX = "$AIRBYTE_PREFIX.shutdown"
const val SIDECAR_PREFIX = "$AIRBYTE_PREFIX.sidecar"
const val STORAGE_PREFIX = "$AIRBYTE_PREFIX.cloud.storage"
const val STRIPE_PREFIX = "$AIRBYTE_PREFIX.stripe"
const val TEMPORAL_PREFIX = "$AIRBYTE_PREFIX.temporal"
const val WEBAPP_PREFIX = "$AIRBYTE_PREFIX.web-app"
const val WORKFLOW_PREFIX = "$AIRBYTE_PREFIX.workflow"
const val WORKLOAD_API_PREFIX = "$AIRBYTE_PREFIX.workload-api"
const val WORKLOAD_LAUNCHER_PREFIX = "$AIRBYTE_PREFIX.workload-launcher"

// Default values for configuration properties
internal const val DEFAULT_AIRBYTE_VERSION = "dev"
internal const val DEFAULT_AIRBYTE_DEPLOYMENT_ENVIRONMENT = "local"
internal const val DEFAULT_AIRBYTE_PROTOCOL_MAXIMUM_VERSION = "0.3.0"
internal const val DEFAULT_AIRBYTE_PROTOCOL_MINIMUM_VERSION = "0.0.0"
internal const val DEFAULT_AIRBYTE_WORKSPACE_ROOT = "/workspace"
internal const val DEFAULT_AUTH_DATAPLANE_CLIENT_ID_SECRET_KEY = "dataplane-client-id"
internal const val DEFAULT_AUTH_DATAPLANE_CLIENT_SECRET_SECRET_KEY = "dataplane-client-secret"
internal const val DEFAULT_AUTH_INSTANCE_ADMIN_CLIENT_ID_SECRET_KEY = "instance-admin-client-id"
internal const val DEFAULT_AUTH_INSTANCE_ADMIN_CLIENT_SECRET_SECRET_KEY = "instance-admin-client-secret"
internal const val DEFAULT_AUTH_INSTANCE_ADMIN_PASSWORD_SECRET_KEY = "instance-admin-password"
internal const val DEFAULT_AUTH_JWT_SIGNATURE_SECRET_KEY = "jwt-signature-secret"
internal const val DEFAULT_AUTH_KUBERNETES_SECRET_NAME = "airbyte-auth-secrets"
internal const val DEFAULT_AUTH_REALM = "airbyte"
internal const val DEFAULT_AUTH_APP_TOKEN_EXPIRATION_MINS = 15L
internal const val DEFAULT_AUTH_DATAPLANE_TOKEN_EXPIRATION_MINS = 5L
internal const val DEFAULT_AUTH_EMBEDDED_TOKEN_EXPIRATION_MINS = 20L
internal const val DEFAULT_AUTH_SERVICE_ACCOUNT_TOKEN_EXPIRATION_MINS = 15L
internal const val DEFAULT_AUTH_TOKEN_ISSUER = "airbyte"
internal const val DEFAULT_AWS_ASSUMED_ROLE_ACCESS_KEY = "AWS_ACCESS_KEY_ID"
internal const val DEFAULT_AWS_ASSUMED_ROLE_SECRET_KEY = "AWS_SECRET_ACCESS_KEY"
internal const val DEFAULT_CONNECTOR_BUILDER_API_CONNECT_TIMEOUT_SECONDS = 30
internal const val DEFAULT_CONNECTOR_BUILDER_API_READ_TIMEOUT_SECONDS = 300
internal const val DEFAULT_CONNECTOR_REGISTRY_REMOTE_TIMEOUT_MS = 30000L
internal const val DEFAULT_CONNECTOR_REGISTRY_SEED_PROVIDER = "remote"
internal const val DEFAULT_CONNECTOR_ROLLOUT_WAIT_TIME_BETWEEN_ROLLOUT_SECONDS = 60
internal const val DEFAULT_CONNECTOR_ROLLOUT_WAIT_BETWEEN_SYNC_RESULTS_QUERIES_SECONDS = 10
internal const val DEFAULT_CONNECTOR_ROLLOUT_EXPIRATION_SECONDS = 360
internal const val DEFAULT_CONTEXT_ATTEMPT_ID = 0
internal const val DEFAULT_CONTEXT_JOB_ID = 0L
internal const val DEFAULT_DATA_DOG_ORCHESTRATOR_DISABLED_INTEGRATIONS =
  "GRPC,GRPC_CLIENT,GRPC_SERVER,NETTY,NETTY_4_1,GOOGLE_HTTP_CLIENT,HTTPURLCONNECTION,URLCONNECTION"
internal const val DEFAULT_DATA_PLANE_QUEUE_CHECK = "CHECK_CONNECTION"
internal const val DEFAULT_DATA_PLANE_QUEUE_DISCOVER = "DISCOVER_SCHEMA"
internal const val DEFAULT_DATA_PLANE_QUEUE_SYNC = "SYNC"
internal const val DEFAULT_FEATURE_FLAG_PATH = "/etc/launchdarkly/flags"
internal const val DEFAULT_FLYWAY_INITIALIZATION_TIMEOUT_MS = 60000L
internal const val DEFAULT_INTERNAL_DOCUMENTATION_HOST = "https://reference.airbyte.com/"
internal const val DEFAULT_KEYCLOAK_AIRBYTE_REALM = "airbyte"
internal const val DEFAULT_KEYCLOAK_BASE_PATH = "/auth"
internal const val DEFAULT_KEYCLOAK_CLIENT_ID = "admin-cli"
internal const val DEFAULT_KEYCLOAK_CLIENT_REALM = "airbyte"
internal const val DEFAULT_KEYCLOAK_INTERNAL_REALM = "_airbyte-internal"
internal const val DEFAULT_KEYCLOAK_PASSWORD = "keycloak123"
internal const val DEFAULT_KEYCLOAK_PROTOCOL = "http"
internal const val DEFAULT_KEYCLOAK_REALM = "master"
internal const val DEFAULT_KEYCLOAK_USERNAME = "airbyteAdmin"
internal const val DEFAULT_KEYCLOAK_WEB_CLIENT_ID = "airbyte-webapp"
internal const val DEFAULT_KUBERNETES_CLIENT_CALL_TIMEOUT_SECONDS = 30L
internal const val DEFAULT_KUBERNETES_CLIENT_CONNECT_TIMEOUT_SECONDS = 30L
internal const val DEFAULT_KUBERNETES_CLIENT_READ_TIMEOUT_SECONDS = 30L
internal const val DEFAULT_KUBERNETES_CLIENT_WRITE_TIMEOUT_SECONDS = 30L
internal const val DEFAULT_KUBERNETES_CLIENT_CONNECTION_POOL_KEEP_ALIVE_SECONDS = 600L
internal const val DEFAULT_KUBERNETES_CLIENT_CONNECTION_POOL_MAX_IDLE_CONNECTIONS = 25
internal const val DEFAULT_KUBERNETES_CLIENT_RETRY_DELAY_SECONDS = 2L
internal const val DEFAULT_KUBERNETES_CLIENT_RETRY_MAX = 5
internal const val DEFAULT_KUBERNETES_RESOURCE_CHECK_RATE_SECONDS = "PT30S"
internal const val DEFAULT_LOG_TAIL_SIZE = 1000000
internal const val DEFAULT_MANIFEST_SERVER_API_CONNECT_TIMEOUT_SECONDS = 30L
internal const val DEFAULT_MANIFEST_SERVER_API_READ_TIMEOUT_SECONDS = 300L
internal const val DEFAULT_PLATFORM_COMPATIBILITY_REMOTE_TIMEOUT_MS = 30000L
internal const val DEFAULT_POD_SWEEPER_RUNNING_TTL_MINUTES = -1L
internal const val DEFAULT_POD_SWEEPER_SUCCEEDED_TTL_MINUTES = 10L
internal const val DEFAULT_POD_SWEEPER_UNSUCCESSFUL_TTL_MINUTES = 120L
internal const val DEFAULT_POD_SWEEPER_RATE_MINUTES = "PT2M"
internal const val DEFAULT_SHUTDOWN_DELAY_MS = 20000L
internal const val DEFAULT_SIDECAR_FILE_TIMEOUT_MINUTES = 9
internal const val DEFAULT_SIDECAR_FILE_TIMEOUT_MINUTES_WITHIN_SYNC = 30
internal const val DEFAULT_STORAGE_LOCAL_ROOT = "/storage"
internal const val DEFAULT_STORAGE_LOCATION = "airbyte-storage"
internal const val DEFAULT_STORAGE_REPLICATION_DUMP_LOCATION = "cloud-replication-dump"
internal const val DEFAULT_TEMPORAL_HOST = "airbyte-temporal:7233"
internal const val DEFAULT_TEMPORAL_RETENTION_DAYS = 30
internal const val DEFAULT_TEMPORAL_RPC_TIMEOUT_SECONDS = "PT60S"
internal const val DEFAULT_TEMPORAL_RPC_LONG_POLL_TIMEOUT_SECONDS = "PT70S"
internal const val DEFAULT_TEMPORAL_RPC_QUERY_TIMEOUT_SECONDS = "PT10S"
internal const val DEFAULT_WORKER_CHECK_MAX_WORKERS = 5
internal const val DEFAULT_WORKER_CONNECTION_NO_JITTER_CUTOFF_MINUTES = 5
internal const val DEFAULT_WORKER_CONNECTION_HIGH_FREQUENCY_JITTER_AMOUNT_MINUTES = 2
internal const val DEFAULT_WORKER_CONNECTION_HIGH_FREQUENCY_JITTER_THRESHOLD_MINUTES = 90
internal const val DEFAULT_WORKER_CONNECTION_MEDIUM_FREQUENCY_JITTER_AMOUNT_MINUTES = 5
internal const val DEFAULT_WORKER_CONNECTION_MEDIUM_FREQUENCY_JITTER_THRESHOLD_MINUTES = 150
internal const val DEFAULT_WORKER_CONNECTION_LOW_FREQUENCY_JITTER_AMOUNT_MINUTES = 15
internal const val DEFAULT_WORKER_CONNECTION_LOW_FREQUENCY_JITTER_THRESHOLD_MINUTES = 390
internal const val DEFAULT_WORKER_CONNECTION_VERY_LOW_FREQUENCY_JITTER_AMOUNT_MINUTES = 25
internal const val DEFAULT_WORKER_DISCOVER_AUTO_REFRESH_WINDOW = 1440
internal const val DEFAULT_WORKER_DISCOVER_MAX_WORKERS = 5
internal const val DEFAULT_WORKER_FILE_TRANSFER_EPHEMERAL_STORAGE_LIMIT = "5G"
internal const val DEFAULT_WORKER_FILE_TRANSFER_EPHEMERAL_STORAGE_REQUEST = "5G"
internal const val DEFAULT_WORKER_JOB_NAMESPACE = "jobs"
internal const val DEFAULT_WORKER_KUBE_IMAGE_PULL_POLICY = "IfNotPresent"
internal const val DEFAULT_WORKER_KUBE_PROFILER_CPU_LIMIT = "1"
internal const val DEFAULT_WORKER_KUBE_PROFILER_CPU_REQUEST = "1"
internal const val DEFAULT_WORKER_KUBE_PROFILER_MEMORY_LIMIT = "1024Mi"
internal const val DEFAULT_WORKER_KUBE_PROFILER_MEMORY_REQUEST = "1024Mi"
internal const val DEFAULT_WORKER_KUBE_SERVICE_ACCOUNT = "airbyte-admin"
internal const val DEFAULT_WORKER_KUBE_VOLUME_STAGING_MOUNT_PATH = "/staging/files"
internal const val DEFAULT_WORKER_NOTIFY_MAX_WORKERS = 5
internal const val DEFAULT_WORKER_REPLICATION_DISPATCHER_THREADS = 4
internal const val DEFAULT_WORKER_REPLICATION_PERSISTENCE_FLUSH_PERIOD_SEC = 10L
internal const val DEFAULT_WORKER_SPEC_MAX_WORKERS = 5
internal const val DEFAULT_WORKER_SYNC_MAX_ATTEMPTS = 3
internal const val DEFAULT_WORKER_SYNC_MAX_INIT_TIMEOUT_MINUTES = 3
internal const val DEFAULT_WORKER_SYNC_MAX_TIMEOUT_DAYS = 3
internal const val DEFAULT_WORKER_SYNC_MAX_WORKERS = 5
internal const val DEFAULT_WORKFLOW_FAILURE_RESTART_DELAY = 600L
internal const val DEFAULT_WORKLOAD_REDELIVERY_WINDOW_SECONDS = 300
internal const val DEFAULT_WORKLOAD_LAUNCHER_HEARTBEAT_RATE = "PT30S"
internal const val DEFAULT_WORKLOAD_LAUNCHER_WORKLOAD_START_TIMEOUT = "PT5H"
internal const val DEFAULT_WORKLOAD_LAUNCHER_PARALLELISM = 10
internal const val DEFAULT_WORKLOAD_LAUNCHER_NETWORK_POLICY_INTROSPECTION = false
internal const val DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_POLL_INTERVAL_SECONDS = 1
internal const val DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_POLL_SIZE_ITEMS = 10
internal const val DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_QUEUE_TASK_CAP = 5
internal const val DEFAULT_DATAPLANE_GROUPS_DEFAULT_DATAPLANE_GROUP_NAME = "AUTO"

const val DEFAULT_AUTH_IDENTITY_PROVIDER_TYPE = "simple"
const val DEFAULT_CLOUD_PUBSUB_MESSAGE_COUNT_BATCH_SIZE = 50L
const val DEFAULT_CLOUD_PUBSUB_PUBLISH_DELAY_THRESHOLD_MS = 100L
const val DEFAULT_CLOUD_PUBSUB_REQUEST_BYTES_THRESHOLD = 5000L
const val DEFAULT_CONNECTOR_CONFIG_DIR = "/config"
const val DEFAULT_CONTAINER_ORCHESTRATOR_PLATFORM_MODE = "ORCHESTRATOR"
const val STORAGE_TYPE = "$STORAGE_PREFIX.type"
const val DEFAULT_WORKER_KUBE_JOB_CONFIGURATION = "default"
const val SECRET_MANAGER_AWS: SecretPersistenceTypeName = "aws_secret_manager"
const val SECRET_MANAGER_AZURE_KEY_VAULT: SecretPersistenceTypeName = "azure_key_vault"
const val SECRET_MANAGER_GOOGLE: SecretPersistenceTypeName = "google_secret_manager"
const val SECRET_MANAGER_VAULT: SecretPersistenceTypeName = "vault"
const val SECRET_MANAGER_TESTING_CONFIG_DB_TABLE: SecretPersistenceTypeName = "testing_config_db_table"
const val SECRET_MANAGER_NO_OP: SecretPersistenceTypeName = "no_op"
const val SECRET_PERSISTENCE = "$SECRET_PREFIX.persistence"
const val SECRET_STORE_PREFIX = "$SECRET_PREFIX.store"
const val SECRET_STORE_AWS_PREFIX = "$SECRET_STORE_PREFIX.aws"
const val WORKLOAD_LAUNCHER_HEARTBEAT_RATE = "${WORKLOAD_LAUNCHER_PREFIX}.heartbeat-rate"
const val WORKLOAD_LAUNCHER_NETWORK_POLICY_INTROSPECTION = "${WORKLOAD_LAUNCHER_PREFIX}.network-policy-introspection"

typealias SecretPersistenceTypeName = String

enum class AnalyticsTrackingStrategy {
  LOGGING,
  SEGMENT,
}

enum class EntitlementClientType {
  DEFAULT,
  STIGG,
}

enum class JobErrorReportingStrategy {
  LOGGING,
  SENTRY,
}

enum class SecretPersistenceType {
  AWS_SECRET_MANAGER,
  AZURE_KEY_VAULT,
  GOOGLE_SECRET_MANAGER,
  NO_OP,
  TESTING_CONFIG_DB_TABLE,
  VAULT,
}

enum class StorageType {
  AZURE,
  GCS,
  LOCAL,
  MINIO,
  S3,
}

@ConfigurationProperties(ANALYTICS_PREFIX)
data class AirbyteAnalyticsConfig(
  val flushIntervalSec: Long = 10,
  val strategy: AnalyticsTrackingStrategy = AnalyticsTrackingStrategy.LOGGING,
  val writeKey: String = "",
)

@ConfigurationProperties(API_PREFIX)
data class AirbyteApiConfig(
  val host: String = "",
)

@ConfigurationProperties(AUDIT_LOGGING_PREFIX)
data class AirbyteAuditLoggingConfig(
  val enabled: Boolean = false,
)

@ConfigurationProperties(AUTH_PREFIX)
data class AirbyteAuthConfig(
  val dataplaneCredentials: AirbyteAuthDataplaneCredentialsConfig = AirbyteAuthDataplaneCredentialsConfig(),
  val defaultRealm: String = DEFAULT_AUTH_REALM,
  val identityProvider: AirbyteAuthIdentityProviderConfig = AirbyteAuthIdentityProviderConfig(),
  val initialUser: AirbyteAuthInitialUserConfig = AirbyteAuthInitialUserConfig(),
  val instanceAdmin: AirbyteAuthInstanceAdminConfig = AirbyteAuthInstanceAdminConfig(),
  val kubernetesSecret: AirbyteAuthKubernetesSecretConfig = AirbyteAuthKubernetesSecretConfig(),
  val tokenExpiration: AirbyteAuthTokenExpirationConfig = AirbyteAuthTokenExpirationConfig(),
  val tokenIssuer: String = DEFAULT_AUTH_TOKEN_ISSUER,
) {
  @ConfigurationProperties("instanceAdmin")
  data class AirbyteAuthInstanceAdminConfig(
    val clientId: String = "",
    val clientSecret: String = "",
    val password: String = "",
    val username: String = "",
  )

  @ConfigurationProperties("kubernetes-secret")
  data class AirbyteAuthKubernetesSecretConfig(
    val creationEnabled: Boolean = true,
    val name: String = DEFAULT_AUTH_KUBERNETES_SECRET_NAME,
    val keys: AirbyteAuthKubernetesSecretKeysConfig = AirbyteAuthKubernetesSecretKeysConfig(),
    val values: AirbyteAuthKubernetesSecretValuesConfig = AirbyteAuthKubernetesSecretValuesConfig(),
  ) {
    @ConfigurationProperties("keys")
    data class AirbyteAuthKubernetesSecretKeysConfig(
      val instanceAdminPasswordSecretKey: String = DEFAULT_AUTH_INSTANCE_ADMIN_PASSWORD_SECRET_KEY,
      val instanceAdminClientIdSecretKey: String = DEFAULT_AUTH_INSTANCE_ADMIN_CLIENT_ID_SECRET_KEY,
      val instanceAdminClientSecretSecretKey: String = DEFAULT_AUTH_INSTANCE_ADMIN_CLIENT_SECRET_SECRET_KEY,
      val jwtSignatureSecretKey: String = DEFAULT_AUTH_JWT_SIGNATURE_SECRET_KEY,
    )

    @ConfigurationProperties("values")
    data class AirbyteAuthKubernetesSecretValuesConfig(
      val instanceAdminPassword: String = "",
      val instanceAdminClientId: String = "",
      val instanceAdminClientSecret: String = "",
      val jwtSignatureSecret: String = "",
    )
  }

  @ConfigurationProperties("dataplane-credentials")
  data class AirbyteAuthDataplaneCredentialsConfig(
    val clientIdSecretKey: String = DEFAULT_AUTH_DATAPLANE_CLIENT_ID_SECRET_KEY,
    val clientSecretSecretKey: String = DEFAULT_AUTH_DATAPLANE_CLIENT_SECRET_SECRET_KEY,
  )

  @ConfigurationProperties("token-expiration")
  data class AirbyteAuthTokenExpirationConfig(
    val applicationTokenExpirationInMinutes: Long = DEFAULT_AUTH_APP_TOKEN_EXPIRATION_MINS,
    val dataplaneTokenExpirationInMinutes: Long = DEFAULT_AUTH_DATAPLANE_TOKEN_EXPIRATION_MINS,
    val embeddedTokenExpirationInMinutes: Long = DEFAULT_AUTH_EMBEDDED_TOKEN_EXPIRATION_MINS,
    val serviceAccountTokenExpirationInMinutes: Long = DEFAULT_AUTH_SERVICE_ACCOUNT_TOKEN_EXPIRATION_MINS,
  )

  @ConfigurationProperties("initial-user")
  data class AirbyteAuthInitialUserConfig(
    val email: String = "",
    val firstName: String = "",
    val lastName: String = "",
    val password: String = "",
  )

  @ConfigurationProperties("identity-provider")
  data class AirbyteAuthIdentityProviderConfig(
    val audiences: List<String> = listOf("airbyte-server"),
    val issuers: List<String> = emptyList(),
    val oidc: OidcIdentityProviderConfig = OidcIdentityProviderConfig(),
    val type: String = DEFAULT_AUTH_IDENTITY_PROVIDER_TYPE,
    val verifyAudience: Boolean = false,
    val verifyIssuer: Boolean = false,
  ) {
    @ConfigurationProperties("oidc")
    data class OidcIdentityProviderConfig(
      val appName: String = "",
      val audience: String = "",
      val clientId: String = "",
      val clientSecret: String = "",
      val displayName: String = "",
      val domain: String = "",
      val endpoints: OidcEndpointConfig = OidcEndpointConfig(),
      val extraScopes: String = "",
      val fields: GenericOidcFieldMappingConfig = GenericOidcFieldMappingConfig(),
    ) {
      @ConfigurationProperties("fields")
      data class GenericOidcFieldMappingConfig(
        val sub: String = "sub",
        val email: String = "email",
        val name: String = "name",
        val issuer: String = "iss",
      )

      @ConfigurationProperties("endpoints")
      data class OidcEndpointConfig(
        val authorizationServerEndpoint: String = "",
      )
    }
  }
}

@ConfigurationProperties(CLOUD_PUBSUB_PREFIX)
data class AirbyteCloudPubSubConfig(
  val enabled: Boolean = false,
  val errorReporting: AirbyteCloudPubSubErrorReportingConfig = AirbyteCloudPubSubErrorReportingConfig(),
  val messageCountBatchSize: Long = DEFAULT_CLOUD_PUBSUB_MESSAGE_COUNT_BATCH_SIZE,
  val publishDelayThresholdMs: Long = DEFAULT_CLOUD_PUBSUB_PUBLISH_DELAY_THRESHOLD_MS,
  val requestBytesThreshold: Long = DEFAULT_CLOUD_PUBSUB_REQUEST_BYTES_THRESHOLD,
  val topic: String = "",
) {
  @ConfigurationProperties("error-reporting")
  data class AirbyteCloudPubSubErrorReportingConfig(
    val sentry: AirbyteCloudPubSubErrorReportingSentryConfig = AirbyteCloudPubSubErrorReportingSentryConfig(),
    val strategy: JobErrorReportingStrategy = JobErrorReportingStrategy.LOGGING,
  ) {
    @ConfigurationProperties("sentry")
    data class AirbyteCloudPubSubErrorReportingSentryConfig(
      val dsn: String = "",
    )
  }
}

@ConfigurationProperties(AIRBYTE_PREFIX)
data class AirbyteConfig(
  val acceptanceTestEnabled: Boolean = false,
  val airbyteUrl: String = "",
  val deploymentEnvironment: String = DEFAULT_AIRBYTE_DEPLOYMENT_ENVIRONMENT,
  val installationId: UUID? = null, // Used to track abctl installations and defined/set by abctl
  val licenseKey: String = "",
  val edition: Configs.AirbyteEdition = Configs.AirbyteEdition.COMMUNITY,
  val protocol: AirbyteProtocolConfiguration = AirbyteProtocolConfiguration(),
  val version: String = DEFAULT_AIRBYTE_VERSION,
  val workspaceRoot: String = DEFAULT_AIRBYTE_WORKSPACE_ROOT,
) {
  @ConfigurationProperties("protocol")
  data class AirbyteProtocolConfiguration(
    val minVersion: String = DEFAULT_AIRBYTE_PROTOCOL_MINIMUM_VERSION,
    val maxVersion: String = DEFAULT_AIRBYTE_PROTOCOL_MAXIMUM_VERSION,
  )
}

@ConfigurationProperties("${CONNECTOR_BUILDER_PREFIX}-api")
data class AirbyteConnectorBuilderApiConfig(
  val basePath: String = "",
  val connectTimeoutSeconds: Int = DEFAULT_CONNECTOR_BUILDER_API_CONNECT_TIMEOUT_SECONDS,
  val readTimeoutSeconds: Int = DEFAULT_CONNECTOR_BUILDER_API_READ_TIMEOUT_SECONDS,
  val signatureSecret: String = "",
)

@ConfigurationProperties(CONNECTOR_BUILDER_PREFIX)
data class AirbyteConnectorBuilderConfig(
  val aiAssist: AirbyteConnectorBuilderAiAssistConfig = AirbyteConnectorBuilderAiAssistConfig(),
  val capabilities: AirbyteConnectorBuilderCapabilitiesConfig = AirbyteConnectorBuilderCapabilitiesConfig(),
  val github: AirbyteConnectorBuilderGithubConfig = AirbyteConnectorBuilderGithubConfig(),
) {
  @ConfigurationProperties("ai-assist")
  data class AirbyteConnectorBuilderAiAssistConfig(
    val urlBase: String = "",
  )

  @ConfigurationProperties("capabilities")
  data class AirbyteConnectorBuilderCapabilitiesConfig(
    val enableUnsafeCode: Boolean = false,
  )

  @ConfigurationProperties("github")
  data class AirbyteConnectorBuilderGithubConfig(
    val airbytePatToken: String = "",
  )
}

@ConfigurationProperties(CONNECTOR_PREFIX)
data class AirbyteConnectorConfig(
  val configDir: String = DEFAULT_CONNECTOR_CONFIG_DIR,
  val specificResourceDefaultsEnabled: Boolean = false,
  val source: AirbyteSourceConnectorConfig = AirbyteSourceConnectorConfig(),
  val stagingDir: String = "",
) {
  @ConfigurationProperties("source")
  data class AirbyteSourceConnectorConfig(
    val credentials: AirbyteSourceConnectorCredentialsConfig = AirbyteSourceConnectorCredentialsConfig(),
  ) {
    @ConfigurationProperties("credentials")
    data class AirbyteSourceConnectorCredentialsConfig(
      val aws: AirbyteSourceConnectorAwsCredentialsConfig = AirbyteSourceConnectorAwsCredentialsConfig(),
    ) {
      @ConfigurationProperties("aws")
      data class AirbyteSourceConnectorAwsCredentialsConfig(
        val assumedRole: AirbyteSourceConnectorAwsAssumedRoleConfig = AirbyteSourceConnectorAwsAssumedRoleConfig(),
      ) {
        @ConfigurationProperties("assumed-role")
        data class AirbyteSourceConnectorAwsAssumedRoleConfig(
          val accessKey: String = DEFAULT_AWS_ASSUMED_ROLE_ACCESS_KEY,
          val secretKey: String = DEFAULT_AWS_ASSUMED_ROLE_SECRET_KEY,
          val secretName: String = "",
        )
      }
    }
  }
}

@ConfigurationProperties(CONNECTOR_REGISTRY_PREFIX)
data class AirbyteConnectorRegistryConfig(
  val enterprise: AirbyteConnectorRegistryEnterpriseConfig = AirbyteConnectorRegistryEnterpriseConfig(),
  val remote: AirbyteConnectorRegistryRemoteConfig = AirbyteConnectorRegistryRemoteConfig(),
  val seedProvider: String = DEFAULT_CONNECTOR_REGISTRY_SEED_PROVIDER,
) {
  @ConfigurationProperties("remote")
  data class AirbyteConnectorRegistryRemoteConfig(
    val baseUrl: String = "",
    val timeoutMs: Long = DEFAULT_CONNECTOR_REGISTRY_REMOTE_TIMEOUT_MS,
  )

  @ConfigurationProperties("enterprise")
  data class AirbyteConnectorRegistryEnterpriseConfig(
    val enterpriseStubsUrl: String = "",
  )
}

@ConfigurationProperties(CONNECTOR_ROLLOUT_PREFIX)
data class AirbyteConnectorRolloutConfig(
  val gcs: AirbyteConnectorRolloutGcsConfig = AirbyteConnectorRolloutGcsConfig(),
  val timeouts: AirbyteConnectorRolloutTimeoutsConfig = AirbyteConnectorRolloutTimeoutsConfig(),
) {
  @ConfigurationProperties("gcs")
  data class AirbyteConnectorRolloutGcsConfig(
    val applicationCredentials: String = "",
    val projectId: String = "",
    val bucketName: String = "",
    val objectPrefix: String = "",
  )

  @ConfigurationProperties("timeouts")
  data class AirbyteConnectorRolloutTimeoutsConfig(
    val waitBetweenRolloutSeconds: Int = DEFAULT_CONNECTOR_ROLLOUT_WAIT_TIME_BETWEEN_ROLLOUT_SECONDS,
    val waitBetweenSyncResultsQueriesSeconds: Int = DEFAULT_CONNECTOR_ROLLOUT_WAIT_BETWEEN_SYNC_RESULTS_QUERIES_SECONDS,
    val rolloutExpirationSeconds: Int = DEFAULT_CONNECTOR_ROLLOUT_EXPIRATION_SECONDS,
  )
}

@ConfigurationProperties(CONTAINER_PREFIX)
data class AirbyteContainerConfig(
  val rootlessWorkload: Boolean = false,
)

@ConfigurationProperties("$CONTAINER_PREFIX.orchestrator")
data class AirbyteContainerOrchestratorConfig(
  val enableUnsafeCode: Boolean = false,
  val javaOpts: String = "",
  val platformMode: String = DEFAULT_CONTAINER_ORCHESTRATOR_PLATFORM_MODE,
)

@ConfigurationProperties("${AIRBYTE_PREFIX}.context")
data class AirbyteContextConfig(
  val attemptId: Int = DEFAULT_CONTEXT_ATTEMPT_ID,
  val connectionId: String = "",
  val jobId: Long = DEFAULT_CONTEXT_JOB_ID,
  val workloadId: String = "",
  val workspaceId: String = "",
) {
  fun connectionIdAsUUID(): UUID = UUID.fromString(connectionId)

  fun workspaceIdAsUUID(): UUID = UUID.fromString(workspaceId)
}

@ConfigurationProperties(CONTROL_PLANE_PREFIX)
data class AirbyteControlPlaneConfig(
  val authEndpoint: String = "",
)

@ConfigurationProperties(DATA_DOG_PREFIX)
data class AirbyteDataDogConfig(
  val agent: AirbyteDataDogAgentConfig = AirbyteDataDogAgentConfig(),
  val orchestratorDisabledIntegrations: String = DEFAULT_DATA_DOG_ORCHESTRATOR_DISABLED_INTEGRATIONS,
  val env: String = "",
  val version: String = "",
) {
  @ConfigurationProperties("agent")
  data class AirbyteDataDogAgentConfig(
    val host: String = "",
    val port: String = "",
  )
}

@ConfigurationProperties(DATA_PLANE_PREFIX)
data class AirbyteDataPlaneConfig(
  val credentials: AirbyteDataPlaneCredentialsConfig = AirbyteDataPlaneCredentialsConfig(),
  val serviceAccount: AirbyteDataPlaneServiceAccountConfig = AirbyteDataPlaneServiceAccountConfig(),
) {
  @ConfigurationProperties("credentials")
  data class AirbyteDataPlaneCredentialsConfig(
    val clientIdSecretName: String = "",
    val clientIdSecretKey: String = "",
    val clientSecretSecretName: String = "",
    val clientSecretSecretKey: String = "",
  )

  @ConfigurationProperties("service-account")
  data class AirbyteDataPlaneServiceAccountConfig(
    val credentialsPath: String = "",
    val email: String = "",
  )
}

@ConfigurationProperties(DATA_PLANE_PREFIX)
data class AirbyteDataPlaneQueueConfig(
  val check: AirbyteDataPlaneQueueCheckConfig = AirbyteDataPlaneQueueCheckConfig(),
  val discover: AirbyteDataPlaneQueueDiscoverConfig = AirbyteDataPlaneQueueDiscoverConfig(),
  val sync: AirbyteDataPlaneQueueSyncConfig = AirbyteDataPlaneQueueSyncConfig(),
) {
  @ConfigurationProperties("check")
  data class AirbyteDataPlaneQueueCheckConfig(
    val taskQueue: String = DEFAULT_DATA_PLANE_QUEUE_CHECK,
  )

  @ConfigurationProperties("discover")
  data class AirbyteDataPlaneQueueDiscoverConfig(
    val taskQueue: String = DEFAULT_DATA_PLANE_QUEUE_DISCOVER,
  )

  @ConfigurationProperties("sync")
  data class AirbyteDataPlaneQueueSyncConfig(
    val taskQueue: String = DEFAULT_DATA_PLANE_QUEUE_SYNC,
  )
}

@ConfigurationProperties(DATAPLANE_GROUPS_PREFIX)
data class AirbyteDataplaneGroupsConfig(
  val defaultDataplaneGroupName: String = DEFAULT_DATAPLANE_GROUPS_DEFAULT_DATAPLANE_GROUP_NAME,
)

@ConfigurationProperties(STIGG_PREFIX)
data class AirbyteStiggClientConfig(
  val apiKey: String = "",
  val enabled: Boolean = false,
  val sidecarHost: String = "",
  val sidecarPort: Int = 8800,
  val webhookSecret: String = "",
)

@ConfigurationProperties(FEATURE_FLAG_PREFIX)
data class AirbyteFeatureFlagConfig(
  val baseUrl: String = "",
  val client: FeatureFlagClientType = FeatureFlagClientType.CONFIGFILE,
  val path: Path = Path.of(DEFAULT_FEATURE_FLAG_PATH),
  val apiKey: String = "",
) {
  enum class FeatureFlagClientType {
    CONFIGFILE,
    FFS,
    LAUNCHDARKLY,
    TEST,
  }
}

@ConfigurationProperties(FLYWAY_PREFIX)
data class AirbyteFlywayConfig(
  val config: AirbyteFlywayConfigDatabaseConfig = AirbyteFlywayConfigDatabaseConfig(),
  val jobs: AirbyteFlywayJobsDatabaseConfig = AirbyteFlywayJobsDatabaseConfig(),
) {
  @ConfigurationProperties("config")
  data class AirbyteFlywayConfigDatabaseConfig(
    val initializationTimeoutMs: Long = DEFAULT_FLYWAY_INITIALIZATION_TIMEOUT_MS,
    val minimumMigrationVersion: String = "",
  )

  @ConfigurationProperties("jobs")
  data class AirbyteFlywayJobsDatabaseConfig(
    val initializationTimeoutMs: Long = DEFAULT_FLYWAY_INITIALIZATION_TIMEOUT_MS,
    val minimumMigrationVersion: String = "",
  )
}

@ConfigurationProperties(INTERNAL_API_PREFIX)
data class AirbyteInternalApiClientConfig(
  val basePath: String = "",
  val workloadApiHost: String = "",
  val connectorBuilderApiHost: String = "",
  val connectTimeoutSeconds: Long = 30,
  val readTimeoutSeconds: Long = 600,
  val throwsOn5xx: Boolean = true,
  val retries: RetryConfig = RetryConfig(),
  val auth: AuthConfig = AuthConfig(),
) {
  enum class AuthType {
    DATAPLANE_ACCESS_TOKEN,
    INTERNAL_CLIENT_TOKEN,
  }

  @ConfigurationProperties("auth")
  data class AuthConfig(
    val type: AuthType = AuthType.INTERNAL_CLIENT_TOKEN,
    val clientId: String = "",
    val clientSecret: String = "",
    val tokenEndpoint: String = "",
    val token: String = "",
    val signatureSecret: String = "",
  )

  @ConfigurationProperties("retries")
  data class RetryConfig(
    val max: Int = 5,
    val delaySeconds: Long = 2,
    val jitterFactor: Double = .25,
  )
}

@ConfigurationProperties(INTERNAL_DOCUMENTATION_PREFIX)
data class AirbyteInternalDocumentationConfig(
  val host: String = DEFAULT_INTERNAL_DOCUMENTATION_HOST,
)

/**
 * This class bundles all internal Keycloak configuration into a convenient singleton that can be
 * consumed by multiple Micronaut applications.
 */
@ConfigurationProperties(KEYCLOAK_PREFIX)
data class AirbyteKeycloakConfig(
  val airbyteRealm: String = DEFAULT_KEYCLOAK_AIRBYTE_REALM,
  val basePath: String = DEFAULT_KEYCLOAK_BASE_PATH,
  val clientId: String = DEFAULT_KEYCLOAK_CLIENT_ID,
  val clientRealm: String = DEFAULT_KEYCLOAK_CLIENT_REALM,
  val host: String = "",
  val internalRealm: String = DEFAULT_KEYCLOAK_INTERNAL_REALM,
  val password: String = DEFAULT_KEYCLOAK_PASSWORD,
  val protocol: String = DEFAULT_KEYCLOAK_PROTOCOL,
  val realm: String = DEFAULT_KEYCLOAK_REALM,
  val resetRealm: Boolean = false,
  val username: String = DEFAULT_KEYCLOAK_USERNAME,
  val webClientId: String = DEFAULT_KEYCLOAK_WEB_CLIENT_ID,
) {
  fun getKeycloakUserInfoEndpointForRealm(realm: String): String {
    val hostWithoutTrailingSlash = if (host.endsWith("/")) host.substring(0, host.length - 1) else host
    val basePathWithLeadingSlash = if (basePath.startsWith("/")) basePath else "/$basePath"
    val keycloakUserInfoURI = "/protocol/openid-connect/userinfo"
    return "$protocol://$hostWithoutTrailingSlash$basePathWithLeadingSlash/realms/$realm$keycloakUserInfoURI"
  }

  fun getServerUrl(): String = "$protocol://$host$basePath"
}

@ConfigurationProperties(KUBERNETES_PREFIX)
data class AirbyteKubernetesConfig(
  val client: AirbyteKubernetesClientConfig = AirbyteKubernetesClientConfig(),
  val resourceCheckRate: Duration = Duration.parse(DEFAULT_KUBERNETES_RESOURCE_CHECK_RATE_SECONDS),
) {
  @ConfigurationProperties("client")
  data class AirbyteKubernetesClientConfig(
    val callTimeoutSec: Long = DEFAULT_KUBERNETES_CLIENT_CALL_TIMEOUT_SECONDS,
    val connectTimeoutSec: Long = DEFAULT_KUBERNETES_CLIENT_CONNECT_TIMEOUT_SECONDS,
    val readTimeoutSec: Long = DEFAULT_KUBERNETES_CLIENT_READ_TIMEOUT_SECONDS,
    val writeTimeoutSec: Long = DEFAULT_KUBERNETES_CLIENT_WRITE_TIMEOUT_SECONDS,
    val connectionPool: AirbyteKubernetesClientConnectionPoolConfig = AirbyteKubernetesClientConnectionPoolConfig(),
    val retries: AirbyteKubernetesClientRetryConfig = AirbyteKubernetesClientRetryConfig(),
  ) {
    @ConfigurationProperties("connection-pool")
    data class AirbyteKubernetesClientConnectionPoolConfig(
      val keepAliveSec: Long = DEFAULT_KUBERNETES_CLIENT_CONNECTION_POOL_KEEP_ALIVE_SECONDS,
      val maxIdleConnections: Int = DEFAULT_KUBERNETES_CLIENT_CONNECTION_POOL_MAX_IDLE_CONNECTIONS,
    )

    @ConfigurationProperties("retries")
    data class AirbyteKubernetesClientRetryConfig(
      val delaySeconds: Long = DEFAULT_KUBERNETES_CLIENT_RETRY_DELAY_SECONDS,
      val max: Int = DEFAULT_KUBERNETES_CLIENT_RETRY_MAX,
    )
  }
}

@ConfigurationProperties(LOGGING_PREFIX)
data class AirbyteLoggingConfig(
  val client: AirbyteLoggingClientConfig = AirbyteLoggingClientConfig(),
  val logLevel: LogLevel = LogLevel.INFO,
  val s3PathStyleAccess: String = "",
) {
  @ConfigurationProperties("client")
  data class AirbyteLoggingClientConfig(
    val logTailSize: Int = DEFAULT_LOG_TAIL_SIZE,
  )
}

@ConfigurationProperties(MANIFEST_SERVER_API_PREFIX)
data class AirbyteManifestServerApiClientConfig(
  val basePath: String = "",
  val connectTimeoutSeconds: Long = DEFAULT_MANIFEST_SERVER_API_CONNECT_TIMEOUT_SECONDS,
  val readTimeoutSeconds: Long = DEFAULT_MANIFEST_SERVER_API_READ_TIMEOUT_SECONDS,
  val signatureSecret: String = "",
)

@ConfigurationProperties(NOTIFICATION_PREFIX)
data class AirbyteNotificationConfig(
  val customerIo: AirbyteNotificationCustomerIoConfig = AirbyteNotificationCustomerIoConfig(),
) {
  @ConfigurationProperties("customerio")
  data class AirbyteNotificationCustomerIoConfig(
    val apiKey: String = "",
  )
}

@ConfigurationProperties(OPENAI_PREFIX)
data class AirbyteOpenAiConfig(
  val apiKeys: AirbyteOpenAiApiKeysConfig = AirbyteOpenAiApiKeysConfig(),
) {
  @ConfigurationProperties("api-keys")
  data class AirbyteOpenAiApiKeysConfig(
    val failedSyncAssistant: String = "",
  )
}

@ConfigurationProperties(PLATFORM_COMPATIBILITY_PREFIX)
data class AirbytePlatformCompatibilityConfig(
  val remote: AirbytePlatformCompatibilityRemoteConfig = AirbytePlatformCompatibilityRemoteConfig(),
) {
  @ConfigurationProperties("remote")
  data class AirbytePlatformCompatibilityRemoteConfig(
    val timeoutMs: Long = DEFAULT_PLATFORM_COMPATIBILITY_REMOTE_TIMEOUT_MS,
  )
}

@ConfigurationProperties(POD_SWEEPER_PREFIX)
data class AirbytePodSweeperConfig(
  val runningTtl: Long = DEFAULT_POD_SWEEPER_RUNNING_TTL_MINUTES,
  val succeededTtl: Long = DEFAULT_POD_SWEEPER_SUCCEEDED_TTL_MINUTES,
  val unsuccessfulTtl: Long = DEFAULT_POD_SWEEPER_UNSUCCESSFUL_TTL_MINUTES,
  val rate: Duration = Duration.parse(DEFAULT_POD_SWEEPER_RATE_MINUTES),
)

interface SecretsManagerConfig {
  fun toEnvVarMap(): Map<String, String>
}

@ConfigurationProperties(SECRET_PREFIX)
data class AirbyteSecretsManagerConfig(
  val persistence: SecretPersistenceType = SecretPersistenceType.NO_OP,
  val store: AirbyteSecretsManagerStoreConfig,
  val useRuntimeSecretPersistence: Boolean = false,
) {
  fun getSecretsConfig() =
    when (persistence) {
      SecretPersistenceType.AWS_SECRET_MANAGER -> store.aws
      SecretPersistenceType.AZURE_KEY_VAULT -> store.azure
      SecretPersistenceType.GOOGLE_SECRET_MANAGER -> store.gcp
      SecretPersistenceType.NO_OP -> NoOpSecretsManagerConfig()
      SecretPersistenceType.TESTING_CONFIG_DB_TABLE -> TestSecretsManagerConfig()
      SecretPersistenceType.VAULT -> store.vault
    }

  @ConfigurationProperties("store")
  data class AirbyteSecretsManagerStoreConfig(
    val aws: AwsSecretsManagerConfig = AwsSecretsManagerConfig(),
    val azure: AzureKeyVaultSecretsManagerConfig = AzureKeyVaultSecretsManagerConfig(),
    val gcp: GoogleSecretsManagerConfig = GoogleSecretsManagerConfig(),
    val vault: VaultSecretsManagerConfig = VaultSecretsManagerConfig(),
  ) {
    /**
     * AWS Secrets Manager configuration
     *
     * TODO: we should not refer to this by the cloud provider, but instead by the product name (e.g. aws_secret_manager)
     */
    @ConfigurationProperties("aws")
    data class AwsSecretsManagerConfig(
      val accessKey: String = "",
      val secretKey: String = "",
      val region: String = "",
      val kmsKeyArn: String = "",
      val tags: String = "",
      val accessKeyRefName: String = "",
      val accessKeyRefKey: String = "",
      val secretKeyRefName: String = "",
      val secretKeyRefKey: String = "",
    ) : SecretsManagerConfig {
      override fun toEnvVarMap(): Map<String, String> = emptyMap()

      override fun toString(): String =
        "AwsSecretsManagerConfig(accessKey=${accessKey.mask()}, secretKey=${secretKey.mask()}, region=$region, kmsKeyArn=$kmsKeyArn, tags=$tags)"
    }

    /**
     * Azure Key Vault configuration
     *
     * TODO: we should not refer to this by the cloud provider, but instead by the product name (e.g. azure_key_vault)
     */
    @ConfigurationProperties("azure")
    data class AzureKeyVaultSecretsManagerConfig(
      val vaultUrl: String = "",
      val tenantId: String = "",
      val clientId: String = "",
      val clientSecret: String = "",
      val tags: String = "",
      val clientIdRefName: String = "",
      val clientIdRefKey: String = "",
      val clientSecretRefName: String = "",
      val clientSecretRefKey: String = "",
    ) : SecretsManagerConfig {
      override fun toEnvVarMap(): Map<String, String> = emptyMap()

      override fun toString(): String =
        "AzureKeyVaultSecretsManagerConfig(vaultUrl=$vaultUrl, tenantId=$tenantId, clientId=${clientId.mask()}, clientSecret=${clientSecret.mask()}, tags=$tags)"
    }

    /**
     * Google Secrets Manager configuration
     *
     * TODO: we should not refer to this by the cloud provider, but instead by the product name (e.g. google_secret_manager)
     */
    @ConfigurationProperties("gcp")
    data class GoogleSecretsManagerConfig(
      val projectId: String = "",
      val credentials: String = "",
      val region: String? = "",
      val credentialsRefName: String = "",
      val credentialsRefKey: String = "",
    ) : SecretsManagerConfig {
      override fun toEnvVarMap(): Map<String, String> = emptyMap()

      override fun toString(): String = "GoogleSecretsManagerConfig(projectId=$projectId, credentials=${credentials.mask()}, region=$region)"
    }

    /**
     * Vault configuration
     */
    @ConfigurationProperties("vault")
    data class VaultSecretsManagerConfig(
      val address: String = "",
      val prefix: String = "",
      val token: String = "",
      val tokenRefName: String = "",
      val tokenRefKey: String = "",
    ) : SecretsManagerConfig {
      override fun toEnvVarMap(): Map<String, String> = emptyMap()

      override fun toString(): String = "VaultSecretsManagerConfig(address=$address, prefix=$prefix, token=${token.mask()})"
    }
  }
}

class TestSecretsManagerConfig : SecretsManagerConfig {
  override fun toEnvVarMap(): Map<String, String> = emptyMap()

  override fun toString(): String = "TestSecretsManagerConfig"
}

class NoOpSecretsManagerConfig : SecretsManagerConfig {
  override fun toEnvVarMap(): Map<String, String> = emptyMap()

  override fun toString(): String = "NoOpSecretsManagerConfig"
}

@ConfigurationProperties(SHUTDOWN_PREFIX)
data class AirbyteShutdownConfig(
  val delayMs: Long = DEFAULT_SHUTDOWN_DELAY_MS,
)

@ConfigurationProperties(SIDECAR_PREFIX)
data class AirbyteSidecarConfig(
  val fileTimeoutMinutes: Int = DEFAULT_SIDECAR_FILE_TIMEOUT_MINUTES,
  val fileTimeoutMinutesWithinSync: Int = DEFAULT_SIDECAR_FILE_TIMEOUT_MINUTES_WITHIN_SYNC,
)

@ConfigurationProperties(STORAGE_PREFIX)
data class AirbyteStorageConfig(
  val type: StorageType = StorageType.MINIO,
  val bucket: AirbyteStorageBucketConfig = AirbyteStorageBucketConfig(),
  val azure: AzureStorageConfig = AzureStorageConfig(),
  val gcs: GcsStorageConfig = GcsStorageConfig(),
  val local: LocalStorageConfig = LocalStorageConfig(),
  val minio: MinioStorageConfig = MinioStorageConfig(),
  val s3: S3StorageConfig = S3StorageConfig(),
) {
  fun getStorageConfig() =
    when (type) {
      StorageType.AZURE -> azure
      StorageType.GCS -> gcs
      StorageType.S3 -> s3
      StorageType.MINIO -> minio
      StorageType.LOCAL -> local
    }

  @ConfigurationProperties("bucket")
  data class AirbyteStorageBucketConfig(
    val log: String = DEFAULT_STORAGE_LOCATION,
    val state: String = DEFAULT_STORAGE_LOCATION,
    val workloadOutput: String = DEFAULT_STORAGE_LOCATION,
    val activityPayload: String = DEFAULT_STORAGE_LOCATION,
    val auditLogging: String = DEFAULT_STORAGE_LOCATION,
    val profilerOutput: String = DEFAULT_STORAGE_LOCATION,
    val replicationDump: String = DEFAULT_STORAGE_REPLICATION_DUMP_LOCATION,
  )

  /**
   * Azure storage configuration
   */
  @ConfigurationProperties("azure")
  @Requires(property = STORAGE_TYPE, pattern = "(?i)^azure$")
  data class AzureStorageConfig(
    val connectionString: String = "",
  ) : StorageConfig {
    override fun toString(): String = "AzureStorageConfig(connectionString=${connectionString.mask()})"
  }

  /**
   * GCS storage configuration
   */
  @ConfigurationProperties("gcs")
  @Requires(property = STORAGE_TYPE, pattern = "(?i)^gcs$")
  data class GcsStorageConfig(
    val applicationCredentials: String = "",
  ) : StorageConfig {
    override fun toString(): String = "GcsStorageConfig(applicationCredentials=${applicationCredentials.mask()})"
  }

  /**
   * Local storage configuration
   */
  @ConfigurationProperties("local")
  @Requires(property = STORAGE_TYPE, pattern = "(?i)^local$")
  data class LocalStorageConfig(
    val root: String = DEFAULT_STORAGE_LOCAL_ROOT,
  ) : StorageConfig

  /**
   * MinIO storage configuration
   */
  @ConfigurationProperties("minio")
  @Requires(property = STORAGE_TYPE, pattern = "(?i)^minio$")
  data class MinioStorageConfig(
    val accessKey: String = "",
    val secretAccessKey: String = "",
    val endpoint: String = "",
  ) : StorageConfig {
    override fun toString(): String =
      "MinioStorageConfig(accessKey=${accessKey.mask()}, secretAccessKey=${secretAccessKey.mask()}, endpoint=$endpoint)"
  }

  /**
   * S3 storage configuration
   */
  @ConfigurationProperties("s3")
  @Requires(property = STORAGE_TYPE, pattern = "(?i)^s3$")
  data class S3StorageConfig(
    val accessKey: String = "",
    val secretAccessKey: String = "",
    val region: String = "",
  ) : StorageConfig {
    override fun toString(): String = "S3StorageConfig(accessKey=${accessKey.mask()}, secretAccessKey=${secretAccessKey.mask()}, region=$region)"
  }
}

interface StorageConfig

@Singleton
class StorageEnvironmentVariableProvider(
  private val storageConfig: StorageConfig,
  private val buckets: AirbyteStorageConfig.AirbyteStorageBucketConfig,
) {
  fun toEnvVarMap(): Map<String, String> {
    val envVars =
      mutableMapOf<String, String>().apply {
        put(EnvVar.STORAGE_BUCKET_LOG.name, buckets.log)
        put(EnvVar.STORAGE_BUCKET_STATE.name, buckets.state)
        put(EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name, buckets.workloadOutput)
        put(EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name, buckets.activityPayload)
        put(EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.name, buckets.auditLogging)
        put(EnvVar.STORAGE_BUCKET_REPLICATION_DUMP.name, buckets.replicationDump)
      }
    when (storageConfig) {
      is AirbyteStorageConfig.AzureStorageConfig -> {
        envVars.apply {
          put(EnvVar.STORAGE_TYPE.name, StorageType.AZURE.name)
          put(EnvVar.AZURE_STORAGE_CONNECTION_STRING.name, storageConfig.connectionString)
        }
      }
      is AirbyteStorageConfig.GcsStorageConfig -> {
        envVars.apply {
          put(EnvVar.STORAGE_TYPE.name, StorageType.GCS.name)
          put(EnvVar.GOOGLE_APPLICATION_CREDENTIALS.name, storageConfig.applicationCredentials)
        }
      }
      is AirbyteStorageConfig.LocalStorageConfig -> {
        envVars.apply {
          put(EnvVar.STORAGE_TYPE.name, StorageType.LOCAL.name)
        }
      }
      is AirbyteStorageConfig.MinioStorageConfig -> {
        envVars.apply {
          put(EnvVar.STORAGE_TYPE.name, StorageType.MINIO.name)
          put(EnvVar.AWS_ACCESS_KEY_ID.name, storageConfig.accessKey)
          put(EnvVar.AWS_SECRET_ACCESS_KEY.name, storageConfig.secretAccessKey)
          put(EnvVar.MINIO_ENDPOINT.name, storageConfig.endpoint)
        }
      }
      is AirbyteStorageConfig.S3StorageConfig -> {
        envVars.apply {
          put(EnvVar.STORAGE_TYPE.name, StorageType.S3.name)
          put(EnvVar.AWS_ACCESS_KEY_ID.name, storageConfig.accessKey)
          put(EnvVar.AWS_SECRET_ACCESS_KEY.name, storageConfig.secretAccessKey)
          put(EnvVar.AWS_DEFAULT_REGION.name, storageConfig.region)
        }
      }
    }

    return envVars
  }
}

@ConfigurationProperties(STRIPE_PREFIX)
data class AirbyteStripeConfig(
  val apiKey: String = "",
  val endpointSecret: String = "",
)

@ConfigurationProperties(TEMPORAL_PREFIX)
data class AirbyteTemporalConfig(
  val cloud: AirbyteTemporalCloudConfig = AirbyteTemporalCloudConfig(),
  val host: String = DEFAULT_TEMPORAL_HOST,
  val retention: Int = DEFAULT_TEMPORAL_RETENTION_DAYS,
  val sdk: AirbyteTemporalSdkConfig = AirbyteTemporalSdkConfig(),
) {
  @ConfigurationProperties("cloud")
  data class AirbyteTemporalCloudConfig(
    val billing: AirbyteTemporalBillingConfig = AirbyteTemporalBillingConfig(),
    val client: AirbyteTemporalCloudClientConfig = AirbyteTemporalCloudClientConfig(),
    val connectorRollout: AirbyteTemporalCloudConnectorRolloutConfig = AirbyteTemporalCloudConnectorRolloutConfig(),
    val enabled: Boolean = false,
    val host: String = "",
    val namespace: String = "",
  ) {
    @ConfigurationProperties("billing")
    data class AirbyteTemporalBillingConfig(
      val host: String = "",
      val namespace: String = "",
    )

    @ConfigurationProperties("client")
    data class AirbyteTemporalCloudClientConfig(
      val cert: String = "",
      val key: String = "",
    )

    @ConfigurationProperties("connector-rollout")
    data class AirbyteTemporalCloudConnectorRolloutConfig(
      val host: String = "",
      val namespace: String = "",
    )
  }

  @ConfigurationProperties("sdk")
  data class AirbyteTemporalSdkConfig(
    val timeouts: AirbyteTemporalSdkTimeoutConfig = AirbyteTemporalSdkTimeoutConfig(),
  ) {
    @ConfigurationProperties("timeouts")
    data class AirbyteTemporalSdkTimeoutConfig(
      val rpcTimeout: Duration = Duration.parse(DEFAULT_TEMPORAL_RPC_TIMEOUT_SECONDS),
      val rpcLongPollTimeout: Duration = Duration.parse(DEFAULT_TEMPORAL_RPC_LONG_POLL_TIMEOUT_SECONDS),
      val rpcQueryTimeout: Duration = Duration.parse(DEFAULT_TEMPORAL_RPC_QUERY_TIMEOUT_SECONDS),
    )
  }
}

@ConfigurationProperties(WEBAPP_PREFIX)
data class AirbyteWebappConfig(
  val datadogApplicationId: String = "",
  val datadogClientToken: String = "",
  val datadogEnv: String = "",
  val datadogService: String = "",
  val datadogSite: String = "",
  val hockeystackApiKey: String = "",
  val launchdarklyKey: String = "",
  val osanoKey: String = "",
  val posthogApiKey: String = "",
  val posthogHost: String = "",
  val segmentToken: String = "",
  val sonarApiUrl: String = "",
  val coralAgentsApiUrl: String = "",
  val url: String = "",
  val zendeskKey: String = "",
)

@ConfigurationProperties(ORCHESTRATION_PREFIX)
data class AirbyteWorkerConfig(
  val check: AirbyteWorkerCheckConfig = AirbyteWorkerCheckConfig(),
  val connection: AirbyteWorkerConnectionConfig = AirbyteWorkerConnectionConfig(),
  val connectorSidecar: AirbyteWorkerConnectorSidecarConfig = AirbyteWorkerConnectorSidecarConfig(),
  val discover: AirbyteWorkerDiscoverConfig = AirbyteWorkerDiscoverConfig(),
  val fileTransfer: AirbyteWorkerFileTransferConfig = AirbyteWorkerFileTransferConfig(),
  val isolated: AirbyteWorkerIsolatedConfig = AirbyteWorkerIsolatedConfig(),
  val job: AirbyteWorkerJobConfig = AirbyteWorkerJobConfig(),
  val kubeJobConfigVariantOverride: String = "",
  val kubeJobConfigs: List<AirbyteWorkerKubeJobConfig> = listOf(AirbyteWorkerKubeJobConfig()),
  val notify: AirbyteWorkerNotifyConfig = AirbyteWorkerNotifyConfig(),
  val replication: AirbyteWorkerReplicationConfig = AirbyteWorkerReplicationConfig(),
  val spec: AirbyteWorkerSpecConfig = AirbyteWorkerSpecConfig(),
  val sync: AirbyteWorkerSyncConfig = AirbyteWorkerSyncConfig(),
) {
  @ConfigurationProperties("check")
  data class AirbyteWorkerCheckConfig(
    val enabled: Boolean = true,
    val maxWorkers: Int = DEFAULT_WORKER_CHECK_MAX_WORKERS,
  )

  @ConfigurationProperties("connection")
  data class AirbyteWorkerConnectionConfig(
    val enabled: Boolean = true,
    val scheduleJitter: AirbyteWorkerConnectionScheduleJitterConfig = AirbyteWorkerConnectionScheduleJitterConfig(),
  ) {
    @ConfigurationProperties("schedule-jitter")
    data class AirbyteWorkerConnectionScheduleJitterConfig(
      val noJitterCutoffMinutes: Int = DEFAULT_WORKER_CONNECTION_NO_JITTER_CUTOFF_MINUTES,
      val highFrequencyBucket: AirbyteWorkerConnectionScheduleJitterHighFrequencyBucketConfig =
        AirbyteWorkerConnectionScheduleJitterHighFrequencyBucketConfig(),
      val mediumFrequencyBucket: AirbyteWorkerConnectionScheduleJitterMediumFrequencyBucketConfig =
        AirbyteWorkerConnectionScheduleJitterMediumFrequencyBucketConfig(),
      val lowFrequencyBucket: AirbyteWorkerConnectionScheduleJitterLowFrequencyBucketConfig =
        AirbyteWorkerConnectionScheduleJitterLowFrequencyBucketConfig(),
      val veryLowFrequencyBucket: AirbyteWorkerConnectionScheduleJitterVeryLowFrequencyBucketConfig =
        AirbyteWorkerConnectionScheduleJitterVeryLowFrequencyBucketConfig(),
    ) {
      @ConfigurationProperties("high-frequency-bucket")
      data class AirbyteWorkerConnectionScheduleJitterHighFrequencyBucketConfig(
        val jitterAmountMinutes: Int = DEFAULT_WORKER_CONNECTION_HIGH_FREQUENCY_JITTER_AMOUNT_MINUTES,
        val thresholdMinutes: Int = DEFAULT_WORKER_CONNECTION_HIGH_FREQUENCY_JITTER_THRESHOLD_MINUTES,
      )

      @ConfigurationProperties("medium-frequency-bucket")
      data class AirbyteWorkerConnectionScheduleJitterMediumFrequencyBucketConfig(
        val jitterAmountMinutes: Int = DEFAULT_WORKER_CONNECTION_MEDIUM_FREQUENCY_JITTER_AMOUNT_MINUTES,
        val thresholdMinutes: Int = DEFAULT_WORKER_CONNECTION_MEDIUM_FREQUENCY_JITTER_THRESHOLD_MINUTES,
      )

      @ConfigurationProperties("low-frequency-bucket")
      data class AirbyteWorkerConnectionScheduleJitterLowFrequencyBucketConfig(
        val jitterAmountMinutes: Int = DEFAULT_WORKER_CONNECTION_LOW_FREQUENCY_JITTER_AMOUNT_MINUTES,
        val thresholdMinutes: Int = DEFAULT_WORKER_CONNECTION_LOW_FREQUENCY_JITTER_THRESHOLD_MINUTES,
      )

      @ConfigurationProperties("very-low-frequency-bucket")
      data class AirbyteWorkerConnectionScheduleJitterVeryLowFrequencyBucketConfig(
        val jitterAmountMinutes: Int = DEFAULT_WORKER_CONNECTION_VERY_LOW_FREQUENCY_JITTER_AMOUNT_MINUTES,
      )
    }
  }

  @ConfigurationProperties("connector-sidecar")
  data class AirbyteWorkerConnectorSidecarConfig(
    val resources: AirbyteWorkerConnectorSidecarResourcesConfig = AirbyteWorkerConnectorSidecarResourcesConfig(),
  ) {
    @ConfigurationProperties("resources")
    data class AirbyteWorkerConnectorSidecarResourcesConfig(
      val cpuLimit: String = "",
      val cpuRequest: String = "",
      val memoryLimit: String = "",
      val memoryRequest: String = "",
    )
  }

  @ConfigurationProperties("discover")
  data class AirbyteWorkerDiscoverConfig(
    val autoRefreshWindow: Int = DEFAULT_WORKER_DISCOVER_AUTO_REFRESH_WINDOW,
    val enabled: Boolean = true,
    val maxWorkers: Int = DEFAULT_WORKER_DISCOVER_MAX_WORKERS,
  )

  @ConfigurationProperties("file-transfer")
  data class AirbyteWorkerFileTransferConfig(
    val resources: AirbyteWorkerFileTransferResourcesConfiguration = AirbyteWorkerFileTransferResourcesConfiguration(),
  ) {
    @ConfigurationProperties("resources")
    data class AirbyteWorkerFileTransferResourcesConfiguration(
      val ephemeralStorageLimit: String = DEFAULT_WORKER_FILE_TRANSFER_EPHEMERAL_STORAGE_LIMIT,
      val ephemeralStorageRequest: String = DEFAULT_WORKER_FILE_TRANSFER_EPHEMERAL_STORAGE_REQUEST,
    )
  }

  @ConfigurationProperties("isolated")
  data class AirbyteWorkerIsolatedConfig(
    val kube: AirbyteWorkerIsolatedKubeConfig = AirbyteWorkerIsolatedKubeConfig(),
  ) {
    @ConfigurationProperties("kube")
    data class AirbyteWorkerIsolatedKubeConfig(
      val useCustomNodeSelector: Boolean = false,
      val nodeSelectors: String = "",
    )
  }

  @ConfigurationProperties("job")
  data class AirbyteWorkerJobConfig(
    val errorReporting: AirbyteWorkerJobErrorReportingConfig = AirbyteWorkerJobErrorReportingConfig(),
    val kubernetes: AirbyteWorkerJobKubernetesConfig = AirbyteWorkerJobKubernetesConfig(),
  ) {
    @ConfigurationProperties("error-reporting")
    data class AirbyteWorkerJobErrorReportingConfig(
      val sentry: AirbyteWorkerJobErrorReportingSentryConfig = AirbyteWorkerJobErrorReportingSentryConfig(),
      val strategy: JobErrorReportingStrategy = JobErrorReportingStrategy.LOGGING,
    ) {
      @ConfigurationProperties("sentry")
      data class AirbyteWorkerJobErrorReportingSentryConfig(
        val dsn: String = "",
      )
    }

    @ConfigurationProperties("kube")
    data class AirbyteWorkerJobKubernetesConfig(
      val connectorImageRegistry: String = "",
      val init: AirbyteWorkerJobKubernetesInitConfig = AirbyteWorkerJobKubernetesInitConfig(),
      val main: AirbyteWorkerJobKubernetesMainConfig = AirbyteWorkerJobKubernetesMainConfig(),
      val namespace: String = DEFAULT_WORKER_JOB_NAMESPACE,
      val profiler: AirbyteWorkerJobKubernetesProfilerConfig = AirbyteWorkerJobKubernetesProfilerConfig(),
      val serviceAccount: String = DEFAULT_WORKER_KUBE_SERVICE_ACCOUNT,
      val sidecar: AirbyteWorkerJobKubernetesSidecarConfig = AirbyteWorkerJobKubernetesSidecarConfig(),
      val tolerations: String = "",
      val volumes: AirbyteWorkerJobKubernetesVolumeConfig = AirbyteWorkerJobKubernetesVolumeConfig(),
    ) {
      @ConfigurationProperties("init")
      data class AirbyteWorkerJobKubernetesInitConfig(
        val container: AirbyteWorkerJobKubernetesInitContainerConfig = AirbyteWorkerJobKubernetesInitContainerConfig(),
      ) {
        @ConfigurationProperties("container")
        data class AirbyteWorkerJobKubernetesInitContainerConfig(
          val image: String = "",
          val imagePullPolicy: String = DEFAULT_WORKER_KUBE_IMAGE_PULL_POLICY,
          val imagePullSecret: List<String> = emptyList(),
        )
      }

      @ConfigurationProperties("main")
      data class AirbyteWorkerJobKubernetesMainConfig(
        val container: AirbyteWorkerJobKubernetesMainContainerConfig = AirbyteWorkerJobKubernetesMainContainerConfig(),
      ) {
        @ConfigurationProperties("container")
        data class AirbyteWorkerJobKubernetesMainContainerConfig(
          val image: String = "",
          val imagePullPolicy: String = DEFAULT_WORKER_KUBE_IMAGE_PULL_POLICY,
          val imagePullSecret: List<String> = emptyList(),
        )
      }

      @ConfigurationProperties("profiler")
      data class AirbyteWorkerJobKubernetesProfilerConfig(
        val container: AirbyteWorkerJobKubernetesProfilerContainerConfig = AirbyteWorkerJobKubernetesProfilerContainerConfig(),
      ) {
        @ConfigurationProperties("container")
        data class AirbyteWorkerJobKubernetesProfilerContainerConfig(
          val image: String = "",
          val imagePullPolicy: String = DEFAULT_WORKER_KUBE_IMAGE_PULL_POLICY,
          val imagePullSecret: List<String> = emptyList(),
          val cpuLimit: String = DEFAULT_WORKER_KUBE_PROFILER_CPU_LIMIT,
          val cpuRequest: String = DEFAULT_WORKER_KUBE_PROFILER_CPU_REQUEST,
          val memoryLimit: String = DEFAULT_WORKER_KUBE_PROFILER_MEMORY_LIMIT,
          val memoryRequest: String = DEFAULT_WORKER_KUBE_PROFILER_MEMORY_REQUEST,
        )
      }

      @ConfigurationProperties("sidecar")
      data class AirbyteWorkerJobKubernetesSidecarConfig(
        val container: AirbyteWorkerJobKubernetesSidecarContainerConfig = AirbyteWorkerJobKubernetesSidecarContainerConfig(),
      ) {
        @ConfigurationProperties("container")
        data class AirbyteWorkerJobKubernetesSidecarContainerConfig(
          val image: String = "",
          val imagePullPolicy: String = DEFAULT_WORKER_KUBE_IMAGE_PULL_POLICY,
          val imagePullSecret: List<String> = emptyList(),
        )
      }

      @ConfigurationProperties("volumes")
      data class AirbyteWorkerJobKubernetesVolumeConfig(
        val dataPlaneCreds: AirbyteWorkerJobKubernetesVolumeDataPlaneCredentialsConfig =
          AirbyteWorkerJobKubernetesVolumeDataPlaneCredentialsConfig(),
        val local: AirbyteWorkerJobKubernetesVolumeLocalConfig = AirbyteWorkerJobKubernetesVolumeLocalConfig(),
        val gcsCreds: AirbyteWorkerJobKubernetesVolumeGcsCredsConfig = AirbyteWorkerJobKubernetesVolumeGcsCredsConfig(),
        val secret: AirbyteWorkerJobKubernetesVolumeSecretConfig = AirbyteWorkerJobKubernetesVolumeSecretConfig(),
        val staging: AirbyteWorkerJobKubernetesVolumeStagingConfig = AirbyteWorkerJobKubernetesVolumeStagingConfig(),
      ) {
        @ConfigurationProperties("data-plane-creds")
        data class AirbyteWorkerJobKubernetesVolumeDataPlaneCredentialsConfig(
          val mountPath: String = "",
          val secretName: String = "",
        )

        @ConfigurationProperties("local")
        data class AirbyteWorkerJobKubernetesVolumeLocalConfig(
          val enabled: Boolean = false,
        )

        @ConfigurationProperties("gcs-creds")
        data class AirbyteWorkerJobKubernetesVolumeGcsCredsConfig(
          val mountPath: String = "",
          val secretName: String = "",
        )

        @ConfigurationProperties("secret")
        data class AirbyteWorkerJobKubernetesVolumeSecretConfig(
          val mountPath: String = "",
          val secretName: String = "",
        )

        @ConfigurationProperties("staging")
        data class AirbyteWorkerJobKubernetesVolumeStagingConfig(
          val mountPath: String = DEFAULT_WORKER_KUBE_VOLUME_STAGING_MOUNT_PATH,
        )
      }
    }
  }

  /**
   * This class uses [EachProperty] instead of [ConfigurationProperties] so that it can dynamically
   * handle job-specific configuration.  For example, Kubernetes resources may be set per job
   * type (sync, check, discover, etc), connector type (api, database, etc) or other vectors.
   * Because of this, the fields in this configuration class need to be modifiable, as Micronaut
   * will first create the instance based on each dynamic "name" and then try to set the fields
   * via a setter.
   */
  @EachProperty("kube-job-configs")
  class AirbyteWorkerKubeJobConfig(
    @param:Parameter val name: String = DEFAULT_WORKER_KUBE_JOB_CONFIGURATION,
  ) {
    var annotations: String = ""
    var labels: String = ""
    var nodeSelectors: String = ""
    var cpuLimit: String = ""
    var cpuRequest: String = ""
    var memoryLimit: String = ""
    var memoryRequest: String = ""
    var ephemeralStorageLimit: String = ""
    var ephemeralStorageRequest: String = ""
  }

  @ConfigurationProperties("notify")
  data class AirbyteWorkerNotifyConfig(
    val enabled: Boolean = true,
    val maxWorkers: Int = DEFAULT_WORKER_NOTIFY_MAX_WORKERS,
  )

  @ConfigurationProperties("replication")
  data class AirbyteWorkerReplicationConfig(
    val dispatcher: AirbyteWorkerReplicationDispatcherConfig = AirbyteWorkerReplicationDispatcherConfig(),
    val persistenceFlushPeriodSec: Long = DEFAULT_WORKER_REPLICATION_PERSISTENCE_FLUSH_PERIOD_SEC,
  ) {
    @ConfigurationProperties("dispatcher")
    data class AirbyteWorkerReplicationDispatcherConfig(
      val nThreads: Int = DEFAULT_WORKER_REPLICATION_DISPATCHER_THREADS,
    )
  }

  @ConfigurationProperties("spec")
  data class AirbyteWorkerSpecConfig(
    val enabled: Boolean = true,
    val maxWorkers: Int = DEFAULT_WORKER_SPEC_MAX_WORKERS,
  )

  @ConfigurationProperties("sync")
  data class AirbyteWorkerSyncConfig(
    val enabled: Boolean = true,
    val maxWorkers: Int = DEFAULT_WORKER_SYNC_MAX_WORKERS,
    val maxAttempts: Int = DEFAULT_WORKER_SYNC_MAX_ATTEMPTS,
    val maxTimeout: Int = DEFAULT_WORKER_SYNC_MAX_TIMEOUT_DAYS,
    val maxInitTimeout: Int = DEFAULT_WORKER_SYNC_MAX_INIT_TIMEOUT_MINUTES,
  )
}

@ConfigurationProperties(WORKFLOW_PREFIX)
data class AirbyteWorkflowConfig(
  val failure: AirbyteWorkflowFailureConfig = AirbyteWorkflowFailureConfig(),
) {
  @ConfigurationProperties("failure")
  data class AirbyteWorkflowFailureConfig(
    val restartDelay: Long = DEFAULT_WORKFLOW_FAILURE_RESTART_DELAY,
  )
}

@ConfigurationProperties(WORKLOAD_API_PREFIX)
data class AirbyteWorkloadApiClientConfig(
  val basePath: String = "",
  val connectTimeoutSeconds: Long = 30,
  val readTimeoutSeconds: Long = 600,
  val retries: RetryConfig = RetryConfig(),
  val heartbeat: WorkloadApiHeartbeatConfig = WorkloadApiHeartbeatConfig(),
  val workloadRedeliveryWindowSeconds: Int = DEFAULT_WORKLOAD_REDELIVERY_WINDOW_SECONDS,
) {
  @ConfigurationProperties("heartbeat")
  data class WorkloadApiHeartbeatConfig(
    val intervalSeconds: Long = 10,
    val timeoutSeconds: Long = 600,
  )

  @ConfigurationProperties("retries")
  data class RetryConfig(
    val max: Int = 5,
    val delaySeconds: Long = 2,
    val jitterFactor: Double = .25,
  )
}

@ConfigurationProperties(WORKLOAD_LAUNCHER_PREFIX)
data class AirbyteWorkloadLauncherConfig(
  val heartbeatRate: Duration = Duration.parse(DEFAULT_WORKLOAD_LAUNCHER_HEARTBEAT_RATE),
  val workloadStartTimeout: Duration = Duration.parse(DEFAULT_WORKLOAD_LAUNCHER_WORKLOAD_START_TIMEOUT),
  val networkPolicyIntrospection: Boolean = DEFAULT_WORKLOAD_LAUNCHER_NETWORK_POLICY_INTROSPECTION,
  val parallelism: AirbyteWorkloadLauncherParallelismConfig = AirbyteWorkloadLauncherParallelismConfig(),
  val consumer: AirbyteWorkloadLauncherConsumerConfig = AirbyteWorkloadLauncherConsumerConfig(),
) {
  @ConfigurationProperties("parallelism")
  data class AirbyteWorkloadLauncherParallelismConfig(
    val defaultQueue: Int = DEFAULT_WORKLOAD_LAUNCHER_PARALLELISM,
    val highPriorityQueue: Int = DEFAULT_WORKLOAD_LAUNCHER_PARALLELISM,
    val maxSurge: Int = DEFAULT_WORKLOAD_LAUNCHER_PARALLELISM,
  )

  @ConfigurationProperties("consumer")
  data class AirbyteWorkloadLauncherConsumerConfig(
    val queueTaskCap: Int = DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_QUEUE_TASK_CAP,
    val defaultQueue: AirbyteWorkloadLauncherDefaultQueueConsumerConfig =
      AirbyteWorkloadLauncherDefaultQueueConsumerConfig(),
    val highPriorityQueue: AirbyteWorkloadLauncherHighPriorityQueueConsumerConfig =
      AirbyteWorkloadLauncherHighPriorityQueueConsumerConfig(),
  ) {
    @ConfigurationProperties("default-queue")
    data class AirbyteWorkloadLauncherDefaultQueueConsumerConfig(
      val pollIntervalSeconds: Int = DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_POLL_INTERVAL_SECONDS,
      val pollSizeItems: Int = DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_POLL_SIZE_ITEMS,
    )

    @ConfigurationProperties("high-priority-queue")
    data class AirbyteWorkloadLauncherHighPriorityQueueConsumerConfig(
      val pollIntervalSeconds: Int = DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_POLL_INTERVAL_SECONDS,
      val pollSizeItems: Int = DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_POLL_SIZE_ITEMS,
    )
  }
}

fun SecretPersistenceType.toSecretPersistenceTypeName(): SecretPersistenceTypeName =
  when (this) {
    SecretPersistenceType.AWS_SECRET_MANAGER -> SECRET_MANAGER_AWS
    SecretPersistenceType.AZURE_KEY_VAULT -> SECRET_MANAGER_AZURE_KEY_VAULT
    SecretPersistenceType.GOOGLE_SECRET_MANAGER -> SECRET_MANAGER_GOOGLE
    SecretPersistenceType.NO_OP -> SECRET_MANAGER_NO_OP
    SecretPersistenceType.VAULT -> SECRET_MANAGER_VAULT
    SecretPersistenceType.TESTING_CONFIG_DB_TABLE -> SECRET_MANAGER_TESTING_CONFIG_DB_TABLE
  }

fun SecretPersistenceTypeName.toSecretPersistenceType(): SecretPersistenceType? =
  when (this) {
    SECRET_MANAGER_AWS -> SecretPersistenceType.AWS_SECRET_MANAGER
    SECRET_MANAGER_AZURE_KEY_VAULT -> SecretPersistenceType.AZURE_KEY_VAULT
    SECRET_MANAGER_GOOGLE -> SecretPersistenceType.GOOGLE_SECRET_MANAGER
    SECRET_MANAGER_VAULT -> SecretPersistenceType.VAULT
    SECRET_MANAGER_NO_OP -> SecretPersistenceType.NO_OP
    SECRET_MANAGER_TESTING_CONFIG_DB_TABLE -> SecretPersistenceType.TESTING_CONFIG_DB_TABLE
    else -> null
  }

/**
 * Extension function to mask any sensitive data.
 *
 * Any non-null [String] that calls [mask] will return `"*******"`. Any null [String] will return `"null"`.
 */
internal fun String?.mask(): String = this?.let { _ -> "*******" } ?: "null"
