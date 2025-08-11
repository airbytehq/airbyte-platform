/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.envvar

/**
 * A collection of an environment variables currently used by the Airbyte platform.
 *
 * The enum value _must exactly_ match the name of environment-variables.
 *
 * These are defined in alphabetical order for findability/readability reasons.
 */
enum class EnvVar {
  AIRBYTE_EDITION,
  AIRBYTE_ENABLE_UNSAFE_CODE,
  AIRBYTE_ROLE,
  AIRBYTE_URL,
  AIRBYTE_VERSION,
  ATTEMPT_ID,
  AWS_ACCESS_KEY_ID,
  AWS_ASSUME_ROLE_SECRET_ACCESS_KEY,
  AWS_ASSUME_ROLE_SECRET_NAME,
  AWS_DEFAULT_REGION,
  AWS_SECRET_ACCESS_KEY,
  AZURE_STORAGE_CONNECTION_STRING,

  CDK_ENTRYPOINT,
  CDK_PYTHON,
  CLOUD_STORAGE_APPENDER_THREADS,
  CONFIG_ROOT,
  CONNECTION_ID,
  CUSTOMERIO_API_KEY,

  DATABASE_PASSWORD,
  DATABASE_URL,
  DATABASE_USER,
  DD_AGENT_HOST,
  DD_CONSTANT_TAGS,
  DD_DOGSTATSD_PORT,
  DD_SERVICE,
  DD_VERSION,
  DEPLOYMENT_ENV,
  DOCKER_HOST,
  DOCKER_NETWORK,

  FEATURE_FLAG_BASEURL,
  FEATURE_FLAG_CLIENT,
  FEATURE_FLAG_PATH,

  GOOGLE_APPLICATION_CREDENTIALS,

  JAVA_OPTS,
  JOB_DEFAULT_ENV_,
  JOB_DEFAULT_ENV_MAP,
  JOB_ERROR_REPORTING_SENTRY_DSN,
  JOB_ERROR_REPORTING_STRATEGY,
  JOB_ID,
  JOB_ISOLATED_KUBE_NODE_SELECTORS,
  JOB_KUBE_ANNOTATIONS,
  JOB_KUBE_LABELS,
  JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY,
  JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET,
  JOB_KUBE_NAMESPACE,
  JOB_KUBE_NODE_SELECTORS,
  JOB_KUBE_SERVICEACCOUNT,
  JOB_KUBE_TOLERATIONS,
  JOB_MAIN_CONTAINER_CPU_LIMIT,
  JOB_MAIN_CONTAINER_CPU_REQUEST,
  JOB_MAIN_CONTAINER_MEMORY_LIMIT,
  JOB_MAIN_CONTAINER_MEMORY_REQUEST,

  LAUNCHDARKLY_KEY,
  LOCAL,
  LOCAL_CONNECTOR_CATALOG_PATH,
  LOCAL_DOCKER_MOUNT,
  LOG_IDLE_ROUTE_TTL,
  LOG_LEVEL,

  MINIO_ENDPOINT,

  OPERATION_TYPE,

  PATH_TO_CONNECTORS,
  PLATFORM_LOG_FORMAT,
  PUBLISH_METRICS,
  PUB_SUB_ENABLED,
  PUB_SUB_TOPIC_NAME,

  REMOTE_DATAPLANE_SERVICEACCOUNTS,
  ROOTLESS_WORKLOAD,

  S3_PATH_STYLE_ACCESS,
  SERVICE_NAME,
  SIDECAR_KUBE_CPU_LIMIT,
  SIDECAR_MEMORY_REQUEST,
  STORAGE_BUCKET_ACTIVITY_PAYLOAD,

  /**
   * STORAGE_BUCKET_AUDIT_LOGGING is separate from other log storage buckets.
   * It is by default unset unless the SME customer enables the audit-logging feature via `values.yaml`.
   */
  STORAGE_BUCKET_AUDIT_LOGGING,
  STORAGE_BUCKET_LOG,
  STORAGE_BUCKET_REPLICATION_DUMP,
  STORAGE_BUCKET_STATE,
  STORAGE_BUCKET_WORKLOAD_OUTPUT,
  STORAGE_TYPE,
  SYNC_JOB_INIT_RETRY_TIMEOUT_MINUTES,

  TEMPORAL_HISTORY_RETENTION_IN_DAYS,

  USE_CUSTOM_NODE_SELECTOR,

  WORKER_ENVIRONMENT,
  WORKLOAD_ID,
  WORKSPACE_DOCKER_MOUNT,
  WORKSPACE_ROOT,

  /** These exist testing purposes only! DO NOT USE in non-test code! */
  Z_TESTING_PURPOSES_ONLY_1,
  Z_TESTING_PURPOSES_ONLY_2,
  Z_TESTING_PURPOSES_ONLY_3,
  ;

  /**
   * Fetch the value of this [EnvVar], returning [default] if the value is null or an empty string.
   *
   * @param default value to return if this environment variable is null or empty
   */
  @Deprecated("Inject your env vars with Micronaut. System.getenv is a last resort.")
  @JvmOverloads
  fun fetch(default: String? = null): String? = System.getenv(this.name).takeUnless { it.isNullOrBlank() } ?: default

  /**
   * Fetch the value of this [EnvVar], returning a non-null [default] if the value is null or an empty string.
   *
   * @param default value to return if this environment variable is null or empty
   *
   * If kotlin contracts ever become stable, this method could be replaced with a contract on the [fetch] method.
   */
  fun fetchNotNull(default: String = ""): String = System.getenv(this.name).takeUnless { it.isNullOrBlank() } ?: default
}
