package io.airbyte.workers.sync

import io.airbyte.analytics.AIRBYTE_ROLE_ENV_VAR
import io.airbyte.analytics.AIRBYTE_VERSION_ENV_VAR
import io.airbyte.analytics.DEPLOYMENT_MODE_ENV_VAR
import io.airbyte.analytics.SEGMENT_WRITE_KEY_ENV_VAR
import io.airbyte.analytics.TRACKING_STRATEGY_ENV_VAR
import io.airbyte.commons.envvar.EnvVar

private const val LOG_LEVEL = "LOG_LEVEL"

// necessary for s3/minio logging. used in the logging configuration.
private const val S3_PATH_STYLE_ACCESS = "S3_PATH_STYLE_ACCESS"

object OrchestratorConstants {
  /**
   * Set of all the environment variables necessary for the container orchestrator to run.
   */
  @JvmField
  val ENV_VARS_TO_TRANSFER =
    buildSet {
      // add variables defined in this file
      addAll(
        setOf(
          EnvVar.FEATURE_FLAG_BASEURL.toString(),
          EnvVar.FEATURE_FLAG_CLIENT.toString(),
          EnvVar.FEATURE_FLAG_PATH.toString(),
          LOG_LEVEL,
          S3_PATH_STYLE_ACCESS,
        ),
      )
      // add tracking client
      addAll(
        setOf(
          AIRBYTE_ROLE_ENV_VAR,
          AIRBYTE_VERSION_ENV_VAR,
          DEPLOYMENT_MODE_ENV_VAR,
          SEGMENT_WRITE_KEY_ENV_VAR,
          TRACKING_STRATEGY_ENV_VAR,
        ),
      )
      // add EnvVars
      addAll(
        setOf(
          EnvVar.AWS_ACCESS_KEY_ID,
          EnvVar.AWS_ASSUME_ROLE_SECRET_ACCESS_KEY,
          EnvVar.AWS_DEFAULT_REGION,
          EnvVar.AWS_SECRET_ACCESS_KEY,
          EnvVar.DD_AGENT_HOST,
          EnvVar.DD_DOGSTATSD_PORT,
          EnvVar.DOCKER_HOST,
          EnvVar.GOOGLE_APPLICATION_CREDENTIALS,
          EnvVar.JOB_DEFAULT_ENV_MAP,
          EnvVar.JOB_ERROR_REPORTING_SENTRY_DSN,
          EnvVar.JOB_ERROR_REPORTING_STRATEGY,
          EnvVar.JOB_ISOLATED_KUBE_NODE_SELECTORS,
          EnvVar.JOB_KUBE_ANNOTATIONS,
          EnvVar.JOB_KUBE_LABELS,
          EnvVar.JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY,
          EnvVar.JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET,
          EnvVar.JOB_KUBE_NAMESPACE,
          EnvVar.JOB_KUBE_NODE_SELECTORS,
          EnvVar.JOB_KUBE_SERVICEACCOUNT,
          EnvVar.JOB_KUBE_TOLERATIONS,
          EnvVar.JOB_MAIN_CONTAINER_CPU_LIMIT,
          EnvVar.JOB_MAIN_CONTAINER_CPU_REQUEST,
          EnvVar.JOB_MAIN_CONTAINER_MEMORY_LIMIT,
          EnvVar.JOB_MAIN_CONTAINER_MEMORY_REQUEST,
          EnvVar.LAUNCHDARKLY_KEY,
          EnvVar.LOCAL_DOCKER_MOUNT,
          EnvVar.LOCAL_ROOT,
          EnvVar.METRIC_CLIENT,
          EnvVar.MINIO_ENDPOINT,
          EnvVar.OTEL_COLLECTOR_ENDPOINT,
          EnvVar.PUBLISH_METRICS,
          EnvVar.PUB_SUB_ENABLED,
          EnvVar.PUB_SUB_TOPIC_NAME,
          EnvVar.ROOTLESS_WORKLOAD,
          EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD,
          EnvVar.STORAGE_BUCKET_LOG,
          EnvVar.STORAGE_BUCKET_STATE,
          EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT,
          EnvVar.STORAGE_TYPE,
          EnvVar.USE_CUSTOM_NODE_SELECTOR,
          EnvVar.WORKER_ENVIRONMENT,
          EnvVar.WORKSPACE_DOCKER_MOUNT,
          EnvVar.WORKSPACE_ROOT,
        ).map { it.name },
      )
    }
}
