package io.airbyte.workers.sync

import io.airbyte.analytics.AIRBYTE_ROLE_ENV_VAR
import io.airbyte.analytics.AIRBYTE_VERSION_ENV_VAR
import io.airbyte.analytics.DEPLOYMENT_MODE_ENV_VAR
import io.airbyte.analytics.SEGMENT_WRITE_KEY_ENV_VAR
import io.airbyte.analytics.TRACKING_STRATEGY_ENV_VAR
import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.features.EnvVariableFeatureFlags
import io.airbyte.config.EnvConfigs

private const val LOG_LEVEL = "LOG_LEVEL"

// necessary for s3/minio logging. used in the log4j2 configuration.
private const val S3_PATH_STYLE_ACCESS = "S3_PATH_STYLE_ACCESS"
private const val FEATURE_FLAG_CLIENT = "FEATURE_FLAG_CLIENT"
private const val FEATURE_FLAG_PATH = "FEATURE_FLAG_PATH"

object OrchestratorConstants {
  const val JOB_OUTPUT_FILENAME = "jobOutput.json"
  const val CONNECTION_CONFIGURATION = "connectionConfiguration.json"
  const val EXIT_CODE_FILE = "exitCode.txt"
  const val INIT_FILE_ENV_MAP = "envMap.json"
  const val INIT_FILE_INPUT = "input.json"
  const val INIT_FILE_JOB_RUN_CONFIG = "jobRunConfig.json"
  const val INIT_FILE_APPLICATION = "application.txt"
  const val SIDECAR_INPUT = "sidecarInput.json"
  const val WORKLOAD_ID_FILE = "workload.txt"

  // See the application.yml of the container-orchestrator for value
  const val SERVER_PORT = 9000

  // define two ports for stdout/stderr usage on the container orchestrator pod
  const val PORT1 = 9877
  const val PORT2 = 9878
  const val PORT3 = 9879
  const val PORT4 = 9880

  @JvmField
  val PORTS = setOf(PORT1, PORT2, PORT3, PORT4)

  /**
   * Set of all the environment variables necessary for the container orchestrator to run.
   */
  @JvmField
  val ENV_VARS_TO_TRANSFER =
    buildSet<String> {
      // add variables defined in this file
      addAll(
        setOf(
          FEATURE_FLAG_CLIENT,
          FEATURE_FLAG_PATH,
          LOG_LEVEL,
          S3_PATH_STYLE_ACCESS,
        ),
      )
      // add job shared envs
      addAll(EnvConfigs.JOB_SHARED_ENVS.keys)
      // add EnvVariableFeatureFlags
      addAll(
        setOf(
          EnvVariableFeatureFlags.AUTO_DETECT_SCHEMA,
          EnvVariableFeatureFlags.APPLY_FIELD_SELECTION,
          EnvVariableFeatureFlags.FIELD_SELECTION_WORKSPACES,
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
          EnvVar.JOB_ISOLATED_KUBE_NODE_SELECTORS,
          EnvVar.JOB_KUBE_ANNOTATIONS,
          EnvVar.JOB_KUBE_BUSYBOX_IMAGE,
          EnvVar.JOB_KUBE_CURL_IMAGE,
          EnvVar.JOB_KUBE_LABELS,
          EnvVar.JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY,
          EnvVar.JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET,
          EnvVar.JOB_KUBE_NAMESPACE,
          EnvVar.JOB_KUBE_NODE_SELECTORS,
          EnvVar.JOB_KUBE_SERVICEACCOUNT,
          EnvVar.JOB_KUBE_SIDECAR_CONTAINER_IMAGE_PULL_POLICY,
          EnvVar.JOB_KUBE_SOCAT_IMAGE,
          EnvVar.JOB_KUBE_TOLERATIONS,
          EnvVar.JOB_MAIN_CONTAINER_CPU_LIMIT,
          EnvVar.JOB_MAIN_CONTAINER_CPU_REQUEST,
          EnvVar.JOB_MAIN_CONTAINER_MEMORY_LIMIT,
          EnvVar.JOB_MAIN_CONTAINER_MEMORY_REQUEST,
          EnvVar.LAUNCHDARKLY_KEY,
          EnvVar.LOCAL_DOCKER_MOUNT,
          EnvVar.LOCAL_ROOT,
          EnvVar.LOG4J_CONFIGURATION_FILE,
          EnvVar.METRIC_CLIENT,
          EnvVar.MINIO_ENDPOINT,
          EnvVar.OTEL_COLLECTOR_ENDPOINT,
          EnvVar.PUBLISH_METRICS,
          EnvVar.ROOTLESS_WORKLOAD,
          EnvVar.SOCAT_KUBE_CPU_LIMIT,
          EnvVar.SOCAT_KUBE_CPU_REQUEST,
          EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD,
          EnvVar.STORAGE_BUCKET_LOG,
          EnvVar.STORAGE_BUCKET_STATE,
          EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT,
          EnvVar.STORAGE_TYPE,
          EnvVar.USE_CUSTOM_NODE_SELECTOR,
          EnvVar.WORKER_ENVIRONMENT,
          EnvVar.WORKSPACE_DOCKER_MOUNT,
          EnvVar.WORKSPACE_ROOT,
        )
          .map { it.name },
      )
    }
}
