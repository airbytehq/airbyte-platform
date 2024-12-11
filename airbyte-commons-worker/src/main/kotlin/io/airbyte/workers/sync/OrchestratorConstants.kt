package io.airbyte.workers.sync

import io.airbyte.analytics.SEGMENT_WRITE_KEY_ENV_VAR
import io.airbyte.analytics.TRACKING_STRATEGY_ENV_VAR
import io.airbyte.commons.envvar.EnvVar

object OrchestratorConstants {
  /**
   * Set of all the environment variables necessary for the container orchestrator to run.
   *
   * Todo: Move these out of here and into discrete config beans.
   */
  @JvmField
  val ENV_VARS_TO_TRANSFER =
    buildSet {
      // add tracking client
      addAll(
        setOf(
          SEGMENT_WRITE_KEY_ENV_VAR,
          TRACKING_STRATEGY_ENV_VAR,
        ),
      )
      // add EnvVars
      addAll(
        setOf(
          EnvVar.AWS_DEFAULT_REGION,
          EnvVar.GOOGLE_APPLICATION_CREDENTIALS,
          EnvVar.JOB_ERROR_REPORTING_SENTRY_DSN,
          EnvVar.JOB_ERROR_REPORTING_STRATEGY,
          EnvVar.MINIO_ENDPOINT,
          EnvVar.PUB_SUB_ENABLED,
          EnvVar.PUB_SUB_TOPIC_NAME,
          EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD,
          EnvVar.STORAGE_BUCKET_LOG,
          EnvVar.STORAGE_BUCKET_STATE,
          EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT,
          EnvVar.STORAGE_BUCKET_AUDIT_LOGGING,
          EnvVar.STORAGE_TYPE,
          EnvVar.WORKSPACE_ROOT,
        ).map { it.name },
      )
    }
}
