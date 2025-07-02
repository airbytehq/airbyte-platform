/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

/**
 * Collection of constants for APM tracing.
 */
object ApmTraceConstants {
  /**
   * Operation name for an APM trace of a Temporal activity.
   */
  const val ACTIVITY_TRACE_OPERATION_NAME: String = "activity"

  /**
   * Operation name for an APM trace of API endpoint execution.
   */
  const val ENDPOINT_EXECUTION_OPERATION_NAME: String = "execute"

  /**
   * Operation name for an APM trace of a job orchestrator.
   */
  const val JOB_ORCHESTRATOR_OPERATION_NAME: String = "job.orchestrator"

  /**
   * Operation name for an APM trace of a worker implementation.
   */
  const val WORKER_OPERATION_NAME: String = "worker"

  /**
   * Operation name for an APM trace of a Temporal workflow.
   */
  const val WORKFLOW_TRACE_OPERATION_NAME: String = "workflow"

  /**
   * Trace tag constants.
   */
  object Tags {
    /**
     * Name of the APM trace tag that holds the attempt number value associated with the trace.
     */
    const val ATTEMPT_NUMBER_KEY: String = "attempt_number"

    /**
     * Name of the APM trace tag that holds the connector builder project id value associated with the
     * trace.
     */
    const val CONNECTOR_BUILDER_PROJECT_ID_KEY: String = "connector_builder_project_id"

    /**
     * Name of the APM trace tag that holds the destination Docker image value associated with the
     * trace.
     */
    const val CONNECTION_ID_KEY: String = "connection_id"

    /**
     * Name of the APM trace tag that holds the connector version value associated with the trace.
     */
    const val CONNECTOR_VERSION_KEY: String = "connector_version"

    /**
     * Name of the APM trace tag that holds the destination definition ID value associated with the
     * trace.
     */
    const val DESTINATION_DEFINITION_ID_KEY: String = "destination.definition_id"

    /**
     * Name of the APM trace tag that holds the destination Docker image value associated with the
     * trace.
     */
    const val DESTINATION_DOCKER_IMAGE_KEY: String = "destination.docker_image"

    /**
     * Name of the APM trace tag that holds the destination ID value associated with the trace.
     */
    const val DESTINATION_ID_KEY: String = "destination.id"

    /**
     * Name of the APM trace tag that holds the Docker image value associated with the trace.
     */
    const val DOCKER_IMAGE_KEY: String = "docker_image"

    /**
     * Name of the APM trace tag that holds the actual type of the error.
     */
    const val ERROR_ACTUAL_TYPE_KEY: String = "error.actual_type"

    /**
     * Name of the APM trace tag that holds the failure origin(s) associated with the trace.
     */
    const val FAILURE_ORIGINS_KEY: String = "failure_origins"

    /**
     * Name of the APM trace tag that holds the failure type(s) associated with the trace.
     */
    const val FAILURE_TYPES_KEY: String = "failure_types"

    /**
     * Name of the APM trace tag that holds whether the sync is a reset or not.
     */
    const val IS_RESET_KEY: String = "is_reset"

    /**
     * Name of the APM trace tag that holds the job ID value associated with the trace.
     */
    const val JOB_ID_KEY: String = "job_id"

    /**
     * Name of the APM trace tag that holds the job root value associated with the trace.
     */
    const val JOB_ROOT_KEY: String = "job_root"

    /**
     * Name of the APM trace tag that holds the process exit value associated with the trace.
     */
    const val PROCESS_EXIT_VALUE_KEY: String = "process.exit_value"

    /**
     * Name of the APM trace tag that holds the replication bytes synced value associated with the
     * trace.
     */
    const val REPLICATION_BYTES_SYNCED_KEY: String = "replication.bytes_synced"

    /**
     * Name of the APM trace tag that holds the replication records synced value associated with the
     * trace.
     */
    const val REPLICATION_RECORDS_SYNCED_KEY: String = "replication.records_synced"

    /**
     * Name of the APM trace tag that holds the replication status value associated with the trace.
     */
    const val REPLICATION_STATUS_KEY: String = "replication.status"

    /**
     * Name of the APM trace tag that holds the source definition ID value associated with the trace.
     */
    const val SOURCE_DEFINITION_ID_KEY: String = "source.definition_id"

    /**
     * Name of the APM trace tag that holds the source Docker image value associated with the trace.
     */
    const val SOURCE_DOCKER_IMAGE_KEY: String = "source.docker_image"

    /**
     * Name of the APM trace tag that holds the source ID value associated with the trace.
     */
    const val SOURCE_ID_KEY: String = "source.id"

    /**
     * Name of the APM trace tag that holds the Temporal activity ID value associated with the trace.
     */
    const val TEMPORAL_ACTIVITY_ID_KEY: String = "temporal.activity_id"

    /**
     * Name of the APM trace tag that holds the Temporal run ID value associated with the trace.
     */
    const val TEMPORAL_RUN_ID_KEY: String = "temporal.run_id"

    /**
     * Name of the APM trace tag that holds the Temporal task queue value associated with the trace.
     */
    const val TEMPORAL_TASK_QUEUE_KEY: String = "temporal.task_queue"

    /**
     * Name of the APM trace tag that holds the Temporal workflow ID value associated with the trace.
     */
    const val TEMPORAL_WORKFLOW_ID_KEY: String = "temporal.workflow_id"

    /**
     * Name of the APM trace tag that holds the webhook config ID value associated with the trace.
     */
    const val WEBHOOK_CONFIG_ID_KEY: String = "webhook.config_id"

    /**
     * Name of the APM trace tag that holds the workspace ID value associated with the trace.
     */
    const val WORKSPACE_ID_KEY: String = "workspace.id"

    /**
     * Name of the APM trace tag that holds the organization ID value associated with the trace.
     */
    const val ORGANIZATION_ID_KEY: String = "organization.id"
  }
}
