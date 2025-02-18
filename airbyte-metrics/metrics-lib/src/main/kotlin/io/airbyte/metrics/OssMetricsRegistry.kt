/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

import jakarta.inject.Named
import jakarta.inject.Singleton

enum class OssMetricsRegistry(
  private val application: MetricEmittingApp,
  private val metricName: String,
  private val metricDescription: String,
  private val metricVisibility: MetricVisibility = MetricVisibility.INTERNAL,
) : MetricsRegistry {
  // Public Metrics
  API_REQUESTS(
    application = MetricEmittingApps.SERVER,
    metricName = "api_requests",
    metricDescription = "count of requests to the API",
    metricVisibility = MetricVisibility.PUBLIC,
  ),
  SYNC_GB_MOVED(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "gb_moved",
    metricDescription = "gauge of gigabytes moved during a sync job",
    metricVisibility = MetricVisibility.PUBLIC,
  ),
  SYNC_DURATION(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "syncs_duration",
    metricDescription = "gauge of sync duration in seconds",
    metricVisibility = MetricVisibility.PUBLIC,
  ),
  SYNC_STATUS(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "syncs",
    metricDescription = "count of syncs by status",
    metricVisibility = MetricVisibility.PUBLIC,
  ),

  // Internal Metrics
  ACTIVITY_CHECK_CONNECTION(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_check_connection",
    metricDescription = "increments when we start a check connection activity",
  ),
  ACTIVITY_DISCOVER_CATALOG(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_discover_catalog",
    metricDescription = "increments when we start a discover catalog activity",
  ),
  ACTIVITY_NORMALIZATION(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_normalization",
    metricDescription = "increments when we start a normalization activity",
  ),
  ACTIVITY_NORMALIZATION_SUMMARY_CHECK(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_normalization_summary_check",
    metricDescription = "increments when we start a normalization summary check activity",
  ),
  ACTIVITY_REFRESH_SCHEMA(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_refresh_schema",
    metricDescription = "increments when we start a refresh schema activity",
  ),
  ACTIVITY_REPLICATION(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_replication",
    metricDescription = "increments when we start a replication activity",
  ),
  ACTIVITY_SPEC(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_spec",
    metricDescription = "increments when we start a spec activity",
  ),
  ACTIVITY_SUBMIT_CHECK_DESTINATION_CONNECTION(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_submit_check_destination_connection",
    metricDescription = "increments when we start a submit check connection activity",
  ),
  ACTIVITY_SUBMIT_CHECK_SOURCE_CONNECTION(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_submit_check_source_connection",
    metricDescription = "increments when we start a submit check connection activity",
  ),
  ACTIVITY_WEBHOOK_OPERATION(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_webhook_operation",
    metricDescription = "increments when we start a webhook operation activity",
  ),
  ACTIVITY_FAILURE(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_failure",
    metricDescription = "Generic metric for incrementing when an activity fails. Add activity name to attributes.",
  ),
  ATTEMPTS_CREATED(
    application = MetricEmittingApps.WORKER,
    metricName = "attempt_created",
    metricDescription = "increments when a new attempt is created. one is emitted per attempt",
  ),
  ATTEMPTS_COMPLETED(
    application = MetricEmittingApps.WORKER,
    metricName = "attempt_completed",
    metricDescription = "increments when a new attempt is completed. one is emitted per attempt",
  ),
  ATTEMPT_CREATED_BY_RELEASE_STAGE(
    application = MetricEmittingApps.WORKER,
    metricName = "attempt_created_by_release_stage",
    metricDescription = "increments when a new attempt is created. attempts are double counted as this is tagged by release stage.",
  ),
  ATTEMPT_FAILED_BY_RELEASE_STAGE(
    application = MetricEmittingApps.WORKER,
    metricName = "attempt_failed_by_release_stage",
    metricDescription = "increments when an attempt fails. attempts are double counted as this is tagged by release stage.",
  ),
  ATTEMPT_FAILED_BY_FAILURE_ORIGIN(
    application = MetricEmittingApps.WORKER,
    metricName = "attempt_failed_by_failure_origin",
    metricDescription =
      "increments for every failure origin a failed attempt has. since a failure can have multiple origins, " +
        "a single failure can be counted more than once. tagged by failure origin and failure type.",
  ),
  ATTEMPT_SUCCEEDED_BY_RELEASE_STAGE(
    application = MetricEmittingApps.WORKER,
    metricName = "attempt_succeeded_by_release_stage",
    metricDescription = "increments when an attempts succeeds. attempts are double counted as this is tagged by release stage.",
  ),
  AUTHENTICATION_REQUEST(
    application = MetricEmittingApps.SERVER,
    metricName = "authentication_request",
    metricDescription = "increments when an authentication request is attempted.",
  ),
  COMMAND(
    application = MetricEmittingApps.WORKER,
    metricName = "command",
    metricDescription = "increments when a command is done.",
  ),
  COMMAND_DURATION(
    application = MetricEmittingApps.WORKER,
    metricName = "command_duration",
    metricDescription = "tracks the duration of a command.",
  ),
  COMMAND_STEP(
    application = MetricEmittingApps.WORKER,
    metricName = "command_step",
    metricDescription = "increments when a command step is done.",
  ),
  COMMAND_STEP_DURATION(
    application = MetricEmittingApps.WORKER,
    metricName = "command_step_duration",
    metricDescription = "tracks the duration of a command step.",
  ),
  KEYCLOAK_TOKEN_VALIDATION(
    application = MetricEmittingApps.SERVER,
    metricName = "keycloak_token_validation",
    metricDescription = "increments when a keycloak auth token validation occurs",
  ),
  OIDC_TOKEN_VALIDATION(
    application = MetricEmittingApps.SERVER,
    metricName = "oidc_token_validation",
    metricDescription = "increments when a oidc auth token validation occurs",
  ),
  BREAKING_SCHEMA_CHANGE_DETECTED(
    application = MetricEmittingApps.SERVER,
    metricName = "breaking_change_detected",
    metricDescription = "a breaking schema change has been detected",
  ),
  CRON_JOB_RUN_BY_CRON_TYPE(
    application = MetricEmittingApps.CRON,
    metricName = "cron_jobs_run",
    metricDescription = "number of cron runs by cron type",
  ),
  CONNECTOR_REGISTRY_DEFINITION_PROCESSED(
    application = MetricEmittingApps.CRON, // Actually `cron` or `bootloader` based on which metric client calls the code
    metricName = "connector_registry_definition_processed",
    metricDescription = "increments when a connector registry definition is processed by the ApplyDefinitionsHelper",
  ),
  EST_NUM_METRICS_EMITTED_BY_REPORTER(
    application = MetricEmittingApps.METRICS_REPORTER,
    metricName = "est_num_metrics_emitted_by_reporter",
    metricDescription = "estimated metrics emitted by the reporter in the last interval. this is estimated since the count is not precise.",
  ),
  INCONSISTENT_ACTIVITY_INPUT(
    application = MetricEmittingApps.WORKER,
    metricName = "inconsistent_activity_input",
    metricDescription = "whenever we detect a mismatch between the input and the actual config",
  ),
  JOB_CANCELLED_BY_RELEASE_STAGE(
    application = MetricEmittingApps.WORKER,
    metricName = "job_cancelled_by_release_stage",
    metricDescription = "increments when a job is cancelled. jobs are double counted as this is tagged by release stage.",
  ),
  JOB_CREATED_BY_RELEASE_STAGE(
    application = MetricEmittingApps.WORKER,
    metricName = "job_created_by_release_stage",
    metricDescription = "increments when a new job is created. jobs are double counted as this is tagged by release stage.",
  ),
  JOB_FAILED_BY_RELEASE_STAGE(
    application = MetricEmittingApps.WORKER,
    metricName = "job_failed_by_release_stage",
    metricDescription = "increments when a job fails. jobs are double counted as this is tagged by release stage.",
  ),
  JOB_SUCCEEDED_BY_RELEASE_STAGE(
    application = MetricEmittingApps.WORKER,
    metricName = "job_succeeded_by_release_stage",
    metricDescription = "increments when a job succeeds. jobs are double counted as this is tagged by release stage.",
  ),
  JSON_STRING_LENGTH(
    application = MetricEmittingApps.WORKER,
    metricName = "json_string_length",
    metricDescription = "string length of a raw json string",
  ),
  KUBE_POD_PROCESS_CREATE_TIME_MILLISECS(
    application = MetricEmittingApps.WORKER,
    metricName = "kube_pod_process_create_time_millisecs",
    metricDescription = "time taken to create a new kube pod process",
  ),
  LOG_CLIENT_FILE_LINE_BYTES_RETRIEVED(
    application = MetricEmittingApps.SERVER,
    metricName = "log_client_file_byte_count",
    metricDescription = "the number of bytes retrieved from the job log file(s)",
  ),
  LOG_CLIENT_FILE_LINE_COUNT_RETRIEVED(
    application = MetricEmittingApps.SERVER,
    metricName = "log_client_file_line_count",
    metricDescription = "the number of lines retrieved from the job log file(s)",
  ),
  LOG_CLIENT_FILES_RETRIEVED(
    application = MetricEmittingApps.SERVER,
    metricName = "log_client_files_retrieved",
    metricDescription = "the number of job log files retrieved in one operation",
  ),
  LOG_CLIENT_FILES_RETRIEVAL_TIME_MS(
    application = MetricEmittingApps.SERVER,
    metricName = "log_client_file_retrieval_time_ms",
    metricDescription = "the amount of time spent retrieving a job log in milliseconds",
  ),
  MAPPER_ERROR(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "mapper_error",
    metricDescription = "increments when mapper errors are detected",
  ),
  MISSING_APPLY_SCHEMA_CHANGE_INPUT(
    application = MetricEmittingApps.SERVER,
    metricName = "missing_apply_schema_change_input",
    metricDescription = "one expected value for applying the schema change is missing",
  ),
  NORMALIZATION_IN_DESTINATION_CONTAINER(
    application = MetricEmittingApps.WORKER,
    metricName = "normalization_in_destination_container",
    metricDescription = "increments when normalization is run in the destination container",
  ),
  NORMALIZATION_IN_NORMALIZATION_CONTAINER(
    application = MetricEmittingApps.WORKER,
    metricName = "normalization_in_normalization_container",
    metricDescription = "increments when normalization is run in the normalization container",
  ),
  NUM_ABNORMAL_SCHEDULED_SYNCS_IN_LAST_DAY(
    application = MetricEmittingApps.METRICS_REPORTER,
    metricName = "num_abnormal_scheduled_syncs_last_day",
    metricDescription = "number of abnormal syncs that have skipped at least 1 scheduled run in last day.",
  ),
  NUM_ACTIVE_CONN_PER_WORKSPACE(
    application = MetricEmittingApps.METRICS_REPORTER,
    metricName = "num_active_conn_per_workspace",
    metricDescription = "number of active connections per workspace",
  ),
  NON_BREAKING_SCHEMA_CHANGE_DETECTED(
    application = MetricEmittingApps.SERVER,
    metricName = "non_breaking_change_detected",
    metricDescription = "a non breaking schema change has been detected",
  ),
  NUM_PENDING_JOBS(
    application = MetricEmittingApps.METRICS_REPORTER,
    metricName = "num_pending_jobs",
    metricDescription = "number of pending jobs",
  ),
  NUM_ORPHAN_RUNNING_JOBS(
    application = MetricEmittingApps.METRICS_REPORTER,
    metricName = "num_orphan_running_jobs",
    metricDescription = "number of jobs reported as running that as associated to connection inactive or deprecated",
  ),
  NUM_RUNNING_JOBS(
    application = MetricEmittingApps.METRICS_REPORTER,
    metricName = "num_running_jobs",
    metricDescription = "number of running jobs",
  ),
  NUM_DISTINCT_SCHEMA_VALIDATION_ERRORS_IN_STREAMS(
    application = MetricEmittingApps.WORKER,
    metricName = "record_schema_validation_error",
    metricDescription = "number of validation errors for a given stream",
  ),
  NUM_UNEXPECTED_FIELDS_IN_STREAMS(
    application = MetricEmittingApps.WORKER,
    metricName = "schemas_unexpected_fields",
    metricDescription = "number of unexpected (top level) fields for a given stream",
  ),
  NUM_TOTAL_SCHEDULED_SYNCS_IN_LAST_DAY(
    application = MetricEmittingApps.METRICS_REPORTER,
    metricName = "num_total_scheduled_syncs_last_day",
    metricDescription = "number of total syncs runs in last day.",
  ),
  NUM_UNUSUALLY_LONG_SYNCS(
    application = MetricEmittingApps.METRICS_REPORTER,
    metricName = "num_unusually_long_syncs",
    metricDescription = "number of unusual long syncs compared to their historic performance.",
  ),
  OLDEST_PENDING_JOB_AGE_SECS(
    MetricEmittingApps.METRICS_REPORTER,
    metricName = "oldest_pending_job_age_secs",
    metricDescription = "oldest pending job in seconds",
  ),
  OLDEST_RUNNING_JOB_AGE_SECS(
    MetricEmittingApps.METRICS_REPORTER,
    metricName = "oldest_running_job_age_secs",
    metricDescription = "oldest running job in seconds",
  ),
  ORCHESTRATOR_OUT_OF_MEMORY(
    application = MetricEmittingApps.WORKER,
    metricName = "orchestrator_out_of_memory",
    metricDescription = "orchestrator out of memory error",
  ),
  ORCHESTRATOR_INIT_COPY_FAILURE(
    application = MetricEmittingApps.WORKER,
    metricName = "orchestrator_init_copy_failure",
    metricDescription = "init files failed to copy over to orchestrator",
  ),
  OVERALL_JOB_RUNTIME_IN_LAST_HOUR_BY_TERMINAL_STATE_SECS(
    application = MetricEmittingApps.METRICS_REPORTER,
    metricName = "overall_job_runtime_in_last_hour_by_terminal_state_secs",
    metricDescription =
      "overall job runtime - scheduling and execution for all attempts - for jobs that reach terminal states in the last hour. " +
        "tagged by terminal states.",
  ),
  RUNNING_PODS_FOUND_FOR_CONNECTION_ID(
    application = MetricEmittingApps.WORKER,
    metricName = "running_pods_found_for_connection_id",
    metricDescription = "whether we found pods running for a given connection id when attempting to start a sync for that connection id",
  ),
  REPLICATION_THROUGHPUT_BPS(
    application = MetricEmittingApps.WORKER,
    metricName = "replication_throughput_bps",
    metricDescription = "throughput of replication in bytes per second",
  ),
  REPLICATION_BYTES_SYNCED(
    application = MetricEmittingApps.WORKER,
    metricName = "replication_bytes_synced",
    metricDescription = "number of bytes synced during replication",
  ),
  REPLICATION_RECORDS_SYNCED(
    application = MetricEmittingApps.WORKER,
    metricName = "replication_records_synced",
    metricDescription = "number of records synced during replication",
  ),
  REPLICATION_WORKER_CREATED(
    application = MetricEmittingApps.WORKER,
    metricName = "replication_worker_created",
    metricDescription = "number of replication worker created",
  ),
  REPLICATION_WORKER_EXECUTOR_SHUTDOWN_ERROR(
    application = MetricEmittingApps.WORKER,
    metricName = "replication_worker_executor_shutdown_error",
    metricDescription = "number of failure to shutdown executors",
  ),
  REPLICATION_MADE_PROGRESS(
    application = MetricEmittingApps.WORKER,
    metricName = "replication_made_progress",
    metricDescription = "Count of replication runs that made progress. To be faceted by attributes.",
  ),
  RESET_REQUEST(
    application = MetricEmittingApps.WORKER,
    metricName = "reset_request",
    metricDescription = "number of requested resets",
  ),
  SOURCE_HEARTBEAT_FAILURE(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "source_hearbeat_failure",
    metricDescription = "Fail a replication because the source missed an heartbeat",
  ),
  STATE_BUFFERING(
    application = MetricEmittingApps.WORKER,
    metricName = "state_buffering",
    metricDescription = "number of state messages being buffered before a flush",
  ),
  STATE_COMMIT_ATTEMPT(
    application = MetricEmittingApps.WORKER,
    metricName = "state_commit_attempt",
    metricDescription = "number of attempts to commit states from the orchestrator/workers",
  ),
  STATE_COMMIT_ATTEMPT_FAILED(
    application = MetricEmittingApps.WORKER,
    metricName = "state_commit_attempt_failed",
    metricDescription = "number of failed attempts to commit states from the orchestrator/workers",
  ),
  STATE_COMMIT_ATTEMPT_SUCCESSFUL(
    application = MetricEmittingApps.WORKER,
    metricName = "state_commit_attempt_successful",
    metricDescription = "number of successful attempts to commit states from the orchestrator/workers",
  ),
  STATE_COMMIT_NOT_ATTEMPTED(
    application = MetricEmittingApps.WORKER,
    metricName = "state_commit_not_attempted",
    metricDescription = "number of attempts to commit states dropped due to an early termination",
  ),
  STATE_COMMIT_CLOSE_SUCCESSFUL(
    application = MetricEmittingApps.WORKER,
    metricName = "state_commit_close_successful",
    metricDescription = "number of final to connection exiting with the a successful final state flush",
  ),
  STATS_COMMIT_ATTEMPT(
    application = MetricEmittingApps.WORKER,
    metricName = "stats_commit_attempt",
    metricDescription = "number of attempts to commit stats from the orchestrator/workers",
  ),
  STATS_COMMIT_ATTEMPT_FAILED(
    application = MetricEmittingApps.WORKER,
    metricName = "stats_commit_attempt_failed",
    metricDescription = "number of failed attempts to commit stats from the orchestrator/workers",
  ),
  STATS_COMMIT_ATTEMPT_SUCCESSFUL(
    application = MetricEmittingApps.WORKER,
    metricName = "stats_commit_attempt_successful",
    metricDescription = "number of successful attempts to commit stats from the orchestrator/workers",
  ),
  STATS_COMMIT_NOT_ATTEMPTED(
    application = MetricEmittingApps.WORKER,
    metricName = "stats_commit_not_attempted",
    metricDescription = "number of attempts to commit stats dropped due to an early termination",
  ),
  STATS_COMMIT_CLOSE_SUCCESSFUL(
    application = MetricEmittingApps.WORKER,
    metricName = "stats_commit_close_successful",
    metricDescription = "number of final to connection exiting with the a successful final stats flush",
  ),
  STATE_ERROR_COLLISION_FROM_SOURCE(
    application = MetricEmittingApps.WORKER,
    metricName = "state_error_collision_from_source",
    metricDescription = "number of state collision from source",
  ),
  STATE_ERROR_UNKNOWN_FROM_DESTINATION(
    application = MetricEmittingApps.WORKER,
    metricName = "state_error_unknown_from_destination",
    metricDescription = "number of unknown states from destination",
  ),
  STATE_METRIC_TRACKER_ERROR(
    application = MetricEmittingApps.WORKER,
    metricName = "state_timestamp_metric_tracker_error",
    metricDescription =
      "number of syncs where the state timestamp metric tracker ran out of memory or " +
        "was unable to match destination state message to source state message",
  ),
  STATE_PROCESSED_FROM_DESTINATION(
    application = MetricEmittingApps.WORKER,
    metricName = "state_processed_from_destination",
    metricDescription = "counter for number of state messages received from destination",
  ),
  STATE_PROCESSED_FROM_SOURCE(
    application = MetricEmittingApps.WORKER,
    metricName = "state_processed_from_source",
    metricDescription = "counter for number of state messages received from source",
  ),

  // TEMPORARY, delete after the migration.
  STATS_TRACKER_IMPLEMENTATION(
    application = MetricEmittingApps.WORKER,
    metricName = "stats_tracker_implementation",
    metricDescription = "count the number of syncs by implementation of stats tracker",
  ),
  STREAM_STATS_WRITE_NUM_QUERIES(
    application = MetricEmittingApps.WORKER,
    metricName = "stream_stats_write_num_queries",
    metricDescription = "number of separate queries to update the stream stats table",
  ),
  TEMPORAL_API_TRANSIENT_ERROR_RETRY(
    application = MetricEmittingApps.WORKER,
    metricName = "temporal_api_transient_error_retry",
    metricDescription = "whenever we retry a temporal api call for transient errors",
  ),
  TEMPORAL_WORKFLOW_ATTEMPT(
    application = MetricEmittingApps.WORKER,
    metricName = "temporal_workflow_attempt",
    metricDescription = "count of the number of workflow attempts",
  ),
  TEMPORAL_WORKFLOW_SUCCESS(
    application = MetricEmittingApps.WORKER,
    metricName = "temporal_workflow_success",
    metricDescription = "count of the number of successful workflow syncs.",
  ),
  TEMPORAL_WORKFLOW_FAILURE(
    application = MetricEmittingApps.WORKER,
    metricName = "temporal_workflow_failure",
    metricDescription = "count of the number of workflow failures",
  ),
  SCHEMA_CHANGE_AUTO_PROPAGATED(
    application = MetricEmittingApps.SERVER,
    metricName = "schema_change_auto_propagated",
    metricDescription = "a schema change have been propagated",
  ),
  WORKER_DESTINATION_BUFFER_SIZE(
    application = MetricEmittingApps.WORKER,
    metricName = "worker_destination_buffer_size",
    metricDescription = "the size of the replication worker destination buffer queue",
  ),
  WORKER_DESTINATION_MESSAGE_READ(
    application = MetricEmittingApps.WORKER,
    metricName = "worker_destination_message_read",
    metricDescription = "whenever a message is read from the destination",
  ),
  WORKER_DESTINATION_MESSAGE_SENT(
    application = MetricEmittingApps.WORKER,
    metricName = "worker_destination_message_sent",
    metricDescription = "whenever a message is sent to the destination",
  ),
  WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT(
    application = MetricEmittingApps.WORKER,
    metricName = "notify_end_of_input_timeout",
    metricDescription = "destination call to notify end of input has timed out",
  ),
  WORKER_SOURCE_BUFFER_SIZE(
    application = MetricEmittingApps.WORKER,
    metricName = "worker_source_buffer_size",
    metricDescription = "the size of the replication worker source buffer queue",
  ),
  WORKER_DESTINATION_ACCEPT_TIMEOUT(
    application = MetricEmittingApps.WORKER,
    metricName = "accept_timeout",
    metricDescription = "destination call to accept has timed out",
  ),
  WORKER_SOURCE_MESSAGE_READ(
    application = MetricEmittingApps.WORKER,
    metricName = "worker_source_message_read",
    metricDescription = "whenever a message is read from the source",
  ),
  WORFLOW_UNREACHABLE(
    application = MetricEmittingApps.WORKER,
    metricName = "workflow_unreachable",
    metricDescription = "whenever a workflow is unreachable",
  ),
  WORKFLOWS_HEALED(
    application = MetricEmittingApps.CRON,
    metricName = "workflows_healed",
    metricDescription = "number of workflow the self healing cron healed",
  ),
  WORKLOAD_MONITOR_RUN(
    application = MetricEmittingApps.CRON,
    metricName = "workload_monitor_run",
    metricDescription = "number of cron run for the workload_monitor",
  ),
  WORKLOAD_MONITOR_DONE(
    application = MetricEmittingApps.CRON,
    metricName = "workload_monitor_done",
    metricDescription = "number of cron completed run for the workload_monitor",
  ),
  WORKLOAD_MONITOR_DURATION(
    application = MetricEmittingApps.CRON,
    metricName = "workload_monitor_duration",
    metricDescription = "duration of a run of the workload_monitor",
  ),
  WORKLOADS_CANCEL(
    application = MetricEmittingApps.CRON,
    metricName = "workload_cancel",
    metricDescription = "number of workloads canceled",
  ),
  WORKLOADS_SIGNAL(
    application = MetricEmittingApps.WORKLOAD_API,
    metricName = "workloads_signal",
    metricDescription = "When emitting signal from the workload-api",
  ),
  NOTIFICATIONS_SENT(
    application = MetricEmittingApps.WORKER,
    metricName = "notifications_sent",
    metricDescription = "number of notifications sent",
  ),
  NON_AIRBYTE_MESSAGE_LOG_LINE(
    application = MetricEmittingApps.WORKER,
    metricName = "non_airbyte_message_log_line",
    metricDescription = "non airbyte message log",
  ),
  LINE_SKIPPED_WITH_RECORD(
    application = MetricEmittingApps.WORKER,
    metricName = "line_skipped_with_record",
    metricDescription = "malformated line with a record",
  ),
  LINE_SKIPPED_TOO_LONG(
    application = MetricEmittingApps.WORKER,
    metricName = "line_skipped_too_long",
    metricDescription = "Skip the line because of its size",
  ),
  TOO_LONG_LINES_DISTRIBUTION(
    application = MetricEmittingApps.WORKER,
    metricName = "too_long_lines_distribution",
    metricDescription = "Too long line distribution",
  ),
  WORKLOAD_LAUNCHER_KUBE_ERROR(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_kube_error",
    metricDescription = "Number of kube error in the workload launcher",
  ),
  WORKLOAD_LAUNCHER_POD_SWEEPER_COUNT(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_pods_cleaned",
    metricDescription = "Number of pods cleaned up by the pod sweeper",
  ),
  WORKLOAD_LAUNCHER_KUBE_COPY_SUCCESS_OOM(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_launcher_kube_copy_success_oom",
    metricDescription = "Number of kube cp errors when trying to write the success file in the launcher",
  ),
  JOB_OUTPUT_WRITE(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "job_output_write",
    metricDescription = "Write a job output in the output folder",
  ),
  JOB_OUTPUT_READ(
    application = MetricEmittingApps.WORKER,
    metricName = "job_output_read",
    metricDescription = "Read a job output from the output folder",
  ),
  DESTINATION_DESERIALIZATION_ERROR(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "destination_deserialization_error",
    metricDescription = "When a sync failed with a deserialization error from the destination",
  ),
  HEARTBEAT_TERMINAL_SHUTDOWN(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "heartbeat_terminal_shutdown",
    metricDescription = "When the heartbeat receives a terminal response from the server, and we shut down the orchestrator",
  ),
  HEARTBEAT_CONNECTIVITY_FAILURE_SHUTDOWN(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "heartbeat_connectivity_failure_shutdown",
    metricDescription = "When the heartbeat cannot communicate with the server, and we shut down the orchestrator",
  ),
  SIDECAR_CHECK(
    application = MetricEmittingApps.SIDECAR_ORCHESTRATOR,
    metricName = "sidecar_check",
    metricDescription = "Exit of the connector sidecar",
  ),
  CATALOG_DISCOVERY(
    application = MetricEmittingApps.SIDECAR_ORCHESTRATOR,
    metricName = "catalog_discover",
    metricDescription = "Exit of the connector sidecar",
  ),
  SPEC(
    application = MetricEmittingApps.SIDECAR_ORCHESTRATOR,
    metricName = "spec",
    metricDescription = "Result of the spec operation",
  ),
  ACTIVITY_PAYLOAD_READ_FROM_DOC_STORE(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_payload_read_from_doc_store",
    metricDescription = "An activity payload was read from the doc store.",
  ),
  ACTIVITY_PAYLOAD_WRITTEN_TO_DOC_STORE(
    application = MetricEmittingApps.WORKER,
    metricName = "activity_payload_written_to_doc_store",
    metricDescription = "An activity payload was written to the doc store.",
  ),
  PAYLOAD_SIZE_EXCEEDED(
    application = MetricEmittingApps.WORKER,
    metricName = "payload_size_exceeded",
    metricDescription = "Detected payload size was over 4mb Temporal limit",
  ),
  PAYLOAD_FAILURE_WRITE(
    application = MetricEmittingApps.WORKER,
    metricName = "payload_failure_write",
    metricDescription = "Failure writing the activity payload to storage.",
  ),
  PAYLOAD_FAILURE_READ(
    application = MetricEmittingApps.WORKER,
    metricName = "payload_failure_read",
    metricDescription = "Failure reading the activity payload from storage.",
  ),
  PAYLOAD_VALIDATION_RESULT(
    application = MetricEmittingApps.WORKER,
    metricName = "payload_validation_result",
    metricDescription = "The result of the comparing the payload in object storage to the one passed from temporal.",
  ),
  CREATE_SECRET_DEFAULT_STORE(
    application = MetricEmittingApps.SERVER,
    metricName = "create_secret_default_store",
    metricDescription = "A secret was created in the default configured secret store.",
  ),
  UPDATE_SECRET_DEFAULT_STORE(
    application = MetricEmittingApps.SERVER,
    metricName = "update_secret_default_store",
    metricDescription = "A secret was created in the default configured secret store.",
  ),
  DELETE_SECRET_DEFAULT_STORE(
    application = MetricEmittingApps.SERVER,
    metricName = "delete_secret_default_store",
    metricDescription = "A secret was created in the default configured secret store.",
  ),
  CATALOG_SIZE_VALIDATION_ERROR(
    application = MetricEmittingApps.SERVER,
    metricName = "catalog_size_validation_error",
    metricDescription = "The catalog provided by the user was larger than our limit and rejected.",
  ),
  EXCESSIVE_CATALOG_SIZE(
    application = MetricEmittingApps.SERVER,
    metricName = "excessive_catalog_size",
    metricDescription = "Distribution of input catalog field counts that exceed the configured limit.",
  ),
  NOTIFICATION_SUCCESS(
    application = MetricEmittingApps.SERVER,
    metricName = "notification_success",
    metricDescription = "A notification was successfully sent",
  ),
  NOTIFICATION_FAILED(
    application = MetricEmittingApps.SERVER,
    metricName = "notification_failure",
    metricDescription = "A notification failed to send",
  ),
  REPLICATION_CONTEXT_NOT_INITIALIZED_ERROR(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "replication_context_not_initialized_error",
    metricDescription = "The replication context was not initialized when it was expected to be.",
  ),
  DISCOVER_CATALOG_RUN_TIME(
    application = MetricEmittingApps.WORKER,
    metricName = "discover_catalog_run_time",
    metricDescription = "Time to run a discover catalog before a replication.",
  ),
  REPLICATION_RUN_TIME(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "replication_run_time",
    metricDescription = "Time to run a replication withing a sync.",
  ),
  SYNC_TOTAL_TIME(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "sync_total_time",
    metricDescription = "Time to run a sync workflow.",
  ),
  SYNC_WITH_EMPTY_CATALOG(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "sync_with_empty_catalog",
    metricDescription = "Sync was started with an empty configured catalog.",
  ),
  CONNECTOR_FAILURE_EXIT_VALUE(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "connector_failure_exit_value",
    metricDescription = "Count of failure exit codes produced by a connector.",
  ),
  CONNECTOR_STORAGE_USAGE_MB(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "connector_storage_usage_mb",
    metricDescription = "Storage in mb used by a connector.",
  ),
  CONNECTION_STAGING_STORAGE_USAGE_MB(
    application = MetricEmittingApps.ORCHESTRATOR,
    metricName = "connection_staging_storage_usage_mb",
    metricDescription = "Staging storage in mb used by a connection.",
  ),
  SECRETS_HYDRATION_FAILURE(
    application = MetricEmittingApps.WORKLOAD_INIT,
    metricName = "secrets_hydration_failure",
    metricDescription = "Count of secrets hydration failures.",
  ),
  WORKLOAD_HYDRATION_FETCH_FAILURE(
    application = MetricEmittingApps.WORKLOAD_INIT,
    metricName = "workload_hydration_fetch_failure",
    metricDescription = "Count of failures fetching workload during hydration step.",
  ),
  INIT_FILE_CLIENT_FAILURE(
    application = MetricEmittingApps.WORKLOAD_INIT,
    metricName = "init_file_client_failure",
    metricDescription = "Count of failures prepping files during hydration step.",
  ),
  WORKLOAD_MESSAGE_PUBLISHED(
    application = MetricEmittingApps.WORKLOAD_API,
    metricName = "workload_message_published",
    metricDescription = "Count of workloads published to the queue",
  ),
  WORKLOAD_LAUNCHER_POLLER_STATUS(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_launcher_poller_status",
    metricDescription = "tracks the status of the workload task poller",
  ),
  WORKLOAD_LAUNCHER_REHYDRATE_FAILURE(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_launcher_rehydrate_failure",
    metricDescription = "tracks whenever the launcher rehydrate process failed",
  ),
  WORKLOAD_QUEUE_SIZE(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_queue_size",
    metricDescription = "used to track the queue size launcher does not processes a workload successfully",
  ),
  WORKLOAD_RECEIVED(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_received",
    metricDescription = "increments when the launcher receives a workload from the queue",
  ),
  WORKLOAD_LAUNCH_DURATION(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_launch_duration",
    metricDescription = "tracks the duration of the launch of a workload",
  ),
  WORKLOAD_CLAIM_RESUMED(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_claim_resumed",
    metricDescription = "increments when a claimed workload is retrieved and processed on startup",
  ),
  WORKLOAD_CLAIMED(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_claimed",
    metricDescription = "increments when the launcher claims a workload",
  ),
  WORKLOAD_NOT_CLAIMED(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_not_claimed",
    metricDescription = "increments when the launcher is not able to claim a workload",
  ),
  WORKLOAD_ALREADY_RUNNING(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_already_running",
    metricDescription = "increments when the launcher claims a workload and finds the that there is a job already running for that workload",
  ),
  WORKLOAD_PROCESSED_ON_RESTART(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_claims_rehydrated",
    metricDescription = "increments when the launcher restarts and finds out a workload that was claimed before restart and needs to be processed",
  ),
  WORKLOAD_PROCESSED(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_processed",
    metricDescription = "increments when the launcher processes a workload",
  ),
  WORKLOAD_STAGE_START(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_stage_start",
    metricDescription = "increments when a workload stage starts",
  ),
  WORKLOAD_STAGE_DONE(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_stage_done",
    metricDescription = "increments when a workload stage is done",
  ),
  WORKLOAD_STAGE_DURATION(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_stage_duration",
    metricDescription = "a distribution of the duration of a workload stage",
  ),
  PODS_DELETED_FOR_MUTEX_KEY(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "workload_pods_deleted_for_mutex_key",
    metricDescription = "existing pods for the provided mutex key were found and deleted",
  ),
  PRODUCER_TO_CONSUMER_LATENCY_MS(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "producer_to_consumer_start_latency_ms",
    metricDescription = "the time it takes to produce a message until it is consumed",
  ),
  PRODUCER_TO_POD_STARTED_LATENCY_MS(
    application = MetricEmittingApps.WORKLOAD_LAUNCHER,
    metricName = "producer_to_pod_started_latency_ms",
    metricDescription = "the time it takes to produce a message until it is fully processed",
  ),
  ;

  override fun getApplication(): MetricEmittingApp = application

  override fun getMetricName(): String = metricName

  override fun getMetricDescription(): String = metricDescription

  override fun getMetricVisibility(): MetricVisibility = metricVisibility
}

@Singleton
@Named("ossMetricsResolver")
class OssMetricsResolver : MetricsResolver {
  override fun resolve(metricId: String): MetricsRegistry? = OssMetricsRegistry.entries.find { it.getMetricName() == metricId }
}
