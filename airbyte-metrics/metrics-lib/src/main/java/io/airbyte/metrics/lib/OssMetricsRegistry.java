/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

import com.google.api.client.util.Preconditions;

/**
 * Enum source of truth of all Airbyte metrics. Each enum value represent a metric and is linked to
 * an application and contains a description to make it easier to understand.
 * <p>
 * Each object of the enum actually represent a metric, so the Registry name is misleading. The
 * reason 'Registry' is in the name is to emphasize this enum's purpose as a source of truth for all
 * metrics. This also helps code readability i.e. AirbyteMetricsRegistry.metricA.
 * <p>
 * Metric Name Convention (adapted from
 * https://docs.datadoghq.com/developers/guide/what-best-practices-are-recommended-for-naming-metrics-and-tags/):
 * <p>
 * - Use lowercase. Metric names are case-sensitive.
 * <p>
 * - Use underscore to delimit names with multiple words.
 * <p>
 * - No spaces. This makes the metric confusing to read.
 * <p>
 * - Avoid numbers. This makes the metric confusing to read. Numbers should only be used as a
 * <p>
 * - Add units at name end if applicable. This is especially relevant for time units.
 * <p>
 * - Include the time period in the name if the metric is meant to be run at a certain interval.
 * <p>
 * Note: These names are used as metric name prefixes. Changing these names will affect
 * dashboard/alerts and our public Datadog integration. Please consult the platform teams if unsure.
 */
public enum OssMetricsRegistry implements MetricsRegistry {

  ACTIVITY_CHECK_CONNECTION(
      MetricEmittingApps.WORKER,
      "activity_check_connection",
      "increments when we start a check connection activity"),
  ACTIVITY_DISCOVER_CATALOG(
      MetricEmittingApps.WORKER,
      "activity_discover_catalog",
      "increments when we start a discover catalog activity"),
  ACTIVITY_NORMALIZATION(
      MetricEmittingApps.WORKER,
      "activity_normalization",
      "increments when we start a normalization activity"),
  ACTIVITY_NORMALIZATION_SUMMARY_CHECK(
      MetricEmittingApps.WORKER,
      "activity_normalization_summary_check",
      "increments when we start a normalization summary check activity"),
  ACTIVITY_REFRESH_SCHEMA(
      MetricEmittingApps.WORKER,
      "activity_refresh_schema",
      "increments when we start a refresh schema activity"),
  ACTIVITY_REPLICATION(
      MetricEmittingApps.WORKER,
      "activity_replication",
      "increments when we start a replication activity"),
  ACTIVITY_SPEC(
      MetricEmittingApps.WORKER,
      "activity_spec",
      "increments when we start a spec activity"),
  ACTIVITY_SUBMIT_CHECK_DESTINATION_CONNECTION(
      MetricEmittingApps.WORKER,
      "activity_submit_check_destination_connection",
      "increments when we start a submit check connection activity"),
  ACTIVITY_SUBMIT_CHECK_SOURCE_CONNECTION(
      MetricEmittingApps.WORKER,
      "activity_submit_check_source_connection",
      "increments when we start a submit check connection activity"),
  ACTIVITY_WEBHOOK_OPERATION(
      MetricEmittingApps.WORKER,
      "activity_webhook_operation",
      "increments when we start a webhook operation activity"),
  ACTIVITY_FAILURE(
      MetricEmittingApps.WORKER,
      "activity_failure",
      "Generic metric for incrementing when an activity fails. Add activity name to attributes."),

  ATTEMPTS_CREATED(
      MetricEmittingApps.WORKER,
      "attempt_created",
      "increments when a new attempt is created. one is emitted per attempt"),
  ATTEMPTS_COMPLETED(
      MetricEmittingApps.WORKER,
      "attempt_completed",
      "increments when a new attempt is completed. one is emitted per attempt"),
  ATTEMPT_CREATED_BY_RELEASE_STAGE(
      MetricEmittingApps.WORKER,
      "attempt_created_by_release_stage",
      "increments when a new attempt is created. attempts are double counted as this is tagged by release stage."),
  ATTEMPT_FAILED_BY_RELEASE_STAGE(
      MetricEmittingApps.WORKER,
      "attempt_failed_by_release_stage",
      "increments when an attempt fails. attempts are double counted as this is tagged by release stage."),
  ATTEMPT_FAILED_BY_FAILURE_ORIGIN(
      MetricEmittingApps.WORKER,
      "attempt_failed_by_failure_origin",
      "increments for every failure origin a failed attempt has. since a failure can have multiple origins, "
          + "a single failure can be counted more than once. tagged by failure origin and failure type."),
  ATTEMPT_SUCCEEDED_BY_RELEASE_STAGE(
      MetricEmittingApps.WORKER,
      "attempt_succeeded_by_release_stage",
      "increments when an attempts succeeds. attempts are double counted as this is tagged by release stage."),
  AUTHENTICATION_REQUEST(
      MetricEmittingApps.SERVER,
      "authentication_request",
      "increments when an authentication request is attempted."),
  COMMAND(
      MetricEmittingApps.WORKER,
      "command",
      "increments when a command is done."),
  COMMAND_DURATION(
      MetricEmittingApps.WORKER,
      "command_duration",
      "tracks the duration of a command."),
  COMMAND_STEP(
      MetricEmittingApps.WORKER,
      "command_step",
      "increments when a command step is done."),
  COMMAND_STEP_DURATION(
      MetricEmittingApps.WORKER,
      "command_step_duration",
      "tracks the duration of a command step."),

  KEYCLOAK_TOKEN_VALIDATION(
      MetricEmittingApps.SERVER,
      "keycloak_token_validation",
      "increments when a keycloak auth token validation occurs"),

  BREAKING_SCHEMA_CHANGE_DETECTED(MetricEmittingApps.SERVER,
      "breaking_change_detected",
      "a breaking schema change has been detected"),
  CRON_JOB_RUN_BY_CRON_TYPE(MetricEmittingApps.CRON,
      "cron_jobs_run",
      "number of cron runs by cron type"),
  CONNECTOR_REGISTRY_DEFINITION_PROCESSED(
      MetricEmittingApps.CRON, // Actually `cron` or `bootloader` based on which metric client calls the code
      "connector_registry_definition_processed",
      "increments when a connector registry definition is processed by the ApplyDefinitionsHelper"),
  EST_NUM_METRICS_EMITTED_BY_REPORTER(
      MetricEmittingApps.METRICS_REPORTER,
      "est_num_metrics_emitted_by_reporter",
      "estimated metrics emitted by the reporter in the last interval. this is estimated since the count is not precise."),
  INCONSISTENT_ACTIVITY_INPUT(MetricEmittingApps.WORKER,
      "inconsistent_activity_input",
      "whenever we detect a mismatch between the input and the actual config"),
  JOB_CANCELLED_BY_RELEASE_STAGE(
      MetricEmittingApps.WORKER,
      "job_cancelled_by_release_stage",
      "increments when a job is cancelled. jobs are double counted as this is tagged by release stage."),
  JOB_CREATED_BY_RELEASE_STAGE(
      MetricEmittingApps.WORKER,
      "job_created_by_release_stage",
      "increments when a new job is created. jobs are double counted as this is tagged by release stage."),
  JOB_FAILED_BY_RELEASE_STAGE(
      MetricEmittingApps.WORKER,
      "job_failed_by_release_stage",
      "increments when a job fails. jobs are double counted as this is tagged by release stage."),
  JOB_SUCCEEDED_BY_RELEASE_STAGE(
      MetricEmittingApps.WORKER,
      "job_succeeded_by_release_stage",
      "increments when a job succeeds. jobs are double counted as this is tagged by release stage."),
  JSON_STRING_LENGTH(
      MetricEmittingApps.WORKER,
      "json_string_length",
      "string length of a raw json string"),
  KUBE_POD_PROCESS_CREATE_TIME_MILLISECS(
      MetricEmittingApps.WORKER,
      "kube_pod_process_create_time_millisecs",
      "time taken to create a new kube pod process"),
  LOG_CLIENT_FILE_LINE_BYTES_RETRIEVED(MetricEmittingApps.SERVER,
      "log_client_file_byte_count",
      "the number of bytes retrieved from the job log file(s)"),
  LOG_CLIENT_FILE_LINE_COUNT_RETRIEVED(MetricEmittingApps.SERVER,
      "log_client_file_line_count",
      "the number of lines retrieved from the job log file(s)"),
  LOG_CLIENT_FILES_RETRIEVED(MetricEmittingApps.SERVER,
      "log_client_files_retrieved",
      "the number of job log files retrieved in one operation"),
  LOG_CLIENT_FILES_RETRIEVAL_TIME_MS(MetricEmittingApps.SERVER,
      "log_client_file_retrieval_time_ms",
      "the amount of time spent retrieving a job log in milliseconds"),
  MAPPER_ERROR(MetricEmittingApps.ORCHESTRATOR,
      "mapper_error",
      "increments when mapper errors are detected"),
  MISSING_APPLY_SCHEMA_CHANGE_INPUT(MetricEmittingApps.SERVER,
      "missing_apply_schema_change_input",
      "one expected value for applying the schema change is missing"),
  NORMALIZATION_IN_DESTINATION_CONTAINER(
      MetricEmittingApps.WORKER,
      "normalization_in_destination_container",
      "increments when normalization is run in the destination container"),
  NORMALIZATION_IN_NORMALIZATION_CONTAINER(
      MetricEmittingApps.WORKER,
      "normalization_in_normalization_container",
      "increments when normalization is run in the normalization container"),
  NUM_ABNORMAL_SCHEDULED_SYNCS_IN_LAST_DAY(
      MetricEmittingApps.METRICS_REPORTER,
      "num_abnormal_scheduled_syncs_last_day",
      "number of abnormal syncs that have skipped at least 1 scheduled run in last day."),
  NUM_ACTIVE_CONN_PER_WORKSPACE(
      MetricEmittingApps.METRICS_REPORTER,
      "num_active_conn_per_workspace",
      "number of active connections per workspace"),
  NON_BREAKING_SCHEMA_CHANGE_DETECTED(MetricEmittingApps.SERVER,
      "non_breaking_change_detected",
      "a non breaking schema change has been detected"),
  NUM_PENDING_JOBS(
      MetricEmittingApps.METRICS_REPORTER,
      "num_pending_jobs",
      "number of pending jobs"),
  NUM_ORPHAN_RUNNING_JOBS(
      MetricEmittingApps.METRICS_REPORTER,
      "num_orphan_running_jobs",
      "number of jobs reported as running that as associated to connection inactive or deprecated"),
  NUM_RUNNING_JOBS(
      MetricEmittingApps.METRICS_REPORTER,
      "num_running_jobs",
      "number of running jobs"),
  NUM_DISTINCT_SCHEMA_VALIDATION_ERRORS_IN_STREAMS(MetricEmittingApps.WORKER,
      "record_schema_validation_error",
      "number of validation errors for a given stream"),
  NUM_UNEXPECTED_FIELDS_IN_STREAMS(MetricEmittingApps.WORKER,
      "schemas_unexpected_fields",
      "number of unexpected (top level) fields for a given stream"),
  NUM_TOTAL_SCHEDULED_SYNCS_IN_LAST_DAY(
      MetricEmittingApps.METRICS_REPORTER,
      "num_total_scheduled_syncs_last_day",
      "number of total syncs runs in last day."),
  NUM_UNUSUALLY_LONG_SYNCS(
      MetricEmittingApps.METRICS_REPORTER,
      "num_unusually_long_syncs",
      "number of unusual long syncs compared to their historic performance."),
  OLDEST_PENDING_JOB_AGE_SECS(MetricEmittingApps.METRICS_REPORTER,
      "oldest_pending_job_age_secs",
      "oldest pending job in seconds"),
  OLDEST_RUNNING_JOB_AGE_SECS(MetricEmittingApps.METRICS_REPORTER,
      "oldest_running_job_age_secs",
      "oldest running job in seconds"),

  ORCHESTRATOR_OUT_OF_MEMORY(MetricEmittingApps.WORKER,
      "orchestrator_out_of_memory",
      "orchestrator out of memory error"),
  ORCHESTRATOR_INIT_COPY_FAILURE(MetricEmittingApps.WORKER,
      "orchestrator_init_copy_failure",
      "init files failed to copy over to orchestrator"),

  OVERALL_JOB_RUNTIME_IN_LAST_HOUR_BY_TERMINAL_STATE_SECS(MetricEmittingApps.METRICS_REPORTER,
      "overall_job_runtime_in_last_hour_by_terminal_state_secs",
      "overall job runtime - scheduling and execution for all attempts - for jobs that reach terminal states in the last hour. "
          + "tagged by terminal states."),

  RUNNING_PODS_FOUND_FOR_CONNECTION_ID(MetricEmittingApps.WORKER,
      "running_pods_found_for_connection_id",
      "whether we found pods running for a given connection id when attempting to start a sync for that connection id"),

  REPLICATION_THROUGHPUT_BPS(MetricEmittingApps.WORKER,
      "replication_throughput_bps",
      "throughput of replication in bytes per second"),
  REPLICATION_BYTES_SYNCED(MetricEmittingApps.WORKER,
      "replication_bytes_synced",
      "number of bytes synced during replication"),
  REPLICATION_RECORDS_SYNCED(MetricEmittingApps.WORKER,
      "replication_records_synced",
      "number of records synced during replication"),
  REPLICATION_WORKER_CREATED(MetricEmittingApps.WORKER,
      "replication_worker_created",
      "number of replication worker created"),
  REPLICATION_WORKER_EXECUTOR_SHUTDOWN_ERROR(MetricEmittingApps.WORKER,
      "replication_worker_executor_shutdown_error",
      "number of failure to shutdown executors"),
  REPLICATION_MADE_PROGRESS(MetricEmittingApps.WORKER,
      "replication_made_progress",
      "Count of replication runs that made progress. To be faceted by attributes."),
  RESET_REQUEST(MetricEmittingApps.WORKER,
      "reset_request",
      "number of requested resets"),
  SOURCE_HEARTBEAT_FAILURE(MetricEmittingApps.ORCHESTRATOR,
      "source_hearbeat_failure",
      "Fail a replication because the source missed an heartbeat"),
  STATE_BUFFERING(MetricEmittingApps.WORKER,
      "state_buffering",
      "number of state messages being buffered before a flush"),
  STATE_COMMIT_ATTEMPT(MetricEmittingApps.WORKER,
      "state_commit_attempt",
      "number of attempts to commit states from the orchestrator/workers"),
  STATE_COMMIT_ATTEMPT_FAILED(MetricEmittingApps.WORKER,
      "state_commit_attempt_failed",
      "number of failed attempts to commit states from the orchestrator/workers"),
  STATE_COMMIT_ATTEMPT_SUCCESSFUL(MetricEmittingApps.WORKER,
      "state_commit_attempt_successful",
      "number of successful attempts to commit states from the orchestrator/workers"),
  STATE_COMMIT_NOT_ATTEMPTED(MetricEmittingApps.WORKER,
      "state_commit_not_attempted",
      "number of attempts to commit states dropped due to an early termination"),
  STATE_COMMIT_CLOSE_SUCCESSFUL(MetricEmittingApps.WORKER,
      "state_commit_close_successful",
      "number of final to connection exiting with the a successful final state flush"),
  STATS_COMMIT_ATTEMPT(MetricEmittingApps.WORKER,
      "stats_commit_attempt",
      "number of attempts to commit stats from the orchestrator/workers"),
  STATS_COMMIT_ATTEMPT_FAILED(MetricEmittingApps.WORKER,
      "stats_commit_attempt_failed",
      "number of failed attempts to commit stats from the orchestrator/workers"),
  STATS_COMMIT_ATTEMPT_SUCCESSFUL(MetricEmittingApps.WORKER,
      "stats_commit_attempt_successful",
      "number of successful attempts to commit stats from the orchestrator/workers"),

  STATS_COMMIT_NOT_ATTEMPTED(MetricEmittingApps.WORKER,
      "stats_commit_not_attempted",
      "number of attempts to commit stats dropped due to an early termination"),
  STATS_COMMIT_CLOSE_SUCCESSFUL(MetricEmittingApps.WORKER,
      "stats_commit_close_successful",
      "number of final to connection exiting with the a successful final stats flush"),
  STATE_ERROR_COLLISION_FROM_SOURCE(MetricEmittingApps.WORKER,
      "state_error_collision_from_source",
      "number of state collision from source"),
  STATE_ERROR_UNKNOWN_FROM_DESTINATION(MetricEmittingApps.WORKER,
      "state_error_unknown_from_destination",
      "number of unknown states from destination"),
  STATE_METRIC_TRACKER_ERROR(MetricEmittingApps.WORKER,
      "state_timestamp_metric_tracker_error",
      "number of syncs where the state timestamp metric tracker ran out of memory or "
          + "was unable to match destination state message to source state message"),
  STATE_PROCESSED_FROM_DESTINATION(MetricEmittingApps.WORKER,
      "state_processed_from_destination",
      "counter for number of state messages received from destination"),
  STATE_PROCESSED_FROM_SOURCE(MetricEmittingApps.WORKER,
      "state_processed_from_source",
      "counter for number of state messages received from source"),
  // TEMPORARY, delete after the migration.
  STATS_TRACKER_IMPLEMENTATION(MetricEmittingApps.WORKER,
      "stats_tracker_implementation",
      "count the number of syncs by implementation of stats tracker"),
  STREAM_STATS_WRITE_NUM_QUERIES(MetricEmittingApps.WORKER,
      "stream_stats_write_num_queries",
      "number of separate queries to update the stream stats table"),
  TEMPORAL_API_TRANSIENT_ERROR_RETRY(MetricEmittingApps.WORKER,
      "temporal_api_transient_error_retry",
      "whenever we retry a temporal api call for transient errors"),
  TEMPORAL_WORKFLOW_ATTEMPT(MetricEmittingApps.WORKER,
      "temporal_workflow_attempt",
      "count of the number of workflow attempts"),
  TEMPORAL_WORKFLOW_SUCCESS(MetricEmittingApps.WORKER,
      "temporal_workflow_success",
      "count of the number of successful workflow syncs."),
  TEMPORAL_WORKFLOW_FAILURE(MetricEmittingApps.WORKER,
      "temporal_workflow_failure",
      "count of the number of workflow failures"),
  SCHEMA_CHANGE_AUTO_PROPAGATED(MetricEmittingApps.SERVER,
      "schema_change_auto_propagated",
      "a schema change have been propagated"),
  WORKER_DESTINATION_BUFFER_SIZE(MetricEmittingApps.WORKER,
      "worker_destination_buffer_size",
      "the size of the replication worker destination buffer queue"),

  WORKER_DESTINATION_MESSAGE_READ(MetricEmittingApps.WORKER,
      "worker_destination_message_read",
      "whenever a message is read from the destination"),

  WORKER_DESTINATION_MESSAGE_SENT(MetricEmittingApps.WORKER,
      "worker_destination_message_sent",
      "whenever a message is sent to the destination"),

  WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT(MetricEmittingApps.WORKER,
      "notify_end_of_input_timeout",
      "destination call to notify end of input has timed out"),

  WORKER_SOURCE_BUFFER_SIZE(MetricEmittingApps.WORKER,
      "worker_source_buffer_size",
      "the size of the replication worker source buffer queue"),

  WORKER_DESTINATION_ACCEPT_TIMEOUT(MetricEmittingApps.WORKER,
      "accept_timeout",
      "destination call to accept has timed out"),

  WORKER_SOURCE_MESSAGE_READ(MetricEmittingApps.WORKER,
      "worker_source_message_read",
      "whenever a message is read from the source"),
  WORFLOW_UNREACHABLE(MetricEmittingApps.WORKER,
      "workflow_unreachable",
      "whenever a workflow is unreachable"),
  WORKFLOWS_HEALED(MetricEmittingApps.CRON,
      "workflows_healed",
      "number of workflow the self healing cron healed"),
  WORKLOAD_MONITOR_RUN(MetricEmittingApps.CRON,
      "workload_monitor_run",
      "number of cron run for the workload_monitor"),
  WORKLOAD_MONITOR_DONE(MetricEmittingApps.CRON,
      "workload_monitor_done",
      "number of cron completed run for the workload_monitor"),
  WORKLOAD_MONITOR_DURATION(MetricEmittingApps.CRON,
      "workload_monitor_duration",
      "duration of a run of the workload_monitor"),
  WORKLOADS_CANCEL(MetricEmittingApps.CRON,
      "workload_cancel",
      "number of workloads canceled"),
  WORKLOADS_SIGNAL(MetricEmittingApps.WORKLOAD_API,
      "workloads_signal",
      "When emitting signal from the workload-api"),
  NOTIFICATIONS_SENT(MetricEmittingApps.WORKER,
      "notifications_sent",
      "number of notifications sent"),
  NON_AIRBYTE_MESSAGE_LOG_LINE(MetricEmittingApps.WORKER,
      "non_airbyte_message_log_line",
      "non airbyte message log"),
  LINE_SKIPPED_WITH_RECORD(MetricEmittingApps.WORKER,
      "line_skipped_with_record",
      "malformated line with a record"),
  LINE_SKIPPED_TOO_LONG(MetricEmittingApps.WORKER,
      "line_skipped_too_long",
      "Skip the line because of its size"),
  TOO_LONG_LINES_DISTRIBUTION(MetricEmittingApps.WORKER,
      "too_long_lines_distribution",
      "Too long line distribution"),
  WORKLOAD_LAUNCHER_KUBE_ERROR(MetricEmittingApps.WORKLOAD_LAUNCHER,
      "workload_kube_error",
      "Number of kube error in the workload launcher"),
  WORKLOAD_LAUNCHER_KUBE_COPY_SUCCESS_OOM(MetricEmittingApps.WORKLOAD_LAUNCHER,
      "workload_launcher_kube_copy_success_oom",
      "Number of kube cp errors when trying to write the success file in the launcher"),
  JOB_OUTPUT_WRITE(MetricEmittingApps.ORCHESTRATOR,
      "job_output_write",
      "Write a job output in the output folder"),
  JOB_OUTPUT_READ(MetricEmittingApps.WORKER,
      "job_output_read",
      "Read a job output from the output folder"),

  DESTINATION_DESERIALIZATION_ERROR(MetricEmittingApps.ORCHESTRATOR,
      "destination_deserialization_error",
      "When a sync failed with a deserialization error from the destination"),

  HEARTBEAT_TERMINAL_SHUTDOWN(MetricEmittingApps.ORCHESTRATOR,
      "heartbeat_terminal_shutdown",
      "When the heartbeat receives a terminal response from the server, and we shut down the orchestrator"),

  HEARTBEAT_CONNECTIVITY_FAILURE_SHUTDOWN(MetricEmittingApps.ORCHESTRATOR,
      "heartbeat_connectivity_failure_shutdown",
      "When the heartbeat cannot communicate with the server, and we shut down the orchestrator"),

  SIDECAR_CHECK(MetricEmittingApps.SIDECAR_ORCHESTRATOR,
      "sidecar_check",
      "Exit of the connector sidecar"),

  CATALOG_DISCOVERY(MetricEmittingApps.SIDECAR_ORCHESTRATOR,
      "catalog_discover",
      "Exit of the connector sidecar"),

  SPEC(MetricEmittingApps.SIDECAR_ORCHESTRATOR,
      "spec",
      "Result of the spec operation"),

  ACTIVITY_PAYLOAD_READ_FROM_DOC_STORE(MetricEmittingApps.WORKER,
      "activity_payload_read_from_doc_store",
      "An activity payload was read from the doc store."),

  ACTIVITY_PAYLOAD_WRITTEN_TO_DOC_STORE(MetricEmittingApps.WORKER,
      "activity_payload_written_to_doc_store",
      "An activity payload was written to the doc store."),

  PAYLOAD_SIZE_EXCEEDED(MetricEmittingApps.WORKER,
      "payload_size_exceeded",
      "Detected payload size was over 4mb Temporal limit"),

  PAYLOAD_FAILURE_WRITE(MetricEmittingApps.WORKER,
      "payload_failure_write",
      "Failure writing the activity payload to storage."),

  PAYLOAD_FAILURE_READ(MetricEmittingApps.WORKER,
      "payload_failure_read",
      "Failure reading the activity payload from storage."),

  PAYLOAD_VALIDATION_RESULT(MetricEmittingApps.WORKER,
      "payload_validation_result",
      "The result of the comparing the payload in object storage to the one passed from temporal."),

  CREATE_SECRET_DEFAULT_STORE(MetricEmittingApps.SERVER,
      "create_secret_default_store",
      "A secret was created in the default configured secret store."),
  UPDATE_SECRET_DEFAULT_STORE(MetricEmittingApps.SERVER,
      "update_secret_default_store",
      "A secret was created in the default configured secret store."),
  DELETE_SECRET_DEFAULT_STORE(MetricEmittingApps.SERVER,
      "delete_secret_default_store",
      "A secret was created in the default configured secret store."),

  CATALOG_SIZE_VALIDATION_ERROR(MetricEmittingApps.SERVER,
      "catalog_size_validation_error",
      "The catalog provided by the user was larger than our limit and rejected."),

  EXCESSIVE_CATALOG_SIZE(MetricEmittingApps.SERVER,
      "excessive_catalog_size",
      "Distribution of input catalog field counts that exceed the configured limit."),

  NOTIFICATION_SUCCESS(MetricEmittingApps.SERVER,
      "notification_success",
      "A notification was successfully sent"),

  NOTIFICATION_FAILED(MetricEmittingApps.SERVER,
      "notification_failure",
      "A notification failed to send"),

  REPLICATION_CONTEXT_NOT_INITIALIZED_ERROR(MetricEmittingApps.ORCHESTRATOR,
      "replication_context_not_initialized_error",
      "The replication context was not initialized when it was expected to be."),

  DISCOVER_CATALOG_RUN_TIME(MetricEmittingApps.WORKER,
      "discover_catalog_run_time",
      "Time to run a discover catalog before a replication."),

  REPLICATION_RUN_TIME(MetricEmittingApps.ORCHESTRATOR,
      "replication_run_time",
      "Time to run a replication withing a sync."),

  SYNC_TOTAL_TIME(MetricEmittingApps.ORCHESTRATOR,
      "sync_total_time",
      "Time to run a sync workflow."),

  SYNC_WITH_EMPTY_CATALOG(MetricEmittingApps.ORCHESTRATOR,
      "sync_with_empty_catalog",
      "Sync was started with an empty configured catalog."),

  CONNECTOR_FAILURE_EXIT_VALUE(MetricEmittingApps.ORCHESTRATOR,
      "connector_failure_exit_value",
      "Count of failure exit codes produced by a connector."),

  CONNECTOR_STORAGE_USAGE_MB(MetricEmittingApps.ORCHESTRATOR,
      "connector_storage_usage_mb",
      "Storage in mb used by a connector."),

  CONNECTION_STAGING_STORAGE_USAGE_MB(MetricEmittingApps.ORCHESTRATOR,
      "connection_staging_storage_usage_mb",
      "Staging storage in mb used by a connection."),

  SECRETS_HYDRATION_FAILURE(MetricEmittingApps.WORKLOAD_INIT,
      "secrets_hydration_failure",
      "Count of secrets hydration failures."),

  WORKLOAD_HYDRATION_FETCH_FAILURE(MetricEmittingApps.WORKLOAD_INIT,
      "workload_hydration_fetch_failure",
      "Count of failures fetching workload during hydration step."),

  INIT_FILE_CLIENT_FAILURE(MetricEmittingApps.WORKLOAD_INIT,
      "init_file_client_failure",
      "Count of failures prepping files during hydration step.");

  private final MetricEmittingApp application;
  private final String metricName;
  private final String metricDescription;

  OssMetricsRegistry(final MetricEmittingApp application,
                     final String metricName,
                     final String metricDescription) {
    Preconditions.checkNotNull(metricDescription);
    Preconditions.checkNotNull(application);

    this.application = application;
    this.metricName = metricName;
    this.metricDescription = metricDescription;
  }

  @Override
  public MetricEmittingApp getApplication() {
    return application;
  }

  @Override
  public String getMetricName() {
    return metricName;
  }

  @Override
  public String getMetricDescription() {
    return metricDescription;
  }

}
