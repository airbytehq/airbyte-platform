/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

import io.airbyte.config.FailureReason
import io.airbyte.config.ReleaseStage

/**
 * Keep track of all metric tags.
 */
object MetricTags {
  const val ACTIVITY_NAME: String = "activity_name"
  const val ACTIVITY_METHOD: String = "activity_method"
  const val ATTEMPT_NUMBER: String = "attempt_number" // 0|1|2|3
  const val ATTEMPT_OUTCOME: String = "attempt_outcome" // succeeded|failed
  const val ATTEMPT_QUEUE: String = "attempt_queue"
  const val AUTHENTICATION_RESPONSE: String = "authentication_response"
  const val AUTHENTICATION_RESPONSE_FAILURE_REASON: String = "authentication_response_failure_reason"
  const val AUTHENTICATION_REQUEST_URI_ATTRIBUTE_KEY: String = "request_uri"
  const val CANCELLATION_SOURCE: String = "cancellation_source"
  const val COMMAND: String = "command"
  const val COMMAND_STEP: String = "command_step"
  const val CONFIG_TYPES: String = "config_types"
  const val CONNECTION_ID: String = "connection_id"
  const val CONNECTION_IDS: String = "connection_ids"
  const val CONNECTOR_IMAGE: String = "connector_image"
  const val CONNECTOR_TYPE: String = "connector_type"
  const val CRON_TYPE: String = "cron_type"
  const val CRUD_OPERATION: String = "crud_operation"
  const val DESTINATION_DEFINITION_ID: String = "destination_definition_id"
  const val DESTINATION_ID: String = "destination_id"
  const val DESTINATION_IMAGE: String = "destination_image"
  const val DESTINATION_IMAGE_IS_DEFAULT: String = "destination_image_is_default"
  const val ERROR: String = "error"
  const val EXPIRE_SECRET: String = "expire_secret"
  const val FAILURE: String = "failure"
  const val FAILURE_CAUSE: String = "failure_cause"
  const val FAILURE_ORIGIN: String = "failure_origin"
  const val FAILURE_TYPE: String = "failure_type"
  const val INTERNAL_MESSAGE: String = "internal_message"
  const val EXTERNAL_MESSAGE: String = "external_message"
  const val GEOGRAPHY: String = "geography"
  const val IMPLEMENTATION: String = "implementation"
  const val IS_CUSTOM_CONNECTOR_SYNC: String = "is_custom_connector_sync"
  const val IS_RESET: String = "is_reset"
  const val JOB_ID: String = "job_id"
  const val JOB_STATUS: String = "job_status"
  const val KILLED: String = "killed"
  const val MADE_PROGRESS: String = "made_progress"

  // the release stage of the highest release connector in the sync (GA > Beta > Alpha)
  const val MAX_CONNECTOR_RELEASE_STATE: String = "max_connector_release_stage"
  const val MESSAGE_TYPE: String = "message_type"

  // the release stage of the lowest release stage connector in the sync (GA > Beta > Alpha)
  const val MIN_CONNECTOR_RELEASE_STATE: String = "min_connector_release_stage"
  const val NOTIFICATION_TRIGGER: String = "notification_trigger"
  const val NOTIFICATION_CLIENT: String = "notification_client"
  const val RELEASE_STAGE: String = "release_stage"
  const val SECRET_COORDINATES_UPDATED: String = "secret_coordinates_updated"
  const val SOURCE_ID: String = "source_id"
  const val SOURCE_DEFINITION_ID: String = "source_definition_id"
  const val SECRET_STORAGE_ID: String = "secret_storage_id"
  const val SOURCE_IMAGE: String = "source_image"
  const val SOURCE_IMAGE_IS_DEFAULT: String = "source_image_is_default"
  const val SKIPPED = "skipped"
  const val STATUS: String = "status"
  const val SUCCESS: String = "success"
  const val WILL_RETRY: String = "will_retry"
  const val WORKSPACE_ID: String = "workspace_id"
  const val WORKFLOW_TYPE: String = "workflow_type"
  const val WORKLOAD_TYPE: String = "workload_type"
  const val UNKNOWN: String = "unknown"
  const val USER_TYPE: String = "user_type" // real user, service account, data plane user, etc
  const val CLIENT_ID: String = "client_id"
  const val LOG_CLIENT_TYPE: String = "log_client_type"
  const val MALFORMED_LOG_LINE_LENGTH: String = "malformed_log_line_length"
  const val PROMPT_LENGTH: String = "prompt_length"

  // payload metric tags
  const val URI_NULL: String = "uri_null"
  const val URI_ID: String = "uri_id"
  const val URI_VERSION: String = "uri_version"
  const val PAYLOAD_NAME: String = "payload_name"
  const val IS_MATCH: String = "is_match"
  const val IS_MISS: String = "is_miss"
  const val TASK_QUEUE: String = "task_queue"

  // workload tags
  const val DATA_PLANE_ID_TAG: String = "data_plane_id"
  const val DATA_PLANE_NAME_TAG: String = "data_plane_name"
  const val DATA_PLANE_GROUP_TAG: String = "data_plane_group"
  const val DATA_PLANE_GROUP_NAME_TAG: String = "data_plane_group_name"
  const val KUBE_COMMAND_TYPE_TAG: String = "kube_command_type"
  const val KUBE_POD_TYPE_TAG: String = "kube_pod_type"
  const val MUTEX_KEY_TAG: String = "mutex_key"
  const val PRIORITY_TAG: String = "priority"
  const val QUEUE_NAME_TAG: String = "queue_name"
  const val STAGE_NAME_TAG: String = "stage_name"
  const val STATUS_TAG: String = "status"
  const val WORKLOAD_CANCEL_REASON_TAG: String = "cancel_reason"
  const val WORKLOAD_CANCEL_SOURCE_TAG: String = "cancel_source"
  const val WORKLOAD_ID_TAG: String = "workload_id"
  const val WORKLOAD_TYPE_TAG: String = "workload_type"

  @JvmStatic
  fun getReleaseStage(stage: ReleaseStage?): String = if (stage != null) stage.value() else UNKNOWN

  @JvmStatic
  fun getFailureOrigin(origin: FailureReason.FailureOrigin?): String =
    if (origin != null) origin.value() else FailureReason.FailureOrigin.UNKNOWN.value()

  @JvmStatic
  fun getFailureType(origin: FailureReason.FailureType?): String = if (origin != null) origin.value() else UNKNOWN
}
