/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.ReleaseStage;

/**
 * Keep track of all metric tags.
 */
public class MetricTags {

  public static final String ACTIVITY_NAME = "activity_name";
  public static final String ACTIVITY_METHOD = "activity_method";
  public static final String ATTEMPT_NUMBER = "attempt_number"; // 0|1|2|3
  public static final String ATTEMPT_OUTCOME = "attempt_outcome"; // succeeded|failed
  public static final String ATTEMPT_QUEUE = "attempt_queue";
  public static final String AUTHENTICATION_RESPONSE = "authentication_response";
  public static final String AUTHENTICATION_RESPONSE_FAILURE_REASON = "authentication_response_failure_reason";
  public static final String AUTHENTICATION_REQUEST_URI_ATTRIBUTE_KEY = "request_uri";
  public static final String CANCELLATION_SOURCE = "cancellation_source";
  public static final String COMMAND = "command";
  public static final String COMMAND_STEP = "command_step";
  public static final String CONFIG_TYPES = "config_types";
  public static final String CONNECTION_ID = "connection_id";
  public static final String CONNECTION_IDS = "connection_ids";
  public static final String CONNECTOR_IMAGE = "connector_image";
  public static final String CONNECTOR_TYPE = "connector_type";
  public static final String CRON_TYPE = "cron_type";
  public static final String CRUD_OPERATION = "crud_operation";
  public static final String DESTINATION_ID = "destination_id";
  public static final String DESTINATION_IMAGE = "destination_image";
  public static final String DESTINATION_IMAGE_IS_DEFAULT = "destination_image_is_default";
  public static final String EXPIRE_SECRET = "expire_secret";
  public static final String FAILURE = "failure";
  public static final String FAILURE_CAUSE = "failure_cause";
  public static final String FAILURE_ORIGIN = "failure_origin";
  public static final String FAILURE_TYPE = "failure_type";
  public static final String INTERNAL_MESSAGE = "internal_message";
  public static final String EXTERNAL_MESSAGE = "external_message";
  public static final String GEOGRAPHY = "geography";
  public static final String IMPLEMENTATION = "implementation";
  public static final String IS_CUSTOM_CONNECTOR_SYNC = "is_custom_connector_sync";
  public static final String IS_RESET = "is_reset";
  public static final String JOB_ID = "job_id";
  public static final String JOB_STATUS = "job_status";
  public static final String KILLED = "killed";
  public static final String MADE_PROGRESS = "made_progress";
  // the release stage of the highest release connector in the sync (GA > Beta > Alpha)
  public static final String MAX_CONNECTOR_RELEASE_STATE = "max_connector_release_stage";
  public static final String MESSAGE_TYPE = "message_type";
  // the release stage of the lowest release stage connector in the sync (GA > Beta > Alpha)
  public static final String MIN_CONNECTOR_RELEASE_STATE = "min_connector_release_stage";
  public static final String NOTIFICATION_TRIGGER = "notification_trigger";
  public static final String NOTIFICATION_CLIENT = "notification_client";
  public static final String RELEASE_STAGE = "release_stage";
  public static final String SOURCE_ID = "source_id";
  public static final String SOURCE_DEFINITION_ID = "source_definition_id";
  public static final String SOURCE_IMAGE = "source_image";
  public static final String SOURCE_IMAGE_IS_DEFAULT = "source_image_is_default";
  public static final String STATUS = "status";
  public static final String SUCCESS = "success";
  public static final String WILL_RETRY = "will_retry";
  public static final String WORKSPACE_ID = "workspace_id";
  public static final String WORKFLOW_TYPE = "workflow_type";
  public static final String WORKLOAD_TYPE = "workload_type";
  public static final String UNKNOWN = "unknown";
  public static final String USER_TYPE = "user_type"; // real user, service account, data plane user, etc
  public static final String CLIENT_ID = "client_id";
  public static final String LOG_CLIENT_TYPE = "log_client_type";
  public static final String MALFORMED_LOG_LINE_LENGTH = "malformed_log_line_length";

  // payload metric tags
  public static final String URI_NULL = "uri_null";
  public static final String URI_ID = "uri_id";
  public static final String URI_VERSION = "uri_version";
  public static final String PAYLOAD_NAME = "payload_name";
  public static final String IS_MATCH = "is_match";
  public static final String IS_MISS = "is_miss";
  public static final String TASK_QUEUE = "task_queue";

  public static String getReleaseStage(final ReleaseStage stage) {
    return stage != null ? stage.value() : UNKNOWN;
  }

  public static String getFailureOrigin(final FailureOrigin origin) {
    return origin != null ? origin.value() : FailureOrigin.UNKNOWN.value();
  }

  public static String getFailureType(final FailureType origin) {
    return origin != null ? origin.value() : UNKNOWN;
  }

}
