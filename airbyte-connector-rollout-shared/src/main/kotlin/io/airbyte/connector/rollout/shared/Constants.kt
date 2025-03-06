/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared

object Constants {
  const val TASK_QUEUE = "ConnectorRolloutTaskQueue"
  const val DEFAULT_NAMESPACE = "default"
  const val AIRBYTE_API_CLIENT_EXCEPTION = "AirbyteApiClientException"
  const val DEFAULT_INITIAL_ROLLOUT_PERCENTAGE = 25
  const val DEFAULT_MAX_ROLLOUT_PERCENTAGE = 50

  // Percentage of syncs required to be successful for a rollout to be automatically released
  const val DEFAULT_SUCCESS_THRESHOLD_PERCENTAGE = 100

  // Percentage of actors who must have finished syncs to consider a rollout complete
  const val DEFAULT_PERCENTAGE_OF_ACTORS_WITH_COMPLETED_SYNCS_REQUIRED = 75

  // 10 mins
  const val VERIFY_ACTIVITY_HEARTBEAT_TIMEOUT_SECONDS = 600

  // 3 hours
  const val VERIFY_ACTIVITY_TIMEOUT_MILLIS = 10800000

  // 1 minute
  const val VERIFY_ACTIVITY_TIME_BETWEEN_POLLS_MILLIS = 60000
}
