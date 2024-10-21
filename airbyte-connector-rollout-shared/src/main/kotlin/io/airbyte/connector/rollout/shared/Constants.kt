/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared

object Constants {
  const val TASK_QUEUE = "ConnectorRolloutTaskQueue"
  const val DEFAULT_NAMESPACE = "default"
  const val AIRBYTE_API_CLIENT_EXCEPTION = "AirbyteApiClientException"

  // 3 hours
  const val VERIFY_ACTIVITY_TIMEOUT_MILLIS = 10800000

  // 1 minute
  const val VERIFY_ACTIVITY_TIME_BETWEEN_POLLS_MILLIS = 60000
}
