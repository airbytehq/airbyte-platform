/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared

object Constants {
  const val TASK_QUEUE = "ConnectorRolloutTaskQueue"
  const val DEFAULT_NAMESPACE = "default"

  // 15 minutes
  const val VERIFY_ACTIVITY_TIMEOUT_MILLIS = 900000

  // 30 seconds
  const val VERIFY_ACTIVITY_TIME_BETWEEN_POLLS_MILLIS = 30000
}
