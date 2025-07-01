/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

/**
 * These extra env variables are created on the fly and passed to the connector.
 */
object WorkerEnvConstants {
  const val WORKER_CONNECTOR_IMAGE: String = "WORKER_CONNECTOR_IMAGE"
  const val WORKER_JOB_ID: String = "WORKER_JOB_ID"
  const val WORKER_JOB_ATTEMPT: String = "WORKER_JOB_ATTEMPT"
}
