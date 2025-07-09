/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

/**
 * Error code for temporal jobs run at Airbyte.
 */
enum class ErrorCode {
  UNKNOWN,
  WORKFLOW_DELETED,
  WORKFLOW_RUNNING,
}
