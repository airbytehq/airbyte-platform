/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import java.util.UUID

/**
 * Exception thrown when a workspace cannot be found or has been deleted.
 * This is a non-retryable error as deleted workspaces will not come back.
 */
class WorkspaceNotFoundException(
  val workspaceId: UUID? = null,
  message: String = "Workspace not found or has been deleted",
  cause: Throwable? = null,
) : RuntimeException(message, cause) {
  companion object {
    fun fromClientException(
      workspaceId: UUID,
      statusCode: Int,
      cause: Throwable,
    ): WorkspaceNotFoundException {
      val message =
        when (statusCode) {
          404 -> "Workspace $workspaceId not found. It may have been deleted."
          else -> "Failed to retrieve workspace $workspaceId (HTTP $statusCode)"
        }
      return WorkspaceNotFoundException(workspaceId, message, cause)
    }
  }
}
