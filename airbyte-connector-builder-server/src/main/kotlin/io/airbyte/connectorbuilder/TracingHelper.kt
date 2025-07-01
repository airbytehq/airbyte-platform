/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder

import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace

/**
 * Collection of utils for APM tracing.
 */
object TracingHelper {
  /**
   * Collection of utils for APM tracing.
   */
  const val CONNECTOR_BUILDER_OPERATION_NAME: String = "connector_builder_server"

  /**
   * Add workspace and project ids to the current trace.
   *
   * @param workspaceId The workspace id
   * @param projectId The project id
   */
  @JvmStatic
  fun addWorkspaceAndProjectIdsToTrace(
    workspaceId: String?,
    projectId: String?,
  ) {
    addTagsToTrace(mutableMapOf("project.id" to (projectId ?: "UNKNOWN")))
    addTagsToTrace(mutableMapOf("workspace.id" to (workspaceId ?: "UNKNOWN")))
  }
}
