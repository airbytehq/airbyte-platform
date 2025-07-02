/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

import java.util.Map
import java.util.UUID

/**
 * Helps Annotating Traces.
 */
object TracingHelper {
  /**
   * Add ConnectionId ta the current Trace.
   */
  @JvmStatic
  fun addConnection(connectionId: UUID?) {
    if (connectionId != null) {
      ApmTraceUtils.addTagsToTrace(Map.of<String, Any>(ApmTraceConstants.Tags.CONNECTION_ID_KEY, connectionId))
    }
  }

  /**
   * Add WorkspaceId ta the current Trace.
   */
  @JvmStatic
  fun addWorkspace(workspaceId: UUID?) {
    if (workspaceId != null) {
      ApmTraceUtils.addTagsToTrace(Map.of<String, Any>(ApmTraceConstants.Tags.WORKSPACE_ID_KEY, workspaceId))
    }
  }

  /**
   * Add SourceId and DestinationId ta the current Trace.
   */
  @JvmStatic
  fun addSourceDestination(
    sourceId: UUID?,
    destinationId: UUID?,
  ) {
    val tags: MutableMap<String, Any> = HashMap()
    if (sourceId != null) {
      tags[ApmTraceConstants.Tags.SOURCE_ID_KEY] = sourceId
    }
    if (destinationId != null) {
      tags[ApmTraceConstants.Tags.DESTINATION_ID_KEY] = destinationId
    }
    ApmTraceUtils.addTagsToTrace(tags.toMap())
  }
}
