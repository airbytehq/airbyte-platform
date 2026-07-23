/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib

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
      ApmTraceUtils.addTagsToTrace(mapOf(ApmTraceConstants.Tags.CONNECTION_ID_KEY to connectionId))
    }
  }

  /**
   * Add WorkspaceId ta the current Trace.
   */
  @JvmStatic
  fun addWorkspace(workspaceId: UUID?) {
    if (workspaceId != null) {
      ApmTraceUtils.addTagsToTrace(mapOf(ApmTraceConstants.Tags.WORKSPACE_ID_KEY to workspaceId))
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
    val tags: MutableMap<String, Any> = hashMapOf()
    if (sourceId != null) {
      tags[ApmTraceConstants.Tags.SOURCE_ID_KEY] = sourceId
    }
    if (destinationId != null) {
      tags[ApmTraceConstants.Tags.DESTINATION_ID_KEY] = destinationId
    }
    ApmTraceUtils.addTagsToTrace(tags.toMap())
  }
}
