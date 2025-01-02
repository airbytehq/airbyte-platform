/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

import io.airbyte.metrics.lib.ApmTraceConstants.Tags;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Helps Annotating Traces.
 */
public class TracingHelper {

  /**
   * Add ConnectionId ta the current Trace.
   */
  public static void addConnection(final UUID connectionId) {
    if (connectionId != null) {
      ApmTraceUtils.addTagsToTrace(Map.of(Tags.CONNECTION_ID_KEY, connectionId));
    }
  }

  /**
   * Add WorkspaceId ta the current Trace.
   */
  public static void addWorkspace(final UUID workspaceId) {
    if (workspaceId != null) {
      ApmTraceUtils.addTagsToTrace(Map.of(Tags.WORKSPACE_ID_KEY, workspaceId));
    }
  }

  /**
   * Add SourceId and DestinationId ta the current Trace.
   */
  public static void addSourceDestination(final UUID sourceId, final UUID destinationId) {
    final Map<String, Object> tags = new HashMap<>();
    if (sourceId != null) {
      tags.put(Tags.SOURCE_ID_KEY, sourceId);
    }
    if (destinationId != null) {
      tags.put(Tags.DESTINATION_ID_KEY, destinationId);
    }
    ApmTraceUtils.addTagsToTrace(tags);
  }

}
