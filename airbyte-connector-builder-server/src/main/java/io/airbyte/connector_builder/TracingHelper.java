/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder;

import io.airbyte.metrics.lib.ApmTraceUtils;
import java.util.Map;
import java.util.Optional;

/**
 * Collection of utils for APM tracing.
 */
public final class TracingHelper {

  /**
   * Collection of utils for APM tracing.
   */
  public static final String CONNECTOR_BUILDER_OPERATION_NAME = "connector_builder_server";

  private TracingHelper() {

  }

  /**
   * Add workspace and project ids to the current trace.
   *
   * @param workspaceId The workspace id
   * @param projectId The project id
   */
  public static void addWorkspaceAndProjectIdsToTrace(final String workspaceId, final String projectId) {
    ApmTraceUtils.addTagsToTrace(Map.of("project.id", Optional.ofNullable(projectId).orElse("UNKNOWN")));
    ApmTraceUtils.addTagsToTrace(Map.of("workspace.id", Optional.ofNullable(workspaceId).orElse("UNKNOWN")));
  }

}
