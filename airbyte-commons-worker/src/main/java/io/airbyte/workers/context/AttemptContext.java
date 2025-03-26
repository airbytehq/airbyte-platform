/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.context;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import io.airbyte.metrics.lib.ApmTraceUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Context of an Attempt.
 */
public record AttemptContext(UUID connectionId, Long jobId, Integer attemptNumber) {

  /**
   * Update the current trace with the ids from the context.
   */
  public void addTagsToTrace() {
    final Map<String, Object> tags = new HashMap<>();
    if (connectionId != null) {
      tags.put(CONNECTION_ID_KEY, connectionId);
    }
    if (jobId != null) {
      tags.put(JOB_ID_KEY, jobId);
    }
    if (attemptNumber != null) {
      tags.put(ATTEMPT_NUMBER_KEY, attemptNumber);
    }
    ApmTraceUtils.addTagsToTrace(tags);
  }

}
