/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state;

import io.airbyte.config.FailureReason;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Internal state of workflow.
 */
// todo (cgardens) - how is this different from WorkflowState.
@Getter
@Setter
@NoArgsConstructor
public class WorkflowInternalState {

  private Long jobId = null;

  /**
   * 0-based incrementing sequence.
   */
  private Integer attemptNumber = null;

  // StandardSyncOutput standardSyncOutput = null;
  private Set<FailureReason> failures = new HashSet<>();
  private Boolean partialSuccess = null;

}
