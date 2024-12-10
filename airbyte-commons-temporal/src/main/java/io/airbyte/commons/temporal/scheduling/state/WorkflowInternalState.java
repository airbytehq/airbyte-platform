/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state;

import io.airbyte.config.FailureReason;
import java.util.HashSet;
import java.util.Set;

/**
 * Internal state of workflow.
 */
// todo (cgardens) - how is this different from WorkflowState.
public class WorkflowInternalState {

  private Long jobId = null;

  /**
   * 0-based incrementing sequence.
   */
  private Integer attemptNumber = null;

  // StandardSyncOutput standardSyncOutput = null;
  private Set<FailureReason> failures = new HashSet<>();
  private Boolean partialSuccess = null;

  public WorkflowInternalState() {}

  public Long getJobId() {
    return jobId;
  }

  public void setJobId(Long jobId) {
    this.jobId = jobId;
  }

  public Integer getAttemptNumber() {
    return attemptNumber;
  }

  public void setAttemptNumber(Integer attemptNumber) {
    this.attemptNumber = attemptNumber;
  }

  public Set<FailureReason> getFailures() {
    return failures;
  }

  public void setFailures(Set<FailureReason> failures) {
    this.failures = failures;
  }

  public Boolean getPartialSuccess() {
    return partialSuccess;
  }

  public void setPartialSuccess(Boolean partialSuccess) {
    this.partialSuccess = partialSuccess;
  }

}
