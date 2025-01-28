/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.workers.models.JobInput;
import io.airbyte.workers.models.SyncJobCheckConnectionInputs;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Objects;

/**
 * GenerateInputActivity.
 */
@ActivityInterface
public interface GenerateInputActivity {

  /**
   * SyncInput.
   */
  class SyncInput {

    private int attemptId;
    private long jobId;

    public SyncInput() {}

    public SyncInput(int attemptId, long jobId) {
      this.attemptId = attemptId;
      this.jobId = jobId;
    }

    public int getAttemptId() {
      return attemptId;
    }

    public void setAttemptId(int attemptId) {
      this.attemptId = attemptId;
    }

    public long getJobId() {
      return jobId;
    }

    public void setJobId(long jobId) {
      this.jobId = jobId;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SyncInput syncInput = (SyncInput) o;
      return attemptId == syncInput.attemptId && jobId == syncInput.jobId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(attemptId, jobId);
    }

    @Override
    public String toString() {
      return "SyncInput{attemptId=" + attemptId + ", jobId=" + jobId + '}';
    }

  }

  /**
   * SyncInputWithAttemptNumber.
   */
  class SyncInputWithAttemptNumber {

    private int attemptNumber;
    private long jobId;

    public SyncInputWithAttemptNumber() {}

    public SyncInputWithAttemptNumber(int attemptNumber, long jobId) {
      this.attemptNumber = attemptNumber;
      this.jobId = jobId;
    }

    public int getAttemptNumber() {
      return attemptNumber;
    }

    public void setAttemptNumber(int attemptNumber) {
      this.attemptNumber = attemptNumber;
    }

    public long getJobId() {
      return jobId;
    }

    public void setJobId(long jobId) {
      this.jobId = jobId;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SyncInputWithAttemptNumber that = (SyncInputWithAttemptNumber) o;
      return attemptNumber == that.attemptNumber && jobId == that.jobId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(attemptNumber, jobId);
    }

    @Override
    public String toString() {
      return "SyncInputWithAttemptNumber{attemptNumber=" + attemptNumber + ", jobId=" + jobId + '}';
    }

  }

  /**
   * This generate the input needed by the child sync workflow.
   */
  @ActivityMethod
  JobInput getSyncWorkflowInput(SyncInput input) throws Exception;

  /**
   * This generate the input needed by the child sync workflow.
   */
  @ActivityMethod
  JobInput getSyncWorkflowInputWithAttemptNumber(SyncInputWithAttemptNumber input) throws Exception;

  @ActivityMethod
  SyncJobCheckConnectionInputs getCheckConnectionInputs(SyncInputWithAttemptNumber input);

}
