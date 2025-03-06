/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Objects;
import java.util.UUID;

/**
 * Activity to check whether a given run (attempt for now) made progress. Output to be used as input
 * to retry logic.
 */
@ActivityInterface
public interface CheckRunProgressActivity {

  /**
   * Input object for CheckRunProgressActivity#checkProgress.
   */
  class Input {

    private Long jobId;
    private Integer attemptNo;
    private UUID connectionId;

    public Input(Long jobId, Integer attemptNo, UUID connectionId) {
      this.jobId = jobId;
      this.attemptNo = attemptNo;
      this.connectionId = connectionId;
    }

    public Input() {}

    public Long getJobId() {
      return jobId;
    }

    public void setJobId(Long jobId) {
      this.jobId = jobId;
    }

    public Integer getAttemptNo() {
      return attemptNo;
    }

    public void setAttemptNo(Integer attemptNo) {
      this.attemptNo = attemptNo;
    }

    public UUID getConnectionId() {
      return connectionId;
    }

    public void setConnectionId(UUID connectionId) {
      this.connectionId = connectionId;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Input input = (Input) o;
      return Objects.equals(jobId, input.jobId) && Objects.equals(attemptNo, input.attemptNo) && Objects.equals(
          connectionId, input.connectionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, attemptNo, connectionId);
    }

    @Override
    public String toString() {
      return "Input{jobId=" + jobId + ", attemptNo=" + attemptNo + ", connectionId=" + connectionId + '}';
    }

  }

  /**
   * Output object for CheckRunProgressActivity#checkProgress.
   */
  class Output {

    private Boolean madeProgress;

    public Output(Boolean madeProgress) {
      this.madeProgress = madeProgress;
    }

    public Output() {}

    public Boolean madeProgress() {
      return madeProgress;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Output output = (Output) o;
      return Objects.equals(madeProgress, output.madeProgress);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(madeProgress);
    }

    @Override
    public String toString() {
      return "Output{madeProgress=" + madeProgress + '}';
    }

  }

  @ActivityMethod
  Output checkProgress(final Input input);

}
