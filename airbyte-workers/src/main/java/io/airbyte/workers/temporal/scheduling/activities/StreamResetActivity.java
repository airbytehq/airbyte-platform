/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Objects;
import java.util.UUID;

/**
 * StreamResetActivity.
 */
@ActivityInterface
public interface StreamResetActivity {

  /**
   * DeleteStreamResetRecordsForJobInput.
   */
  class DeleteStreamResetRecordsForJobInput {

    private UUID connectionId;
    private Long jobId;

    public DeleteStreamResetRecordsForJobInput() {}

    public DeleteStreamResetRecordsForJobInput(UUID connectionId, Long jobId) {
      this.connectionId = connectionId;
      this.jobId = jobId;
    }

    public UUID getConnectionId() {
      return connectionId;
    }

    public void setConnectionId(UUID connectionId) {
      this.connectionId = connectionId;
    }

    public Long getJobId() {
      return jobId;
    }

    public void setJobId(Long jobId) {
      this.jobId = jobId;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DeleteStreamResetRecordsForJobInput that = (DeleteStreamResetRecordsForJobInput) o;
      return Objects.equals(connectionId, that.connectionId) && Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(connectionId, jobId);
    }

    @Override
    public String toString() {
      return "DeleteStreamResetRecordsForJobInput{connectionId=" + connectionId + ", jobId=" + jobId + '}';
    }

  }

  /**
   * Deletes the stream_reset record corresponding to each stream descriptor passed in.
   */
  @ActivityMethod
  void deleteStreamResetRecordsForJob(DeleteStreamResetRecordsForJobInput input);

}
