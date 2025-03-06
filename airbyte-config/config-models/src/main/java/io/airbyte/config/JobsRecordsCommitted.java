/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import jakarta.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

/**
 * POJO / accessors for the attempt domain model.
 */
public class JobsRecordsCommitted {

  private final int attemptNumber;
  private final long jobId;
  private final Long recordsCommitted;
  private final Long endedAtInSecond;

  public JobsRecordsCommitted(final int attemptNumber,
                              final long jobId,
                              final @Nullable Long recordsCommitted,
                              final @Nullable Long endedAtInSecond) {
    this.attemptNumber = attemptNumber;
    this.jobId = jobId;
    this.recordsCommitted = recordsCommitted;
    this.endedAtInSecond = endedAtInSecond;
  }

  public int getAttemptNumber() {
    return attemptNumber;
  }

  public long getJobId() {
    return jobId;
  }

  public Optional<Long> getRecordsCommitted() {
    return Optional.ofNullable(recordsCommitted);
  }

  public Optional<Long> getEndedAtInSecond() {
    return Optional.ofNullable(endedAtInSecond);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final JobsRecordsCommitted attempt = (JobsRecordsCommitted) o;
    return attemptNumber == attempt.attemptNumber
        && jobId == attempt.jobId
        && Objects.equals(recordsCommitted, attempt.recordsCommitted)
        && Objects.equals(endedAtInSecond, attempt.endedAtInSecond);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attemptNumber,
        jobId,
        recordsCommitted,
        endedAtInSecond);
  }

  @Override
  public String toString() {
    return "Attempt{"
        + "id=" + attemptNumber
        + ", jobId=" + jobId
        + ", recordsCommitted=" + recordsCommitted
        + ", endedAtInSecond=" + endedAtInSecond
        + '}';
  }

}
