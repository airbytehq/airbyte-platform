/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.StandardSyncOutput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Objects;
import java.util.UUID;

/**
 * JobCreationAndStatusUpdateActivity.
 */
@ActivityInterface
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public interface JobCreationAndStatusUpdateActivity {

  /**
   * JobCreationInput.
   */
  class JobCreationInput {

    private UUID connectionId;

    public JobCreationInput() {}

    public JobCreationInput(UUID connectionId) {
      this.connectionId = connectionId;
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
      JobCreationInput that = (JobCreationInput) o;
      return Objects.equals(connectionId, that.connectionId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(connectionId);
    }

    @Override
    public String toString() {
      return "JobCreationInput{connectionId=" + connectionId + '}';
    }

  }

  /**
   * JobCreationOutput.
   */
  class JobCreationOutput {

    private Long jobId;

    public JobCreationOutput() {}

    public JobCreationOutput(Long jobId) {
      this.jobId = jobId;
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
      JobCreationOutput that = (JobCreationOutput) o;
      return Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(jobId);
    }

    @Override
    public String toString() {
      return "JobCreationOutput{jobId=" + jobId + '}';
    }

  }

  /**
   * Creates a new job.
   *
   * @param input - POJO that contains the connections
   * @return a POJO that contains the jobId
   */
  @ActivityMethod
  JobCreationOutput createNewJob(JobCreationInput input);

  /**
   * AttemptCreationInput.
   */
  class AttemptCreationInput {

    private Long jobId;

    public AttemptCreationInput() {}

    public AttemptCreationInput(Long jobId) {
      this.jobId = jobId;
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
      AttemptCreationInput that = (AttemptCreationInput) o;
      return Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(jobId);
    }

    @Override
    public String toString() {
      return "AttemptCreationInput{jobId=" + jobId + '}';
    }

  }

  /**
   * AttemptNumberCreationOutput.
   */
  class AttemptNumberCreationOutput {

    private Integer attemptNumber;

    public AttemptNumberCreationOutput() {}

    public AttemptNumberCreationOutput(Integer attemptNumber) {
      this.attemptNumber = attemptNumber;
    }

    public Integer getAttemptNumber() {
      return attemptNumber;
    }

    public void setAttemptNumber(Integer attemptNumber) {
      this.attemptNumber = attemptNumber;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AttemptNumberCreationOutput that = (AttemptNumberCreationOutput) o;
      return Objects.equals(attemptNumber, that.attemptNumber);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(attemptNumber);
    }

    @Override
    public String toString() {
      return "AttemptNumberCreationOutput{attemptNumber=" + attemptNumber + '}';
    }

  }

  /**
   * Create a new attempt for a given job ID.
   *
   * @param input POJO containing the jobId
   * @return A POJO containing the attemptNumber
   */
  @ActivityMethod
  AttemptNumberCreationOutput createNewAttemptNumber(AttemptCreationInput input) throws RetryableException;

  /**
   * JobSuccessInputWithAttemptNumber.
   */
  class JobSuccessInputWithAttemptNumber {

    private Long jobId;
    private Integer attemptNumber;
    private UUID connectionId;
    private StandardSyncOutput standardSyncOutput;

    public JobSuccessInputWithAttemptNumber() {}

    public JobSuccessInputWithAttemptNumber(Long jobId, Integer attemptNumber, UUID connectionId, StandardSyncOutput standardSyncOutput) {
      this.jobId = jobId;
      this.attemptNumber = attemptNumber;
      this.connectionId = connectionId;
      this.standardSyncOutput = standardSyncOutput;
    }

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

    public UUID getConnectionId() {
      return connectionId;
    }

    public void setConnectionId(UUID connectionId) {
      this.connectionId = connectionId;
    }

    public StandardSyncOutput getStandardSyncOutput() {
      return standardSyncOutput;
    }

    public void setStandardSyncOutput(StandardSyncOutput standardSyncOutput) {
      this.standardSyncOutput = standardSyncOutput;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      JobSuccessInputWithAttemptNumber that = (JobSuccessInputWithAttemptNumber) o;
      return Objects.equals(jobId, that.jobId) && Objects.equals(attemptNumber, that.attemptNumber) && Objects.equals(
          connectionId, that.connectionId) && Objects.equals(standardSyncOutput, that.standardSyncOutput);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, attemptNumber, connectionId, standardSyncOutput);
    }

    @Override
    public String toString() {
      return "JobSuccessInputWithAttemptNumber{"
          + "jobId=" + jobId
          + ", attemptNumber=" + attemptNumber
          + ", connectionId=" + connectionId
          + ", standardSyncOutput=" + standardSyncOutput
          + '}';
    }

  }

  /**
   * Set a job status as successful.
   */
  @ActivityMethod
  void jobSuccessWithAttemptNumber(JobSuccessInputWithAttemptNumber input);

  /**
   * JobFailureInput.
   */
  class JobFailureInput {

    private Long jobId;
    private Integer attemptNumber;
    private UUID connectionId;
    private String reason;

    public JobFailureInput() {}

    public JobFailureInput(Long jobId, Integer attemptNumber, UUID connectionId, String reason) {
      this.jobId = jobId;
      this.attemptNumber = attemptNumber;
      this.connectionId = connectionId;
      this.reason = reason;
    }

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

    public UUID getConnectionId() {
      return connectionId;
    }

    public void setConnectionId(UUID connectionId) {
      this.connectionId = connectionId;
    }

    public String getReason() {
      return reason;
    }

    public void setReason(String reason) {
      this.reason = reason;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      JobFailureInput that = (JobFailureInput) o;
      return Objects.equals(jobId, that.jobId) && Objects.equals(attemptNumber, that.attemptNumber) && Objects.equals(
          connectionId, that.connectionId) && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, attemptNumber, connectionId, reason);
    }

    @Override
    public String toString() {
      return "JobFailureInput{"
          + "jobId=" + jobId
          + ", attemptNumber=" + attemptNumber
          + ", connectionId=" + connectionId
          + ", reason='" + reason + '\''
          + '}';
    }

  }

  /**
   * Set a job status as failed.
   */
  @ActivityMethod
  void jobFailure(JobFailureInput input);

  /**
   * AttemptNumberFailureInput.
   */
  class AttemptNumberFailureInput {

    private Long jobId;
    private Integer attemptNumber;
    private UUID connectionId;
    private StandardSyncOutput standardSyncOutput;
    private AttemptFailureSummary attemptFailureSummary;

    public AttemptNumberFailureInput() {}

    public AttemptNumberFailureInput(Long jobId,
                                     Integer attemptNumber,
                                     UUID connectionId,
                                     StandardSyncOutput standardSyncOutput,
                                     AttemptFailureSummary attemptFailureSummary) {
      this.jobId = jobId;
      this.attemptNumber = attemptNumber;
      this.connectionId = connectionId;
      this.standardSyncOutput = standardSyncOutput;
      this.attemptFailureSummary = attemptFailureSummary;
    }

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

    public UUID getConnectionId() {
      return connectionId;
    }

    public void setConnectionId(UUID connectionId) {
      this.connectionId = connectionId;
    }

    public StandardSyncOutput getStandardSyncOutput() {
      return standardSyncOutput;
    }

    public void setStandardSyncOutput(StandardSyncOutput standardSyncOutput) {
      this.standardSyncOutput = standardSyncOutput;
    }

    public AttemptFailureSummary getAttemptFailureSummary() {
      return attemptFailureSummary;
    }

    public void setAttemptFailureSummary(AttemptFailureSummary attemptFailureSummary) {
      this.attemptFailureSummary = attemptFailureSummary;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AttemptNumberFailureInput that = (AttemptNumberFailureInput) o;
      return Objects.equals(jobId, that.jobId) && Objects.equals(attemptNumber, that.attemptNumber) && Objects.equals(
          connectionId, that.connectionId) && Objects.equals(standardSyncOutput, that.standardSyncOutput)
          && Objects.equals(
              attemptFailureSummary, that.attemptFailureSummary);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, attemptNumber, connectionId, standardSyncOutput, attemptFailureSummary);
    }

    @Override
    public String toString() {
      return "AttemptNumberFailureInput{"
          + "jobId=" + jobId
          + ", attemptNumber=" + attemptNumber
          + ", connectionId=" + connectionId
          + ", standardSyncOutput=" + standardSyncOutput
          + ", attemptFailureSummary=" + attemptFailureSummary
          + '}';
    }

  }

  /**
   * Set an attempt status as failed.
   */
  @ActivityMethod
  void attemptFailureWithAttemptNumber(AttemptNumberFailureInput input);

  /**
   * JobCancelledInputWithAttemptNumber.
   */
  class JobCancelledInputWithAttemptNumber {

    private Long jobId;
    private Integer attemptNumber;
    private UUID connectionId;
    private AttemptFailureSummary attemptFailureSummary;

    public JobCancelledInputWithAttemptNumber() {}

    public JobCancelledInputWithAttemptNumber(Long jobId, Integer attemptNumber, UUID connectionId, AttemptFailureSummary attemptFailureSummary) {
      this.jobId = jobId;
      this.attemptNumber = attemptNumber;
      this.connectionId = connectionId;
      this.attemptFailureSummary = attemptFailureSummary;
    }

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

    public UUID getConnectionId() {
      return connectionId;
    }

    public void setConnectionId(UUID connectionId) {
      this.connectionId = connectionId;
    }

    public AttemptFailureSummary getAttemptFailureSummary() {
      return attemptFailureSummary;
    }

    public void setAttemptFailureSummary(AttemptFailureSummary attemptFailureSummary) {
      this.attemptFailureSummary = attemptFailureSummary;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      JobCancelledInputWithAttemptNumber that = (JobCancelledInputWithAttemptNumber) o;
      return Objects.equals(jobId, that.jobId) && Objects.equals(attemptNumber, that.attemptNumber) && Objects.equals(
          connectionId, that.connectionId) && Objects.equals(attemptFailureSummary, that.attemptFailureSummary);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, attemptNumber, connectionId, attemptFailureSummary);
    }

    @Override
    public String toString() {
      return "JobCancelledInputWithAttemptNumber{"
          + "jobId=" + jobId
          + ", attemptNumber=" + attemptNumber
          + ", connectionId=" + connectionId
          + ", attemptFailureSummary=" + attemptFailureSummary
          + '}';
    }

  }

  /**
   * Set a job status as cancelled.
   */
  @ActivityMethod
  void jobCancelledWithAttemptNumber(JobCancelledInputWithAttemptNumber input);

  /**
   * ReportJobStartInput.
   */
  class ReportJobStartInput {

    private Long jobId;
    private UUID connectionId;

    public ReportJobStartInput() {}

    public ReportJobStartInput(Long jobId, UUID connectionId) {
      this.jobId = jobId;
      this.connectionId = connectionId;
    }

    public Long getJobId() {
      return jobId;
    }

    public void setJobId(Long jobId) {
      this.jobId = jobId;
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
      ReportJobStartInput that = (ReportJobStartInput) o;
      return Objects.equals(jobId, that.jobId) && Objects.equals(connectionId, that.connectionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, connectionId);
    }

    @Override
    public String toString() {
      return "ReportJobStartInput{jobId=" + jobId + ", connectionId=" + connectionId + '}';
    }

  }

  @ActivityMethod
  void reportJobStart(ReportJobStartInput reportJobStartInput);

  /**
   * EnsureCleanJobStateInput.
   */
  class EnsureCleanJobStateInput {

    private UUID connectionId;

    public EnsureCleanJobStateInput() {}

    public EnsureCleanJobStateInput(UUID connectionId) {
      this.connectionId = connectionId;
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
      EnsureCleanJobStateInput that = (EnsureCleanJobStateInput) o;
      return Objects.equals(connectionId, that.connectionId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(connectionId);
    }

    @Override
    public String toString() {
      return "EnsureCleanJobStateInput{connectionId=" + connectionId + '}';
    }

  }

  @ActivityMethod
  void ensureCleanJobState(EnsureCleanJobStateInput input);

  /**
   * JobCheckFailureInput.
   */
  class JobCheckFailureInput {

    private Long jobId;
    private Integer attemptId;
    private UUID connectionId;

    public JobCheckFailureInput() {}

    public JobCheckFailureInput(Long jobId, Integer attemptId, UUID connectionId) {
      this.jobId = jobId;
      this.attemptId = attemptId;
      this.connectionId = connectionId;
    }

    public Long getJobId() {
      return jobId;
    }

    public void setJobId(Long jobId) {
      this.jobId = jobId;
    }

    public Integer getAttemptId() {
      return attemptId;
    }

    public void setAttemptId(Integer attemptId) {
      this.attemptId = attemptId;
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
      JobCheckFailureInput that = (JobCheckFailureInput) o;
      return Objects.equals(jobId, that.jobId) && Objects.equals(attemptId, that.attemptId) && Objects.equals(connectionId,
          that.connectionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, attemptId, connectionId);
    }

    @Override
    public String toString() {
      return "JobCheckFailureInput{jobId=" + jobId + ", attemptId=" + attemptId + ", connectionId=" + connectionId + '}';
    }

  }

  @ActivityMethod
  boolean isLastJobOrAttemptFailure(JobCheckFailureInput input);

}
