/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Objects;

/**
 * Activity for adding messages to a given attempt log from workflows.
 */
@ActivityInterface
public interface AppendToAttemptLogActivity {

  /**
   * Supported log levels.
   */
  enum LogLevel {
    ERROR,
    INFO,
    WARN
  }

  /**
   * Input for append to attempt log activity method.
   */
  class LogInput {

    private Long jobId;
    private Integer attemptNumber;
    private String message;
    private LogLevel level;

    public LogInput() {}

    public LogInput(Long jobId, Integer attemptNumber, String message, LogLevel level) {
      this.jobId = jobId;
      this.attemptNumber = attemptNumber;
      this.message = message;
      this.level = level;
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

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public LogLevel getLevel() {
      return level;
    }

    public void setLevel(LogLevel level) {
      this.level = level;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LogInput logInput = (LogInput) o;
      return Objects.equals(jobId, logInput.jobId) && Objects.equals(attemptNumber, logInput.attemptNumber)
          && Objects.equals(message, logInput.message) && level == logInput.level;
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, attemptNumber, message, level);
    }

    @Override
    public String toString() {
      return "LogInput{jobId=" + jobId + ", attemptNumber=" + attemptNumber + ", message='" + message + '\'' + ", level=" + level + '}';
    }

  }

  /**
   * Output for append to attempt log activity method.
   */
  class LogOutput {

    private Boolean success;

    public LogOutput() {}

    public LogOutput(Boolean success) {
      this.success = success;
    }

    public Boolean getSuccess() {
      return success;
    }

    public void setSuccess(Boolean success) {
      this.success = success;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LogOutput logOutput = (LogOutput) o;
      return Objects.equals(success, logOutput.success);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(success);
    }

    @Override
    public String toString() {
      return "LogOutput{success=" + success + '}';
    }

  }

  /**
   * Appends a message to the attempt logs.
   *
   * @param input jobId and attempt to identify the logs and the message to be appended.
   * @return LogOutput with boolean denoting success.
   */
  @ActivityMethod
  LogOutput log(final LogInput input);

}
