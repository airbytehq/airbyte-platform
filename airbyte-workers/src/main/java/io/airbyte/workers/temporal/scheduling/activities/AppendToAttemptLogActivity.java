/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class LogInput {

    private Long jobId;
    private Integer attemptNumber;
    private String message;
    private LogLevel level;

  }

  /**
   * Output for append to attempt log activity method.
   */
  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  class LogOutput {

    private Boolean success;

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
