/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.util.Objects

/**
 * Activity for adding messages to a given attempt log from workflows.
 */
@ActivityInterface
interface AppendToAttemptLogActivity {
  /**
   * Supported log levels.
   */
  enum class LogLevel {
    ERROR,
    INFO,
    WARN,
  }

  /**
   * Input for append to attempt log activity method.
   */
  class LogInput {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var attemptNumber: Int? = null

    @JvmField
    var message: String? = null

    @JvmField
    var level: LogLevel? = null

    constructor()

    constructor(jobId: Long?, attemptNumber: Int?, message: String?, level: LogLevel?) {
      this.jobId = jobId
      this.attemptNumber = attemptNumber
      this.message = message
      this.level = level
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val logInput = o as LogInput
      return jobId == logInput.jobId &&
        attemptNumber == logInput.attemptNumber &&
        message == logInput.message &&
        level == logInput.level
    }

    override fun hashCode(): Int = Objects.hash(jobId, attemptNumber, message, level)

    override fun toString(): String =
      "LogInput{jobId=" + jobId + ", attemptNumber=" + attemptNumber + ", message='" + message + '\'' + ", level=" + level + '}'
  }

  /**
   * Output for append to attempt log activity method.
   */
  class LogOutput {
    @JvmField
    var success: Boolean? = null

    constructor()

    constructor(success: Boolean?) {
      this.success = success
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val logOutput = o as LogOutput
      return success == logOutput.success
    }

    override fun hashCode(): Int = Objects.hashCode(success)

    override fun toString(): String = "LogOutput{success=" + success + '}'
  }

  /**
   * Appends a message to the attempt logs.
   *
   * @param input jobId and attempt to identify the logs and the message to be appended.
   * @return LogOutput with boolean denoting success.
   */
  @ActivityMethod
  fun log(input: LogInput): LogOutput
}
