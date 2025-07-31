/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.workers.models.JobInput
import io.airbyte.workers.models.SyncJobCheckConnectionInputs
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.util.Objects

/**
 * GenerateInputActivity.
 */
@ActivityInterface
interface GenerateInputActivity {
  /**
   * SyncInput.
   */
  class SyncInput {
    @JvmField
    var attemptId: Int = 0

    @JvmField
    var jobId: Long = 0

    constructor()

    constructor(attemptId: Int, jobId: Long) {
      this.attemptId = attemptId
      this.jobId = jobId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val syncInput = o as SyncInput
      return attemptId == syncInput.attemptId && jobId == syncInput.jobId
    }

    override fun hashCode(): Int = Objects.hash(attemptId, jobId)

    override fun toString(): String = "SyncInput{attemptId=" + attemptId + ", jobId=" + jobId + '}'
  }

  /**
   * SyncInputWithAttemptNumber.
   */
  class SyncInputWithAttemptNumber {
    @JvmField
    var attemptNumber: Int = 0

    @JvmField
    var jobId: Long = 0

    constructor()

    constructor(attemptNumber: Int, jobId: Long) {
      this.attemptNumber = attemptNumber
      this.jobId = jobId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as SyncInputWithAttemptNumber
      return attemptNumber == that.attemptNumber && jobId == that.jobId
    }

    override fun hashCode(): Int = Objects.hash(attemptNumber, jobId)

    override fun toString(): String = "SyncInputWithAttemptNumber{attemptNumber=" + attemptNumber + ", jobId=" + jobId + '}'
  }

  /**
   * This generate the input needed by the child sync workflow.
   */
  @ActivityMethod
  @Throws(Exception::class)
  fun getSyncWorkflowInput(input: SyncInput): JobInput

  /**
   * This generate the input needed by the child sync workflow.
   */
  @ActivityMethod
  @Throws(Exception::class)
  fun getSyncWorkflowInputWithAttemptNumber(input: SyncInputWithAttemptNumber): JobInput

  @ActivityMethod
  fun getCheckConnectionInputs(input: SyncInputWithAttemptNumber): SyncJobCheckConnectionInputs
}
