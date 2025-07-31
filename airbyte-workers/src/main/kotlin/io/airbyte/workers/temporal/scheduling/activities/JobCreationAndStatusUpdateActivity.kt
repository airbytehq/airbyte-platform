/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.StandardSyncOutput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.util.Objects
import java.util.UUID

/**
 * JobCreationAndStatusUpdateActivity.
 */
@ActivityInterface
interface JobCreationAndStatusUpdateActivity {
  /**
   * JobCreationInput.
   */
  class JobCreationInput {
    @JvmField
    var connectionId: UUID? = null

    @JvmField
    var isScheduled: Boolean = false

    constructor()

    constructor(connectionId: UUID?, isScheduled: Boolean) {
      this.connectionId = connectionId
      this.isScheduled = isScheduled
    }

    fun connectionId(): UUID? = connectionId

    override fun equals(o: Any?): Boolean {
      if (this === o) {
        return true
      }
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as JobCreationInput
      return isScheduled == that.isScheduled && connectionId == that.connectionId
    }

    override fun hashCode(): Int = Objects.hash(connectionId, isScheduled)

    override fun toString(): String =
      (
        "JobCreationInput{" +
          "connectionId=" + connectionId +
          ", isScheduled=" + isScheduled +
          '}'
      )
  }

  /**
   * JobCreationOutput.
   */
  class JobCreationOutput {
    @JvmField
    var jobId: Long? = null

    constructor()

    constructor(jobId: Long?) {
      this.jobId = jobId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as JobCreationOutput
      return jobId == that.jobId
    }

    override fun hashCode(): Int = Objects.hashCode(jobId)

    override fun toString(): String = "JobCreationOutput{jobId=" + jobId + '}'
  }

  /**
   * Creates a new job.
   *
   * @param input - POJO that contains the connections
   * @return a POJO that contains the jobId
   */
  @ActivityMethod
  fun createNewJob(input: JobCreationInput): JobCreationOutput

  /**
   * AttemptCreationInput.
   */
  class AttemptCreationInput {
    @JvmField
    var jobId: Long? = null

    constructor()

    constructor(jobId: Long?) {
      this.jobId = jobId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as AttemptCreationInput
      return jobId == that.jobId
    }

    override fun hashCode(): Int = Objects.hashCode(jobId)

    override fun toString(): String = "AttemptCreationInput{jobId=" + jobId + '}'
  }

  /**
   * AttemptNumberCreationOutput.
   */
  class AttemptNumberCreationOutput {
    @JvmField
    var attemptNumber: Int? = null

    constructor()

    constructor(attemptNumber: Int?) {
      this.attemptNumber = attemptNumber
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as AttemptNumberCreationOutput
      return attemptNumber == that.attemptNumber
    }

    override fun hashCode(): Int = Objects.hashCode(attemptNumber)

    override fun toString(): String = "AttemptNumberCreationOutput{attemptNumber=" + attemptNumber + '}'
  }

  /**
   * Create a new attempt for a given job ID.
   *
   * @param input POJO containing the jobId
   * @return A POJO containing the attemptNumber
   */
  @ActivityMethod
  @Throws(RetryableException::class)
  fun createNewAttemptNumber(input: AttemptCreationInput): AttemptNumberCreationOutput

  /**
   * JobSuccessInputWithAttemptNumber.
   */
  class JobSuccessInputWithAttemptNumber {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var attemptNumber: Int? = null

    @JvmField
    var connectionId: UUID? = null

    @JvmField
    var standardSyncOutput: StandardSyncOutput? = null

    constructor()

    constructor(jobId: Long?, attemptNumber: Int?, connectionId: UUID?, standardSyncOutput: StandardSyncOutput?) {
      this.jobId = jobId
      this.attemptNumber = attemptNumber
      this.connectionId = connectionId
      this.standardSyncOutput = standardSyncOutput
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as JobSuccessInputWithAttemptNumber
      return jobId == that.jobId &&
        attemptNumber == that.attemptNumber &&
        connectionId == that.connectionId &&
        standardSyncOutput == that.standardSyncOutput
    }

    override fun hashCode(): Int = Objects.hash(jobId, attemptNumber, connectionId, standardSyncOutput)

    override fun toString(): String =
      (
        "JobSuccessInputWithAttemptNumber{" +
          "jobId=" + jobId +
          ", attemptNumber=" + attemptNumber +
          ", connectionId=" + connectionId +
          ", standardSyncOutput=" + standardSyncOutput +
          '}'
      )
  }

  /**
   * Set a job status as successful.
   */
  @ActivityMethod
  fun jobSuccessWithAttemptNumber(input: JobSuccessInputWithAttemptNumber)

  /**
   * JobFailureInput.
   */
  class JobFailureInput {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var attemptNumber: Int? = null

    @JvmField
    var connectionId: UUID? = null

    @JvmField
    var reason: String? = null

    constructor()

    constructor(jobId: Long?, attemptNumber: Int?, connectionId: UUID?, reason: String?) {
      this.jobId = jobId
      this.attemptNumber = attemptNumber
      this.connectionId = connectionId
      this.reason = reason
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as JobFailureInput
      return jobId == that.jobId && attemptNumber == that.attemptNumber && connectionId == that.connectionId && reason == that.reason
    }

    override fun hashCode(): Int = Objects.hash(jobId, attemptNumber, connectionId, reason)

    override fun toString(): String =
      (
        "JobFailureInput{" +
          "jobId=" + jobId +
          ", attemptNumber=" + attemptNumber +
          ", connectionId=" + connectionId +
          ", reason='" + reason + '\'' +
          '}'
      )
  }

  /**
   * Set a job status as failed.
   */
  @ActivityMethod
  fun jobFailure(input: JobFailureInput)

  /**
   * AttemptNumberFailureInput.
   */
  class AttemptNumberFailureInput {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var attemptNumber: Int? = null

    @JvmField
    var connectionId: UUID? = null

    @JvmField
    var standardSyncOutput: StandardSyncOutput? = null

    @JvmField
    var attemptFailureSummary: AttemptFailureSummary? = null

    constructor()

    constructor(
      jobId: Long?,
      attemptNumber: Int?,
      connectionId: UUID?,
      standardSyncOutput: StandardSyncOutput?,
      attemptFailureSummary: AttemptFailureSummary?,
    ) {
      this.jobId = jobId
      this.attemptNumber = attemptNumber
      this.connectionId = connectionId
      this.standardSyncOutput = standardSyncOutput
      this.attemptFailureSummary = attemptFailureSummary
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as AttemptNumberFailureInput
      return jobId == that.jobId &&
        attemptNumber == that.attemptNumber &&
        connectionId == that.connectionId &&
        standardSyncOutput == that.standardSyncOutput &&
        attemptFailureSummary == that.attemptFailureSummary
    }

    override fun hashCode(): Int = Objects.hash(jobId, attemptNumber, connectionId, standardSyncOutput, attemptFailureSummary)

    override fun toString(): String =
      (
        "AttemptNumberFailureInput{" +
          "jobId=" + jobId +
          ", attemptNumber=" + attemptNumber +
          ", connectionId=" + connectionId +
          ", standardSyncOutput=" + standardSyncOutput +
          ", attemptFailureSummary=" + attemptFailureSummary +
          '}'
      )
  }

  /**
   * Set an attempt status as failed.
   */
  @ActivityMethod
  fun attemptFailureWithAttemptNumber(input: AttemptNumberFailureInput)

  /**
   * JobCancelledInputWithAttemptNumber.
   */
  class JobCancelledInputWithAttemptNumber {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var attemptNumber: Int? = null

    @JvmField
    var connectionId: UUID? = null

    @JvmField
    var attemptFailureSummary: AttemptFailureSummary? = null

    constructor()

    constructor(jobId: Long?, attemptNumber: Int?, connectionId: UUID?, attemptFailureSummary: AttemptFailureSummary?) {
      this.jobId = jobId
      this.attemptNumber = attemptNumber
      this.connectionId = connectionId
      this.attemptFailureSummary = attemptFailureSummary
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as JobCancelledInputWithAttemptNumber
      return jobId == that.jobId &&
        attemptNumber == that.attemptNumber &&
        connectionId == that.connectionId &&
        attemptFailureSummary == that.attemptFailureSummary
    }

    override fun hashCode(): Int = Objects.hash(jobId, attemptNumber, connectionId, attemptFailureSummary)

    override fun toString(): String =
      (
        "JobCancelledInputWithAttemptNumber{" +
          "jobId=" + jobId +
          ", attemptNumber=" + attemptNumber +
          ", connectionId=" + connectionId +
          ", attemptFailureSummary=" + attemptFailureSummary +
          '}'
      )
  }

  /**
   * Set a job status as cancelled.
   */
  @ActivityMethod
  fun jobCancelledWithAttemptNumber(input: JobCancelledInputWithAttemptNumber)

  /**
   * ReportJobStartInput.
   */
  class ReportJobStartInput {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var connectionId: UUID? = null

    constructor()

    constructor(jobId: Long?, connectionId: UUID?) {
      this.jobId = jobId
      this.connectionId = connectionId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as ReportJobStartInput
      return jobId == that.jobId && connectionId == that.connectionId
    }

    override fun hashCode(): Int = Objects.hash(jobId, connectionId)

    override fun toString(): String = "ReportJobStartInput{jobId=" + jobId + ", connectionId=" + connectionId + '}'
  }

  @ActivityMethod
  fun reportJobStart(reportJobStartInput: ReportJobStartInput)

  /**
   * EnsureCleanJobStateInput.
   */
  class EnsureCleanJobStateInput {
    @JvmField
    var connectionId: UUID? = null

    constructor()

    constructor(connectionId: UUID?) {
      this.connectionId = connectionId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as EnsureCleanJobStateInput
      return connectionId == that.connectionId
    }

    override fun hashCode(): Int = Objects.hashCode(connectionId)

    override fun toString(): String = "EnsureCleanJobStateInput{connectionId=" + connectionId + '}'
  }

  @ActivityMethod
  fun ensureCleanJobState(input: EnsureCleanJobStateInput)

  /**
   * JobCheckFailureInput.
   */
  class JobCheckFailureInput {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var attemptId: Int? = null

    @JvmField
    var connectionId: UUID? = null

    constructor()

    constructor(jobId: Long?, attemptId: Int?, connectionId: UUID?) {
      this.jobId = jobId
      this.attemptId = attemptId
      this.connectionId = connectionId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as JobCheckFailureInput
      return jobId == that.jobId && attemptId == that.attemptId && connectionId == that.connectionId
    }

    override fun hashCode(): Int = Objects.hash(jobId, attemptId, connectionId)

    override fun toString(): String = "JobCheckFailureInput{jobId=" + jobId + ", attemptId=" + attemptId + ", connectionId=" + connectionId + '}'
  }

  @ActivityMethod
  fun isLastJobOrAttemptFailure(input: JobCheckFailureInput): Boolean
}
