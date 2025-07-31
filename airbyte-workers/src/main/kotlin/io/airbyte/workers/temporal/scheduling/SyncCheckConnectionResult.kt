/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling

import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.temporal.sync.SyncOutputProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles

/**
 * SyncCheckConnectionFailure.
 */
class SyncCheckConnectionResult {
  private val jobId: Long
  private val attemptId: Int
  private var failureOutput: ConnectorJobOutput? = null
  private var origin: FailureReason.FailureOrigin? = null

  constructor(jobRunConfig: JobRunConfig) {
    var jobId = 0L
    var attemptId = 0

    try {
      jobId = jobRunConfig.jobId.toLong()
      attemptId = Math.toIntExact(jobRunConfig.attemptId)
    } catch (e: Exception) {
      // In tests, the jobId and attemptId may not be available
      log.warn("Cannot determine jobId or attemptId: " + e.message)
    }

    this.jobId = jobId
    this.attemptId = attemptId
  }

  constructor(jobId: Long, attemptId: Int) {
    this.jobId = jobId
    this.attemptId = attemptId
  }

  val isFailed: Boolean
    get() = this.origin != null && this.failureOutput != null

  fun setFailureOrigin(origin: FailureReason.FailureOrigin?) {
    this.origin = origin
  }

  fun setFailureOutput(failureOutput: ConnectorJobOutput?) {
    this.failureOutput = failureOutput
  }

  /**
   * Build failure out.
   *
   * @return sync output
   */
  fun buildFailureOutput(): StandardSyncOutput {
    if (!this.isFailed) {
      throw RuntimeException("Cannot build failure output without a failure origin and output")
    }

    val syncOutput =
      StandardSyncOutput()
        .withStandardSyncSummary(SyncOutputProvider.EMPTY_FAILED_SYNC)

    if (failureOutput!!.failureReason != null) {
      syncOutput.failures = listOf<FailureReason>(failureOutput!!.failureReason.withFailureOrigin(origin))
    } else {
      val checkOutput = failureOutput!!.checkConnection
      val ex: Exception = IllegalArgumentException(checkOutput.message)
      val checkFailureReason = FailureHelper.checkFailure(ex, jobId, attemptId, origin)
      syncOutput.failures = listOf(checkFailureReason)
    }

    return syncOutput
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    /**
     * Test if output failed.
     *
     * @param output output
     * @return true, if failed. otherwise, false.
     */
    @JvmStatic
    fun isOutputFailed(output: ConnectorJobOutput): Boolean {
      require(output.outputType == ConnectorJobOutput.OutputType.CHECK_CONNECTION) { "Output type must be CHECK_CONNECTION" }

      return output.failureReason != null || output.checkConnection.status == StandardCheckConnectionOutput.Status.FAILED
    }
  }
}
