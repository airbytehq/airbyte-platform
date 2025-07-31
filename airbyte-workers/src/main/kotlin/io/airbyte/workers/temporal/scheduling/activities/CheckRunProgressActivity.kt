/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.util.Objects
import java.util.UUID

/**
 * Activity to check whether a given run (attempt for now) made progress. Output to be used as input
 * to retry logic.
 */
@ActivityInterface
interface CheckRunProgressActivity {
  /**
   * Input object for CheckRunProgressActivity#checkProgress.
   */
  class Input {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var attemptNo: Int? = null
    var connectionId: UUID? = null

    constructor(jobId: Long?, attemptNo: Int?, connectionId: UUID?) {
      this.jobId = jobId
      this.attemptNo = attemptNo
      this.connectionId = connectionId
    }

    constructor()

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val input = o as Input
      return jobId == input.jobId && attemptNo == input.attemptNo && connectionId == input.connectionId
    }

    override fun hashCode(): Int = Objects.hash(jobId, attemptNo, connectionId)

    override fun toString(): String = "Input{jobId=" + jobId + ", attemptNo=" + attemptNo + ", connectionId=" + connectionId + '}'
  }

  /**
   * Output object for CheckRunProgressActivity#checkProgress.
   */
  class Output {
    private var madeProgress: Boolean? = null

    constructor(madeProgress: Boolean?) {
      this.madeProgress = madeProgress
    }

    constructor()

    fun madeProgress(): Boolean? = madeProgress

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val output = o as Output
      return madeProgress == output.madeProgress
    }

    override fun hashCode(): Int = Objects.hashCode(madeProgress)

    override fun toString(): String = "Output{madeProgress=" + madeProgress + '}'
  }

  @ActivityMethod
  fun checkProgress(input: Input): Output
}
