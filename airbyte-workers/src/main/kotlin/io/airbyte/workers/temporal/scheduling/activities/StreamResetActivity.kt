/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.util.Objects
import java.util.UUID

/**
 * StreamResetActivity.
 */
@ActivityInterface
interface StreamResetActivity {
  /**
   * DeleteStreamResetRecordsForJobInput.
   */
  class DeleteStreamResetRecordsForJobInput {
    @JvmField
    var connectionId: UUID? = null

    @JvmField
    var jobId: Long? = null

    constructor()

    constructor(connectionId: UUID?, jobId: Long?) {
      this.connectionId = connectionId
      this.jobId = jobId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as DeleteStreamResetRecordsForJobInput
      return connectionId == that.connectionId && jobId == that.jobId
    }

    override fun hashCode(): Int = Objects.hash(connectionId, jobId)

    override fun toString(): String = "DeleteStreamResetRecordsForJobInput{connectionId=" + connectionId + ", jobId=" + jobId + '}'
  }

  /**
   * Deletes the stream_reset record corresponding to each stream descriptor passed in.
   */
  @ActivityMethod
  fun deleteStreamResetRecordsForJob(input: DeleteStreamResetRecordsForJobInput)
}
