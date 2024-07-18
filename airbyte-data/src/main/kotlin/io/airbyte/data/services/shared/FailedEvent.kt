package io.airbyte.data.services.shared

import io.airbyte.config.FailureReason
import java.util.Optional

class FailedEvent(
  private val jobId: Long,
  private val startTimeEpochSeconds: Long,
  private val endTimeEpochSeconds: Long,
  private val bytesLoaded: Long,
  private val recordsLoaded: Long,
  private val attemptsCount: Int,
  private val jobType: String,
  private val statusType: String,
  private val failureReason: Optional<FailureReason>,
) : FinalStatusEvent(
    jobId,
    startTimeEpochSeconds,
    endTimeEpochSeconds,
    bytesLoaded,
    recordsLoaded,
    attemptsCount,
    jobType,
    statusType,
  ) {
  fun getFailureReason(): Optional<FailureReason> {
    return failureReason
  }
}
