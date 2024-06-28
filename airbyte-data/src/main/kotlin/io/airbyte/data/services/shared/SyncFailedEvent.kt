package io.airbyte.data.services.shared

import io.airbyte.config.FailureReason
import java.util.Optional

class SyncFailedEvent(
  private val jobId: Long,
  private val startTimeEpochSeconds: Long,
  private val endTimeEpochSeconds: Long,
  private val bytesLoaded: Long,
  private val recordsLoaded: Long,
  private val attemptsCount: Int,
  private val failureReason: Optional<FailureReason>,
) : ConnectionEvent {
  fun startTimeEpochSeconds(): Long {
    return this.startTimeEpochSeconds
  }

  fun endTimeEpochSeconds(): Long {
    return endTimeEpochSeconds
  }

  fun getJobId(): Long {
    return jobId
  }

  fun getBytesLoaded(): Long {
    return bytesLoaded
  }

  fun getRecordsLoaded(): Long {
    return recordsLoaded
  }

  fun getAttemptsCount(): Int {
    return attemptsCount
  }

  fun getFailureReason(): Optional<FailureReason> {
    return failureReason
  }

  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.SYNC_FAILED
  }
}
