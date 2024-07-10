package io.airbyte.data.services.shared

class SyncSucceededEvent(
  private val jobId: Long,
  private val startTimeEpochSeconds: Long,
  private val endTimeEpochSeconds: Long,
  private val bytesLoaded: Long,
  private val recordsLoaded: Long,
  private val attemptsCount: Int,
) : ConnectionEvent {
  fun getStartTimeEpochSeconds(): Long {
    return startTimeEpochSeconds
  }

  fun getEndTimeEpochSeconds(): Long {
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

  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.SYNC_SUCCEEDED
  }
}
