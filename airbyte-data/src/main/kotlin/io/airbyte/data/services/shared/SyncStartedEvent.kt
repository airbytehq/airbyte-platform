package io.airbyte.data.services.shared

class SyncStartedEvent(
  private val jobId: Long,
  private val startTimeEpochSeconds: Long,
) : ConnectionEvent {
  fun getJobId(): Long {
    return jobId
  }

  fun getStartTimeEpochSeconds(): Long {
    return startTimeEpochSeconds
  }

  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.SYNC_STARTED
  }
}
