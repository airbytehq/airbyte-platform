package io.airbyte.data.services.shared

class SyncStartedEvent(
  private val jobId: Long,
  private val startTimeEpochSeconds: Long,
) : ConnectionEvent {
  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.SYNC_STARTED
  }
}
