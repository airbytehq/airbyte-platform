package io.airbyte.data.services.shared

data class SyncCancelledEvent(
  private val jobId: Long,
  private val cancelTimeEpochSeconds: Long,
) : ConnectionEvent {
  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.SYNC_CANCELLED
  }
}
