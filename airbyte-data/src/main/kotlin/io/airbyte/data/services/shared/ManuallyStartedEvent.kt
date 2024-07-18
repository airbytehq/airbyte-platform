package io.airbyte.data.services.shared

import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.protocol.models.StreamDescriptor

class ManuallyStartedEvent(
  private val jobId: Long,
  private val startTimeEpochSeconds: Long,
  private val jobType: String,
  private val streams: List<StreamDescriptor>?,
) : ConnectionEvent {
  fun getJobId(): Long {
    return jobId
  }

  fun getStartTimeEpochSeconds(): Long {
    return startTimeEpochSeconds
  }

  fun getStreams(): List<StreamDescriptor>? {
    return streams
  }

  override fun getEventType(): ConnectionEvent.Type {
    return when (jobType) {
      ConfigType.SYNC.name -> ConnectionEvent.Type.SYNC_STARTED
      ConfigType.REFRESH.name -> ConnectionEvent.Type.REFRESH_STARTED
      ConfigType.CLEAR.name -> ConnectionEvent.Type.CLEAR_STARTED
      else -> ConnectionEvent.Type.UNKNOWN
    }
  }
}
