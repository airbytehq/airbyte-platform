package io.airbyte.data.services.shared

import io.airbyte.config.JobConfig
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType

open class FinalStatusEvent(
  private val jobId: Long,
  private val startTimeEpochSeconds: Long,
  private val endTimeEpochSeconds: Long,
  private val bytesLoaded: Long,
  private val recordsLoaded: Long,
  private val attemptsCount: Int,
  private val jobType: String,
  private val statusType: String,
) : ConnectionEvent {
  fun getJobId(): Long {
    return jobId
  }

  fun getStartTimeEpochSeconds(): Long {
    return startTimeEpochSeconds
  }

  fun getEndTimeEpochSeconds(): Long {
    return endTimeEpochSeconds
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

  @TypeDef(type = DataType.STRING)
  enum class FinalStatus {
    INCOMPLETE,
    FAILED,
    SUCCEEDED,
    CANCELLED,
  }

  override fun getEventType(): ConnectionEvent.Type {
    return when (statusType) {
      FinalStatus.SUCCEEDED.name -> {
        when (jobType) {
          JobConfig.ConfigType.SYNC.name -> ConnectionEvent.Type.SYNC_SUCCEEDED
          JobConfig.ConfigType.REFRESH.name -> ConnectionEvent.Type.REFRESH_SUCCEEDED
          JobConfig.ConfigType.CLEAR.name, JobConfig.ConfigType.RESET_CONNECTION.name -> ConnectionEvent.Type.CLEAR_SUCCEEDED
          else -> ConnectionEvent.Type.UNKNOWN
        }
      }
      FinalStatus.FAILED.name -> {
        when (jobType) {
          JobConfig.ConfigType.SYNC.name -> ConnectionEvent.Type.SYNC_FAILED
          JobConfig.ConfigType.REFRESH.name -> ConnectionEvent.Type.REFRESH_FAILED
          JobConfig.ConfigType.CLEAR.name, JobConfig.ConfigType.RESET_CONNECTION.name -> ConnectionEvent.Type.CLEAR_FAILED
          else -> ConnectionEvent.Type.UNKNOWN
        }
      }
      FinalStatus.INCOMPLETE.name -> {
        when (jobType) {
          JobConfig.ConfigType.SYNC.name -> ConnectionEvent.Type.SYNC_INCOMPLETE
          JobConfig.ConfigType.REFRESH.name -> ConnectionEvent.Type.REFRESH_INCOMPLETE
          JobConfig.ConfigType.CLEAR.name, JobConfig.ConfigType.RESET_CONNECTION.name -> ConnectionEvent.Type.CLEAR_INCOMPLETE
          else -> ConnectionEvent.Type.UNKNOWN
        }
      }
      FinalStatus.CANCELLED.name -> {
        when (jobType) {
          JobConfig.ConfigType.SYNC.name -> ConnectionEvent.Type.SYNC_CANCELLED
          JobConfig.ConfigType.REFRESH.name -> ConnectionEvent.Type.REFRESH_CANCELLED
          JobConfig.ConfigType.CLEAR.name, JobConfig.ConfigType.RESET_CONNECTION.name -> ConnectionEvent.Type.CLEAR_CANCELLED
          else -> ConnectionEvent.Type.UNKNOWN
        }
      }
      else -> ConnectionEvent.Type.UNKNOWN
    }
  }
}
