/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude
import io.airbyte.config.JobConfig
import io.airbyte.domain.models.RejectedRecordsMetadata
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType

@JsonInclude(JsonInclude.Include.NON_NULL)
open class FinalStatusEvent(
  private val jobId: Long,
  private val startTimeEpochSeconds: Long,
  private val endTimeEpochSeconds: Long,
  private val bytesLoaded: Long,
  private val recordsLoaded: Long,
  private val recordsRejected: Long? = null,
  private val attemptsCount: Int,
  private val jobType: String,
  private val statusType: String,
  private val streams: List<StreamDescriptor>? = null,
  private val rejectedRecordsMeta: RejectedRecordsMetadata? = null,
) : ConnectionEvent {
  fun getJobId(): Long = jobId

  fun getStartTimeEpochSeconds(): Long = startTimeEpochSeconds

  fun getEndTimeEpochSeconds(): Long = endTimeEpochSeconds

  fun getBytesLoaded(): Long = bytesLoaded

  fun getRecordsLoaded(): Long = recordsLoaded

  fun getAttemptsCount(): Int = attemptsCount

  fun getRecordsRejected(): Long? = recordsRejected

  fun getRejectedRecordsMeta(): RejectedRecordsMetadata? = rejectedRecordsMeta

  fun getStreams(): List<StreamDescriptor>? = streams

  @TypeDef(type = DataType.STRING)
  enum class FinalStatus {
    INCOMPLETE,
    FAILED,
    SUCCEEDED,
    CANCELLED,
  }

  override fun getEventType(): ConnectionEvent.Type =
    when (statusType) {
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
