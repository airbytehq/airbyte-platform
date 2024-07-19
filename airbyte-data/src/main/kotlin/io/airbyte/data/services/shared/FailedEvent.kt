package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude
import io.airbyte.config.FailureReason
import io.airbyte.protocol.models.StreamDescriptor
import java.util.Optional

@JsonInclude(JsonInclude.Include.NON_NULL)
class FailedEvent(
  private val jobId: Long,
  private val startTimeEpochSeconds: Long,
  private val endTimeEpochSeconds: Long,
  private val bytesLoaded: Long,
  private val recordsLoaded: Long,
  private val attemptsCount: Int,
  private val jobType: String,
  private val statusType: String,
  private val streams: List<StreamDescriptor>? = null,
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
    streams,
  ) {
  fun getFailureReason(): Optional<FailureReason> {
    return failureReason
  }
}
