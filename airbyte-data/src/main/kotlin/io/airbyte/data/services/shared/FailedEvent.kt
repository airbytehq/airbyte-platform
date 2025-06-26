/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude
import io.airbyte.config.FailureReason
import io.airbyte.protocol.models.v0.StreamDescriptor
import java.util.Optional

@JsonInclude(JsonInclude.Include.NON_NULL)
class FailedEvent(
  jobId: Long,
  startTimeEpochSeconds: Long,
  endTimeEpochSeconds: Long,
  bytesLoaded: Long,
  recordsLoaded: Long,
  attemptsCount: Int,
  jobType: String,
  statusType: String,
  streams: List<StreamDescriptor>? = null,
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
  fun getFailureReason(): Optional<FailureReason> = failureReason
}
