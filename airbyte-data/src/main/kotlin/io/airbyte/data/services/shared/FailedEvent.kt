/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude
import io.airbyte.config.FailureReason
import io.airbyte.domain.models.RejectedRecordsMetadata
import io.airbyte.protocol.models.v0.StreamDescriptor
import java.util.Optional

@JsonInclude(JsonInclude.Include.NON_NULL)
class FailedEvent(
  jobId: Long,
  startTimeEpochSeconds: Long,
  endTimeEpochSeconds: Long,
  bytesLoaded: Long,
  recordsLoaded: Long,
  recordsRejected: Long? = null,
  attemptsCount: Int,
  jobType: String,
  statusType: String,
  streams: List<StreamDescriptor>? = null,
  rejectedRecordsMeta: RejectedRecordsMetadata? = null,
  private val failureReason: Optional<FailureReason>,
) : FinalStatusEvent(
    jobId,
    startTimeEpochSeconds,
    endTimeEpochSeconds,
    bytesLoaded,
    recordsLoaded,
    recordsRejected,
    attemptsCount,
    jobType,
    statusType,
    streams,
    rejectedRecordsMeta,
  ) {
  fun getFailureReason(): Optional<FailureReason> = failureReason
}
