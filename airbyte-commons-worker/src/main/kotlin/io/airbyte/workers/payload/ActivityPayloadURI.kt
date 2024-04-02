package io.airbyte.workers.payload

import java.util.UUID

enum class ActivityPayloadURIVersion {
  V1,
}

class ActivityPayloadURI(
  val id: String,
  val version: String = ActivityPayloadURIVersion.V1.name,
) {
  fun v1(
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Int,
    payloadName: String,
  ): ActivityPayloadURI {
    return ActivityPayloadURI("${connectionId}_${jobId}_${attemptNumber}_$payloadName", ActivityPayloadURIVersion.V1.name)
  }
}
