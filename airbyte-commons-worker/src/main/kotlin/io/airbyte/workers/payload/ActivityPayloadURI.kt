package io.airbyte.workers.payload

import java.util.UUID
import io.airbyte.config.ActivityPayloadURI as OpenApi

enum class ActivityPayloadURIVersion {
  V1,
}

class ActivityPayloadURI(
  val id: String,
  val version: String = ActivityPayloadURIVersion.V1.name,
) {
  companion object Factory {
    @JvmStatic
    fun v1(
      connectionId: UUID,
      jobId: String,
      attemptNumber: Long,
      payloadName: String,
    ): ActivityPayloadURI {
      return ActivityPayloadURI("${connectionId}_${jobId}_${attemptNumber}_$payloadName", ActivityPayloadURIVersion.V1.name)
    }

    @JvmStatic
    fun fromOpenApi(dto: OpenApi?): ActivityPayloadURI? {
      if (dto == null || dto.version == null || dto.id == null) {
        return null
      }

      return ActivityPayloadURI(
        version = dto.version,
        id = dto.id,
      )
    }
  }

  fun toOpenApi(): OpenApi {
    return OpenApi()
      .withId(id)
      .withVersion(version)
  }
}
