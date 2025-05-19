/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.storage.activities

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
      jobId: Long,
      attemptNumber: Int,
      payloadName: String,
    ): ActivityPayloadURI = ActivityPayloadURI("${connectionId}_${jobId}_${attemptNumber}_$payloadName", ActivityPayloadURIVersion.V1.name)

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

  fun toOpenApi(): OpenApi =
    OpenApi()
      .withId(id)
      .withVersion(version)
}
