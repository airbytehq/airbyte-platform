/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.config.Dataplane
import io.airbyte.publicApi.server.generated.models.DataplaneResponse
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

object DataplaneResponseMapper {
  fun from(dataplane: Dataplane?): DataplaneResponse? {
    if (dataplane == null) return null

    return DataplaneResponse(
      dataplaneId = dataplane.id,
      regionId = dataplane.dataplaneGroupId,
      name = dataplane.name,
      enabled = dataplane.enabled,
      createdAt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(dataplane.createdAt), ZoneOffset.UTC).toString(),
      updatedAt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(dataplane.updatedAt), ZoneOffset.UTC).toString(),
    )
  }
}
