/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.config.DataplaneGroup
import io.airbyte.publicApi.server.generated.models.RegionResponse
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

object RegionResponseMapper {
  fun from(dataplaneGroup: DataplaneGroup?): RegionResponse? {
    if (dataplaneGroup == null) return null

    return RegionResponse(
      regionId = dataplaneGroup.id,
      name = dataplaneGroup.name,
      organizationId = dataplaneGroup.organizationId,
      enabled = dataplaneGroup.enabled,
      createdAt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(dataplaneGroup.createdAt), ZoneOffset.UTC).toString(),
      updatedAt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(dataplaneGroup.updatedAt), ZoneOffset.UTC).toString(),
    )
  }
}
