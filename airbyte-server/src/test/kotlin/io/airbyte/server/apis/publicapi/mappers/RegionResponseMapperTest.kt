/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.config.DataplaneGroup
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class RegionResponseMapperTest {
  @Test
  fun `Should map a DataplaneGroup to a RegionResponse`() {
    val now = 1748886193L
    val dataplaneGroup =
      DataplaneGroup().apply {
        id = UUID.randomUUID()
        name = "us-east"
        organizationId = UUID.randomUUID()
        enabled = true
        createdAt = now
        updatedAt = now
      }

    val mapped = RegionResponseMapper.from(dataplaneGroup)!!

    assertEquals(dataplaneGroup.id, mapped.regionId)
    assertEquals(dataplaneGroup.name, mapped.name)
    assertEquals(dataplaneGroup.organizationId, mapped.organizationId)
    assertEquals(dataplaneGroup.enabled, mapped.enabled)

    assertEquals(now, Instant.parse(mapped.createdAt).epochSecond)
    assertEquals(now, Instant.parse(mapped.updatedAt).epochSecond)
  }
}
