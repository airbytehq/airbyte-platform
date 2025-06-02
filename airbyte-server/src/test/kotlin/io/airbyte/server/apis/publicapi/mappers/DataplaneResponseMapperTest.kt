/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.config.Dataplane
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class DataplaneResponseMapperTest {
  @Test
  fun `Should map a Dataplane from the config model to a DataplaneResponse in the public API`() {
    val now = 1748886193L
    val dataplane =
      Dataplane().apply {
        id = UUID.randomUUID()
        dataplaneGroupId = UUID.randomUUID()
        name = "test-dataplane"
        enabled = true
        createdAt = now
        updatedAt = now
      }

    val mapped = DataplaneResponseMapper.from(dataplane)!!

    assertEquals(dataplane.id, mapped.dataplaneId)
    assertEquals(dataplane.dataplaneGroupId, mapped.regionId)
    assertEquals(dataplane.name, mapped.name)
    assertEquals(dataplane.enabled, mapped.enabled)

    assertEquals(now, Instant.parse(mapped.createdAt).epochSecond)
    assertEquals(now, Instant.parse(mapped.updatedAt).epochSecond)
  }
}
