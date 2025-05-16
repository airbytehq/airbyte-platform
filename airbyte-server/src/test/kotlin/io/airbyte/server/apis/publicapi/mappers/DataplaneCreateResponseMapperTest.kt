/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.DataplaneCreateResponse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class DataplaneCreateResponseMapperTest {
  @Test
  fun `Should map a DataplaneCreateResponse from the DataplaneService to a DataplaneCreateResponseBody in the public API`() {
    val dataplaneCreateResponse =
      DataplaneCreateResponse().apply {
        regionId = UUID.randomUUID()
        dataplaneId = UUID.randomUUID()
        clientId = UUID.randomUUID().toString()
        clientSecret = UUID.randomUUID().toString()
      }

    val mapped = DataplaneCreateResponseMapper.from(dataplaneCreateResponse)!!

    assertEquals(dataplaneCreateResponse.regionId, mapped.regionId)
    assertEquals(dataplaneCreateResponse.dataplaneId, mapped.dataplaneId)
    assertEquals(dataplaneCreateResponse.clientId, mapped.clientId)
    assertEquals(dataplaneCreateResponse.clientSecret, mapped.clientSecret)
  }
}
