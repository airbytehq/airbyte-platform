/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.config.Application
import io.airbyte.publicApi.server.generated.models.ApplicationRead
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

class ApplicationReadMapperTest {
  @Test
  fun `from should convert an Application object from keycloak to an ApplicationRead to be returned to the API`() {
    // Given
    val now = OffsetDateTime.now()
    val application =
      Application().apply {
        this.id = UUID.randomUUID().toString()
        this.name = "applicationName"
        this.clientId = UUID.randomUUID().toString()
        this.clientSecret = "shhhhh"
        this.createdOn = now.toString()
      }

    // When
    val applicationRead = toApplicationRead(application)

    // Then
    val expected =
      ApplicationRead(
        id = application.id,
        clientId = application.clientId,
        clientSecret = application.clientSecret,
        createdAt = now.toEpochSecond(),
        name = application.name,
      )
    assertEquals(expected, applicationRead)
  }
}
