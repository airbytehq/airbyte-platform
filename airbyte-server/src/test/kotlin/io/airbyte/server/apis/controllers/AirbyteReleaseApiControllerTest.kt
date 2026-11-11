/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.models.AirbyteReleaseCreateRequestBody
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.time.ZoneOffset

internal class AirbyteReleaseApiControllerTest {
  @Test
  fun `createAirbyteRelease is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      AirbyteReleaseApiController().createAirbyteRelease(
        AirbyteReleaseCreateRequestBody(
          version = "2.1.0",
          sourceCommit = "0123456789abcdef0123456789abcdef01234567",
          publishedAt = OffsetDateTime.of(2026, 11, 6, 17, 30, 0, 0, ZoneOffset.UTC),
        ),
      )
    }
  }
}
