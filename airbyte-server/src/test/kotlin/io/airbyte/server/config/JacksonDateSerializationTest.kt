/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.DataplaneGroupRead
import io.airbyte.api.model.generated.DataplaneRead
import io.airbyte.api.model.generated.OrganizationPaymentConfigRead
import io.airbyte.api.model.generated.UserRead
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

/**
 * Verifies that Micronaut's Jackson ObjectMapper serializes OffsetDateTime fields as ISO-8601
 * strings (e.g. "2026-02-05T12:00:00Z") instead of numeric timestamps (e.g. 1738756800.0).
 *
 * This is critical because:
 * - The OpenAPI spec declares these as type: string, format: date-time
 * - Python API clients use isoparse() which only handles strings, not floats
 * - Jackson's default (WRITE_DATES_AS_TIMESTAMPS=true) would serialize as numeric
 *
 * This test lives in a separate file (rather than in each controller's test) because it requires
 * @MicronautTest to inject the real ObjectMapper configured by application.yml. The individual
 * controller tests are plain unit tests using mockk, which don't boot the Micronaut context and
 * therefore can't verify the Jackson serialization wiring. Centralizing these tests here ensures
 * we're testing the actual production configuration, not a hand-built copy.
 */
@MicronautTest
class JacksonDateSerializationTest {
  @Inject
  lateinit var objectMapper: ObjectMapper

  private val testTimestamp = OffsetDateTime.of(2026, 2, 5, 12, 0, 0, 0, ZoneOffset.UTC)
  private val expectedIsoSubstring = "2026-02-05T12:00:00"

  @Test
  fun `UserRead agenticEnabledAt serializes as ISO string`() {
    val userRead =
      UserRead()
        .userId(UUID.randomUUID())
        .email("test@test.com")
        .name("test")
        .agenticEnabledAt(testTimestamp)

    val json = objectMapper.writeValueAsString(userRead)

    assertIsIsoString(json, "agenticEnabledAt")
  }

  @Test
  fun `DataplaneGroupRead createdAt and updatedAt serialize as ISO strings`() {
    val read =
      DataplaneGroupRead()
        .dataplaneGroupId(UUID.randomUUID())
        .organizationId(UUID.randomUUID())
        .name("test-group")
        .enabled(true)
        .createdAt(testTimestamp)
        .updatedAt(testTimestamp)

    val json = objectMapper.writeValueAsString(read)

    assertIsIsoString(json, "createdAt")
    assertIsIsoString(json, "updatedAt")
  }

  @Test
  fun `DataplaneRead createdAt and updatedAt serialize as ISO strings`() {
    val read =
      DataplaneRead()
        .dataplaneId(UUID.randomUUID())
        .dataplaneGroupId(UUID.randomUUID())
        .name("test-dataplane")
        .enabled(true)
        .createdAt(testTimestamp)
        .updatedAt(testTimestamp)

    val json = objectMapper.writeValueAsString(read)

    assertIsIsoString(json, "createdAt")
    assertIsIsoString(json, "updatedAt")
  }

  @Test
  fun `OrganizationPaymentConfigRead gracePeriodEndAt serializes as ISO string`() {
    val read =
      OrganizationPaymentConfigRead()
        .organizationId(UUID.randomUUID())
        .gracePeriodEndAt(testTimestamp)

    val json = objectMapper.writeValueAsString(read)

    assertIsIsoString(json, "gracePeriodEndAt")
  }

  private fun assertIsIsoString(
    json: String,
    fieldName: String,
  ) {
    assertTrue(json.contains(expectedIsoSubstring), "$fieldName should contain ISO-8601 date string, got: $json")
    // Verify it's not a numeric timestamp (would look like "fieldName":1738...)
    assertFalse(
      Regex("\"$fieldName\"\\s*:\\s*\\d").containsMatchIn(json),
      "$fieldName should not be a numeric timestamp, got: $json",
    )
  }
}
