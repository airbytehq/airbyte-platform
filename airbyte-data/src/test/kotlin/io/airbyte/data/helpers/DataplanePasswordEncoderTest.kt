/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.helpers

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DataplanePasswordEncoderTest {
  private val encoder = DataplanePasswordEncoder()

  @Test
  fun `encode should return non-empty hash different from raw password`() {
    val rawPassword = "my-secret-password"
    val encoded = encoder.encode(rawPassword)

    assertNotNull(encoded)
    assertTrue(encoded.isNotBlank())
    assertNotEquals(rawPassword, encoded, "Encoded password should not match the raw password")
  }

  @Test
  fun `matches should return true when raw and encoded passwords match`() {
    val rawPassword = "my-secret-password"
    val encoded = encoder.encode(rawPassword)

    assertTrue(
      encoder.matches(rawPassword, encoded),
      "Matches should return true for the correct raw password",
    )
  }

  @Test
  fun `matches should return false for an incorrect password`() {
    val rawPassword = "my-secret-password"
    val encoded = encoder.encode(rawPassword)
    val wrongPassword = "different-password"

    assertFalse(
      encoder.matches(wrongPassword, encoded),
      "Matches should return false for an incorrect raw password",
    )
  }
}
