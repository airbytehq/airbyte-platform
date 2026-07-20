/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ScimTokenServiceTest {
  private val tokenService = ScimTokenService()

  @Test
  fun `generates prefixed tokens with 32 random bytes encoded as lowercase hex`() {
    val token = tokenService.generateToken()

    assertTrue(token.matches(Regex("^airbyte_scim_[0-9a-f]{64}$")))
  }

  @Test
  fun `generates distinct tokens`() {
    assertNotEquals(tokenService.generateToken(), tokenService.generateToken())
  }

  @Test
  fun `hashes the entire token with SHA-256 as lowercase hex`() {
    assertEquals(
      "aa99c036d6abaf613d1bdf54aea3c928b2b02face7e1f6352af34568c32800bf",
      tokenService.hashToken("airbyte_scim_test"),
    )
  }

  @Test
  fun `hashing is deterministic and distinguishes different tokens`() {
    val token = tokenService.generateToken()

    assertEquals(tokenService.hashToken(token), tokenService.hashToken(token))
    assertNotEquals(tokenService.hashToken(token), tokenService.hashToken(tokenService.generateToken()))
  }

  @Test
  fun `rejects an empty token`() {
    val exception = assertThrows<IllegalArgumentException> { tokenService.hashToken("") }

    assertEquals("Token cannot be empty", exception.message)
  }
}
