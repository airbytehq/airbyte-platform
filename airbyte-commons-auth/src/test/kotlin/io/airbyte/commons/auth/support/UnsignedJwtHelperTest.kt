/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.support

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.Base64
import kotlin.time.Duration.Companion.hours

class UnsignedJwtHelperTest {
  private val now: Instant = Instant.now()
  private val clock: Clock = Clock.fixed(now, ZoneOffset.UTC)

  @Nested
  inner class BuildUnsignedJwtWithExpClaim {
    @Test
    fun `should build an unsigned JWT with a specified expiration claim`() {
      val secondsFromNow = 1.hours.inWholeSeconds
      val jwt = UnsignedJwtHelper.buildUnsignedJwtWithExpClaim(secondsFromNow, clock)

      val parts = jwt.split(".")
      Assertions.assertEquals(3, parts.size)

      val header = String(Base64.getUrlDecoder().decode(parts[0]))
      Assertions.assertTrue(header.contains("alg\":\"none"))

      val payload = String(Base64.getUrlDecoder().decode(parts[1]))
      val expectedExpValue = now.plusSeconds(secondsFromNow).epochSecond
      Assertions.assertTrue(payload.contains("exp\":$expectedExpValue"))

      val signature = parts[2]
      Assertions.assertEquals("", signature)
    }
  }
}
