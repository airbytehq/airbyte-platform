/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.github.benmanes.caffeine.cache.Ticker
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.TimeUnit

class ConnectRateLimiterTest {
  private var nowNanos = 0L
  private val ticker = Ticker { nowNanos }

  @Test
  fun `allows up to the limit then rejects`() {
    val limiter = ConnectRateLimiter(3, ticker)
    val workspaceId = UUID.randomUUID()

    repeat(3) { assertTrue(limiter.tryAcquire(workspaceId)) }

    assertFalse(limiter.tryAcquire(workspaceId))
    assertFalse(limiter.tryAcquire(workspaceId))
  }

  @Test
  fun `limits each workspace independently`() {
    val limiter = ConnectRateLimiter(1, ticker)

    assertTrue(limiter.tryAcquire(UUID.randomUUID()))
    assertTrue(limiter.tryAcquire(UUID.randomUUID()))
  }

  @Test
  fun `allows requests again after the window expires`() {
    val limiter = ConnectRateLimiter(1, ticker)
    val workspaceId = UUID.randomUUID()
    assertTrue(limiter.tryAcquire(workspaceId))
    assertFalse(limiter.tryAcquire(workspaceId))

    nowNanos += TimeUnit.SECONDS.toNanos(61)

    assertTrue(limiter.tryAcquire(workspaceId))
  }

  @Test
  fun `a zero limit rejects everything`() {
    assertFalse(ConnectRateLimiter(0, ticker).tryAcquire(UUID.randomUUID()))
  }
}
