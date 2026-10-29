/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Ticker
import io.micronaut.context.annotation.Value
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

const val DEFAULT_CONNECT_RATE_LIMIT_PER_MINUTE = 30

/**
 * Per-workspace limit on connect commands. Each workspace gets [limitPerMinute] acquisitions in a window that
 * starts with its first acquisition and expires one minute later.
 */
@Singleton
class ConnectRateLimiter internal constructor(
  private val limitPerMinute: Int,
  ticker: Ticker,
) {
  @Inject
  constructor(
    @Value("\${airbyte.server.connect.rate-limit-per-minute:$DEFAULT_CONNECT_RATE_LIMIT_PER_MINUTE}") limitPerMinute: Int,
  ) : this(limitPerMinute, Ticker.systemTicker())

  private val counters =
    Caffeine
      .newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .ticker(ticker)
      .build<UUID, AtomicInteger>()

  /** Returns true and counts the request if the workspace is under its limit. */
  fun tryAcquire(workspaceId: UUID): Boolean = counters.get(workspaceId) { AtomicInteger() }.incrementAndGet() <= limitPerMinute
}
