/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import com.github.benmanes.caffeine.cache.Ticker
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.EnableStrictJsonDeserialization
import io.airbyte.featureflag.FeatureFlagClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

internal class StrictJsonDeserializationFlagTest {
  @Test
  fun `starts disabled and does not evaluate while the cached default is fresh`() {
    val featureFlagClient = mockk<FeatureFlagClient>()
    val ticker = MutableTicker()
    val refreshExecutor = QueuedExecutor()
    val flag = StrictJsonDeserializationFlag(featureFlagClient, refreshExecutor, ticker)

    assertFalse(flag.isEnabled())
    ticker.advance(Duration.ofSeconds(30))
    repeat(10) { assertFalse(flag.isEnabled()) }

    refreshExecutor.runAll()
    verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }
  }

  @Test
  fun `expired requests return stale without blocking and trigger one background refresh`() {
    val featureFlagClient = mockk<FeatureFlagClient>()
    every { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) } returns true
    val ticker = MutableTicker()
    val refreshExecutor = QueuedExecutor()
    val flag = StrictJsonDeserializationFlag(featureFlagClient, refreshExecutor, ticker)
    ticker.advancePastRefreshInterval()

    val requestExecutor = Executors.newFixedThreadPool(8)
    try {
      val start = CountDownLatch(1)
      val requests =
        (1..32).map {
          requestExecutor.submit<Boolean> {
            start.await()
            flag.isEnabled()
          }
        }
      start.countDown()

      assertTrue(requests.all { !it.get(5, TimeUnit.SECONDS) })
      verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }

      refreshExecutor.runAll()

      verify(exactly = 1) { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) }
      assertTrue(flag.isEnabled())
    } finally {
      requestExecutor.shutdownNow()
    }
  }

  @Test
  fun `successful refresh publishes the value for another thirty seconds`() {
    val featureFlagClient = mockk<FeatureFlagClient>()
    every { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) } returnsMany listOf(true, false)
    val ticker = MutableTicker()
    val refreshExecutor = QueuedExecutor()
    val flag = StrictJsonDeserializationFlag(featureFlagClient, refreshExecutor, ticker)

    ticker.advancePastRefreshInterval()
    assertFalse(flag.isEnabled())
    refreshExecutor.runAll()
    assertTrue(flag.isEnabled())

    ticker.advance(Duration.ofSeconds(30))
    repeat(10) { assertTrue(flag.isEnabled()) }
    refreshExecutor.runAll()
    verify(exactly = 1) { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) }

    ticker.advance(Duration.ofNanos(1))
    assertTrue(flag.isEnabled())
    refreshExecutor.runAll()

    verify(exactly = 2) { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) }
    assertFalse(flag.isEnabled())
  }

  @Test
  fun `failed refresh keeps stale and backs off for thirty seconds`() {
    val featureFlagClient = mockk<FeatureFlagClient>()
    val evaluations = AtomicInteger()
    every { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) } answers {
      if (evaluations.incrementAndGet() == 1) {
        throw IllegalStateException("LaunchDarkly unavailable")
      }
      true
    }
    val ticker = MutableTicker()
    val refreshExecutor = QueuedExecutor()
    val flag = StrictJsonDeserializationFlag(featureFlagClient, refreshExecutor, ticker)

    ticker.advancePastRefreshInterval()
    assertFalse(flag.isEnabled())
    refreshExecutor.runAll()
    assertFalse(flag.isEnabled())

    ticker.advance(Duration.ofSeconds(30))
    repeat(10) { assertFalse(flag.isEnabled()) }
    refreshExecutor.runAll()
    verify(exactly = 1) { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) }

    ticker.advance(Duration.ofNanos(1))
    assertFalse(flag.isEnabled())
    refreshExecutor.runAll()

    verify(exactly = 2) { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) }
    assertTrue(flag.isEnabled())
  }

  @Test
  fun `rejected refresh keeps stale and backs off before recovering`() {
    val featureFlagClient = mockk<FeatureFlagClient>()
    every { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) } returns true
    val ticker = MutableTicker()
    val refreshExecutor = SwitchableExecutor()
    val flag = StrictJsonDeserializationFlag(featureFlagClient, refreshExecutor, ticker)
    ticker.advancePastRefreshInterval()

    val requestExecutor = Executors.newFixedThreadPool(8)
    try {
      val start = CountDownLatch(1)
      val requests =
        (1..32).map {
          requestExecutor.submit<Boolean> {
            start.await()
            flag.isEnabled()
          }
        }
      start.countDown()

      assertTrue(requests.all { !it.get(5, TimeUnit.SECONDS) })
      assertEquals(1, refreshExecutor.submissionCount())
      verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }

      repeat(10) { assertFalse(flag.isEnabled()) }
      assertEquals(1, refreshExecutor.submissionCount())

      ticker.advance(Duration.ofSeconds(30))
      repeat(10) { assertFalse(flag.isEnabled()) }
      assertEquals(1, refreshExecutor.submissionCount())

      refreshExecutor.acceptTasks()
      ticker.advance(Duration.ofNanos(1))
      assertFalse(flag.isEnabled())
      assertEquals(2, refreshExecutor.submissionCount())
      verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }

      refreshExecutor.runAll()

      verify(exactly = 1) { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) }
      assertTrue(flag.isEnabled())
    } finally {
      requestExecutor.shutdownNow()
    }
  }

  private class MutableTicker : Ticker {
    private val nanos = AtomicLong()

    override fun read(): Long = nanos.get()

    fun advance(duration: Duration) {
      nanos.addAndGet(duration.toNanos())
    }

    fun advancePastRefreshInterval() {
      advance(Duration.ofSeconds(31))
    }
  }

  private class QueuedExecutor : Executor {
    private val tasks = ConcurrentLinkedQueue<Runnable>()

    override fun execute(command: Runnable) {
      tasks.add(command)
    }

    fun runAll() {
      while (true) {
        val task = tasks.poll() ?: return
        task.run()
      }
    }
  }

  private class SwitchableExecutor : Executor {
    private val accepting = AtomicBoolean()
    private val submissions = AtomicInteger()
    private val tasks = ConcurrentLinkedQueue<Runnable>()

    override fun execute(command: Runnable) {
      submissions.incrementAndGet()
      if (!accepting.get()) {
        throw RejectedExecutionException("executor is unavailable")
      }
      tasks.add(command)
    }

    fun acceptTasks() {
      accepting.set(true)
    }

    fun submissionCount(): Int = submissions.get()

    fun runAll() {
      while (true) {
        val task = tasks.poll() ?: return
        task.run()
      }
    }
  }
}
