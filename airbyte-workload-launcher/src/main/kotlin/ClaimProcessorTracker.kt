/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.google.common.annotations.VisibleForTesting
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

/**
 * Tracks progress of the Rehydrate process.
 *
 * Acts as a CountdownLatch that unblocks when we rehydrated enough claims to start consuming fresh claims.
 */
@Singleton
class ClaimProcessorTracker(
  @Value("\${airbyte.workload-launcher.parallelism.default-queue}") private val parallelism: Int,
  @Value("\${airbyte.workload-launcher.parallelism.max-surge:0}") private val parallelismMaxSurge: Int,
) {
  private val latch: CountDownLatch = CountDownLatch(maxOf(100 - parallelismMaxSurge, 0) percentOf parallelism)

  // overflowCount solves the edge case where we'd have more in flight claims than the max parallelism.
  // if we CountDownLatch provided an increment, we wouldn't need this.
  private val overflowCount = AtomicInteger(0)

  @VisibleForTesting
  val count: Int
    get() = latch.count.toInt() + overflowCount.get()

  fun trackNumberOfClaimsToResume(n: Int) {
    if (n <= parallelism) {
      repeat(parallelism - n) { _ ->
        latch.countDown()
      }
    } else {
      overflowCount.set(n - parallelism)
    }
  }

  fun trackResumed() {
    // simple lock to ensure we go through the overflow before modifying the latch
    // keeping this simple as this part isn't performance sensitive
    synchronized(overflowCount) {
      if (overflowCount.get() > 0) {
        overflowCount.decrementAndGet()
      } else {
        latch.countDown()
      }
    }
  }

  fun await() {
    latch.await()
  }
}

private infix fun Int.percentOf(value: Int): Int = (value.toDouble() * this.toDouble() / 100.0).toInt()
