/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.timer

import java.util.concurrent.atomic.AtomicLong

/**
 * Stopwatch for tracking time spent across multiple executions.
 *
 *
 * This stopwatch will track both time spent and number of executions in a thread-safe
 * implementation. The best use case is within a try-with-resources block.
 *
 * <pre>
 * var sw = new Stopwatch();
 * try (final var s = sw.start()) {
 * // action 1
 * }
 * try (final var s = sw.start()) {
 * // action 2
 * }
 * System.out.println(sw); // print the summary
</pre> *
 */
class Stopwatch {
  /**
   * Represents an instance of stopwatch start/stop. This record log the start time, and will report
   * it back upon close.
   */
  @JvmRecord
  data class StopwatchInstance(val parent: Stopwatch, val startTime: Long) : AutoCloseable {
    override fun close() {
      parent.stop(this)
    }
  }

  private val elapsedTimeInNanos = AtomicLong()
  private val executionCount = AtomicLong()

  fun <T> time(block: () -> T): T = start().use { block() }

  /**
   * Start a timer instance.
   */
  fun start(): StopwatchInstance {
    return StopwatchInstance(this, currentTime())
  }

  fun getElapsedTimeInNanos(): Long {
    return elapsedTimeInNanos.get()
  }

  fun getExecutionCount(): Long {
    return executionCount.get()
  }

  val avgExecTimeInNanos: Double
    get() = getElapsedTimeInNanos().toDouble() / getExecutionCount()

  override fun toString(): String {
    return String.format(
      "%.02f %s/exec (total: %.02f%s, %s executions)",
      avgExecTimeInNanos,
      "ns",
      getElapsedTimeInNanos() / 1000000000.0,
      "s",
      getExecutionCount(),
    )
  }

  private fun currentTime(): Long {
    return System.nanoTime()
  }

  private fun stop(t: StopwatchInstance) {
    val delta = currentTime() - t.startTime
    executionCount.incrementAndGet()
    elapsedTimeInNanos.addAndGet(delta)
  }
}
