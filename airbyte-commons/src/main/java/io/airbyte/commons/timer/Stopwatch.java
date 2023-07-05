/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.timer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Stopwatch for tracking time spent across multiple executions.
 * <p>
 * This stopwatch will track both time spent and number of executions in a thread-safe
 * implementation. The best use case is within a try-with-resources block.
 *
 * <pre>
 * var sw = new Stopwatch();
 * try (final var s = sw.start()) {
 *   // action 1
 * }
 * try (final var s = sw.start()) {
 *   // action 2
 * }
 * System.out.println(sw); // print the summary
 * </pre>
 */
public class Stopwatch {

  /**
   * Represents an instance of stopwatch start/stop. This record log the start time, and will report
   * it back upon close.
   */
  public record StopwatchInstance(Stopwatch parent, long startTime) implements AutoCloseable {

    @Override
    public void close() {
      parent.stop(this);
    }

  }

  private final AtomicLong elapsedTimeInNanos = new AtomicLong();
  private final AtomicLong executionCount = new AtomicLong();

  /**
   * Start a timer instance.
   */
  public StopwatchInstance start() {
    return new StopwatchInstance(this, currentTime());
  }

  public long getElapsedTimeInNanos() {
    return elapsedTimeInNanos.get();
  }

  public long getExecutionCount() {
    return executionCount.get();
  }

  public double getAvgExecTimeInNanos() {
    return (double) getElapsedTimeInNanos() / getExecutionCount();
  }

  @Override
  public String toString() {
    return String.format("%.02f %s/exec (total: %.02f%s, %s executions)",
        getAvgExecTimeInNanos(), "ns",
        getElapsedTimeInNanos() / 1_000_000_000d, "s",
        getExecutionCount());
  }

  private long currentTime() {
    return System.nanoTime();
  }

  private void stop(final StopwatchInstance t) {
    final long delta = currentTime() - t.startTime;
    executionCount.incrementAndGet();
    elapsedTimeInNanos.addAndGet(delta);
  }

}
