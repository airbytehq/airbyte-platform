/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.workload.launcher.ClaimProcessorTracker
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class ClaimProcessorTrackerTest {
  @Test
  fun `claim processor should initialize the latch from parallelism`() {
    val tracker = ClaimProcessorTracker(parallelism = 50, parallelismMaxSurge = 0)
    assertEquals(50, tracker.count)
  }

  @Test
  fun `claim processor should apply max surge on initialization`() {
    val tracker = ClaimProcessorTracker(parallelism = 50, parallelismMaxSurge = 10)
    assertEquals(45, tracker.count)
  }

  @Test
  fun `claim processor should treat overflow max surge as 100`() {
    val tracker = ClaimProcessorTracker(parallelism = 50, parallelismMaxSurge = 200)
    assertEquals(0, tracker.count)
  }

  @Test
  fun `trackNumberOfClaimsToResumes updates the latch count to the number of claims to resume`() {
    val tracker = ClaimProcessorTracker(parallelism = 10, parallelismMaxSurge = 0)
    tracker.trackNumberOfClaimsToResume(3)
    assertEquals(3, tracker.count)
  }

  @Test
  fun `trackNumberOfClaimsToResumes can clear the latch count`() {
    val tracker = ClaimProcessorTracker(parallelism = 10, parallelismMaxSurge = 0)
    tracker.trackNumberOfClaimsToResume(0)
    assertEquals(0, tracker.count)
  }

  @Test
  fun `trackNumberOfClaimsToResumes supports a number of claims bigger than parallelism`() {
    val tracker = ClaimProcessorTracker(parallelism = 10, parallelismMaxSurge = 0)
    tracker.trackNumberOfClaimsToResume(15)
    assertEquals(15, tracker.count)
  }

  @Test
  fun `trackNumberOfClaimsToResumes supports a number of claims bigger than parallelism and still applies surge`() {
    val tracker = ClaimProcessorTracker(parallelism = 100, parallelismMaxSurge = 10)
    tracker.trackNumberOfClaimsToResume(150)
    assertEquals(140, tracker.count)
  }

  @Test
  fun `trackResumed decrements the count`() {
    val tracker = ClaimProcessorTracker(parallelism = 10, parallelismMaxSurge = 0)
    tracker.trackResumed()
    assertEquals(9, tracker.count)
    tracker.trackResumed()
    assertEquals(8, tracker.count)
  }

  @Test
  fun `trackResumed decrements the count correctly with overflow and latches unblocks when expected`() {
    val tracker = ClaimProcessorTracker(parallelism = 2, parallelismMaxSurge = 0)
    val awaitUnblocked = AtomicBoolean(false)
    val awaitThread =
      thread {
        tracker.await()
        awaitUnblocked.set(false)
      }

    tracker.trackNumberOfClaimsToResume(3)
    assertEquals(3, tracker.count)
    assertFalse(awaitUnblocked.get())

    tracker.trackResumed()
    assertEquals(2, tracker.count)
    assertFalse(awaitUnblocked.get())

    tracker.trackResumed()
    assertEquals(1, tracker.count)
    assertFalse(awaitUnblocked.get())

    // This should clear the countDownLatch, we're checking the thread was effectively unblocked
    // through the awaitCheckCount value.
    tracker.trackResumed()
    assertEquals(0, tracker.count)
    awaitThread.join()
    assertFalse(awaitUnblocked.get())

    // and it doesn't crash if it goes over
    tracker.trackResumed()
    assertEquals(0, tracker.count)
  }
}
