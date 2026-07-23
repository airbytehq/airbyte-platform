/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.micronaut.runtime.AirbyteWorkloadLauncherConfig
import io.airbyte.workload.launcher.ClaimProcessorTracker
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class ClaimProcessorTrackerTest {
  @Test
  fun `claim processor should initialize the latch from parallelism`() {
    val configuration =
      AirbyteWorkloadLauncherConfig(
        parallelism =
          AirbyteWorkloadLauncherConfig.AirbyteWorkloadLauncherParallelismConfig(
            defaultQueue = 50,
            maxSurge = 0,
          ),
      )
    val tracker = ClaimProcessorTracker(configuration)
    assertEquals(50, tracker.count)
  }

  @Test
  fun `claim processor should apply max surge on initialization`() {
    val configuration =
      AirbyteWorkloadLauncherConfig(
        parallelism =
          AirbyteWorkloadLauncherConfig.AirbyteWorkloadLauncherParallelismConfig(
            defaultQueue = 50,
            maxSurge = 10,
          ),
      )
    val tracker = ClaimProcessorTracker(configuration)
    assertEquals(45, tracker.count)
  }

  @Test
  fun `claim processor should treat overflow max surge as 100`() {
    val configuration =
      AirbyteWorkloadLauncherConfig(
        parallelism =
          AirbyteWorkloadLauncherConfig.AirbyteWorkloadLauncherParallelismConfig(
            defaultQueue = 50,
            maxSurge = 100,
          ),
      )
    val tracker = ClaimProcessorTracker(configuration)
    assertEquals(0, tracker.count)
  }

  @Test
  fun `trackNumberOfClaimsToResumes updates the latch count to the number of claims to resume`() {
    val configuration =
      AirbyteWorkloadLauncherConfig(
        parallelism =
          AirbyteWorkloadLauncherConfig.AirbyteWorkloadLauncherParallelismConfig(
            defaultQueue = 10,
            maxSurge = 0,
          ),
      )
    val tracker = ClaimProcessorTracker(configuration)
    tracker.trackNumberOfClaimsToResume(3)
    assertEquals(3, tracker.count)
  }

  @Test
  fun `trackNumberOfClaimsToResumes can clear the latch count`() {
    val configuration =
      AirbyteWorkloadLauncherConfig(
        parallelism =
          AirbyteWorkloadLauncherConfig.AirbyteWorkloadLauncherParallelismConfig(
            defaultQueue = 10,
            maxSurge = 0,
          ),
      )
    val tracker = ClaimProcessorTracker(configuration)
    tracker.trackNumberOfClaimsToResume(0)
    assertEquals(0, tracker.count)
  }

  @Test
  fun `trackNumberOfClaimsToResumes supports a number of claims bigger than parallelism`() {
    val configuration =
      AirbyteWorkloadLauncherConfig(
        parallelism =
          AirbyteWorkloadLauncherConfig.AirbyteWorkloadLauncherParallelismConfig(
            defaultQueue = 10,
            maxSurge = 0,
          ),
      )
    val tracker = ClaimProcessorTracker(configuration)
    tracker.trackNumberOfClaimsToResume(15)
    assertEquals(15, tracker.count)
  }

  @Test
  fun `trackNumberOfClaimsToResumes supports a number of claims bigger than parallelism and still applies surge`() {
    val configuration =
      AirbyteWorkloadLauncherConfig(
        parallelism =
          AirbyteWorkloadLauncherConfig.AirbyteWorkloadLauncherParallelismConfig(
            defaultQueue = 100,
            maxSurge = 10,
          ),
      )
    val tracker = ClaimProcessorTracker(configuration)
    tracker.trackNumberOfClaimsToResume(150)
    assertEquals(140, tracker.count)
  }

  @Test
  fun `trackResumed decrements the count`() {
    val configuration =
      AirbyteWorkloadLauncherConfig(
        parallelism =
          AirbyteWorkloadLauncherConfig.AirbyteWorkloadLauncherParallelismConfig(
            defaultQueue = 10,
            maxSurge = 0,
          ),
      )
    val tracker = ClaimProcessorTracker(configuration)
    tracker.trackResumed()
    assertEquals(9, tracker.count)
    tracker.trackResumed()
    assertEquals(8, tracker.count)
  }

  @Test
  fun `trackResumed decrements the count correctly with overflow and latches unblocks when expected`() {
    val configuration =
      AirbyteWorkloadLauncherConfig(
        parallelism =
          AirbyteWorkloadLauncherConfig.AirbyteWorkloadLauncherParallelismConfig(
            defaultQueue = 2,
            maxSurge = 0,
          ),
      )
    val tracker = ClaimProcessorTracker(configuration)
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
