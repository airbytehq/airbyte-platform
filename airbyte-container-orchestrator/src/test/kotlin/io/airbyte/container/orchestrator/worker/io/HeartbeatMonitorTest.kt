/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.featureflag.ShouldFailSyncIfHeartbeatFailure
import io.airbyte.persistence.job.models.HeartbeatConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.function.Supplier

private val HEART_BEAT_FRESH_DURATION: Duration = Duration.ofSeconds(30)
private val NOW: Instant = Instant.now()
private val FIVE_SECONDS_BEFORE: Instant = NOW.minusSeconds(5)
private val THIRTY_SECONDS_BEFORE: Instant = NOW.minusSeconds(30)

internal class HeartbeatMonitorTest {
  private lateinit var replicationInput: ReplicationInput
  private lateinit var nowSupplier: Supplier<Instant>
  private lateinit var replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader
  private lateinit var heartbeatMonitor: HeartbeatMonitor

  @BeforeEach
  fun setup() {
    replicationInput = ReplicationInput().withHeartbeatConfig(HeartbeatConfig().withMaxSecondsBetweenMessages(HEART_BEAT_FRESH_DURATION.toSeconds()))
    nowSupplier = mockk()
    replicationInputFeatureFlagReader = mockk()
    // Default to feature flag being false
    every { replicationInputFeatureFlagReader.read(ShouldFailSyncIfHeartbeatFailure) } returns false
    heartbeatMonitor =
      HeartbeatMonitor(
        replicationInput = replicationInput,
        replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
        nowSupplier = nowSupplier,
      )
  }

  @Test
  fun testNeverBeat() {
    every { nowSupplier.get() } returnsMany listOf(THIRTY_SECONDS_BEFORE, NOW)
    assertEquals(false, heartbeatMonitor.isBeating.isPresent)
  }

  @Test
  fun testFreshBeat() {
    every { nowSupplier.get() } returnsMany listOf(FIVE_SECONDS_BEFORE, NOW)
    heartbeatMonitor.beat()
    assertEquals(Duration.ofSeconds(5), heartbeatMonitor.timeSinceLastBeat.get())
    assertEquals(true, heartbeatMonitor.isBeating.get())
  }

  @Test
  fun testStaleBeat() {
    every { nowSupplier.get() } returnsMany listOf(THIRTY_SECONDS_BEFORE, NOW)
    heartbeatMonitor.beat()
    assertEquals(Duration.ofSeconds(30), heartbeatMonitor.timeSinceLastBeat.get())
    assertEquals(false, heartbeatMonitor.isBeating.get())
  }

  @Test
  fun testHasTimedOutWithFlagEnabled() {
    // Mock the feature flag reader to return true for ShouldFailSyncIfHeartbeatFailure
    every { replicationInputFeatureFlagReader.read(ShouldFailSyncIfHeartbeatFailure) } returns true

    // Create a monitor with a stale beat
    every { nowSupplier.get() } returnsMany listOf(THIRTY_SECONDS_BEFORE, NOW)
    heartbeatMonitor =
      HeartbeatMonitor(
        replicationInput = replicationInput,
        replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
        nowSupplier = nowSupplier,
      )
    heartbeatMonitor.beat()

    // Should return true because the beat is stale and the flag is enabled
    assertEquals(true, heartbeatMonitor.hasTimedOut())
  }

  @Test
  fun testHasTimedOutWithFlagDisabled() {
    // Mock the feature flag reader to return false for ShouldFailSyncIfHeartbeatFailure
    every { replicationInputFeatureFlagReader.read(ShouldFailSyncIfHeartbeatFailure) } returns false

    // Create a monitor with a stale beat
    every { nowSupplier.get() } returnsMany listOf(THIRTY_SECONDS_BEFORE, NOW)
    heartbeatMonitor =
      HeartbeatMonitor(
        replicationInput = replicationInput,
        replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
        nowSupplier = nowSupplier,
      )
    heartbeatMonitor.beat()

    // Should return false because the flag is disabled, even though the beat is stale
    assertEquals(false, heartbeatMonitor.hasTimedOut())
  }
}
