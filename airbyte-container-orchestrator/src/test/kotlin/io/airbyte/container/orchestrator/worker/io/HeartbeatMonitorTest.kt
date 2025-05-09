/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

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
  private lateinit var heartbeatMonitor: HeartbeatMonitor

  @BeforeEach
  fun setup() {
    replicationInput = ReplicationInput().withHeartbeatConfig(HeartbeatConfig().withMaxSecondsBetweenMessages(HEART_BEAT_FRESH_DURATION.toSeconds()))
    nowSupplier = mockk()
    heartbeatMonitor = HeartbeatMonitor(replicationInput = replicationInput, nowSupplier = nowSupplier)
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
}
