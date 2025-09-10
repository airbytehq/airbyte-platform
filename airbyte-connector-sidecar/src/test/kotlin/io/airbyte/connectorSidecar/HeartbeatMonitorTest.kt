/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorSidecar

import io.airbyte.api.client.ApiException
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workload.api.client.WorkloadApiClient
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

class HeartbeatMonitorTest {
  @Test
  fun `should set abort when ClientException with status code 410 is thrown`() {
    val sidecarInput = mockk<SidecarInput>()
    val workloadApiClient =
      mockk<WorkloadApiClient> {
        every { workloadHeartbeat(any()) } throws ApiException(HttpStatus.GONE.code, "http://localhost.test", "")
      }
    val sidecarLogContextFactory = mockk<SidecarLogContextFactory>()
    val clock = Clock.systemUTC()
    val abort = AtomicBoolean(false)
    val heartbeatTimeoutDuration = Duration.ofMinutes(5)

    every { sidecarInput.logPath } returns ""
    every { sidecarInput.workloadId } returns ""
    every { sidecarLogContextFactory.create(any()) } returns mapOf()

    val heartbeatTask =
      HeartbeatMonitor.HeartbeatTask(
        sidecarInput,
        sidecarLogContextFactory,
        workloadApiClient,
        clock,
        heartbeatTimeoutDuration,
        abort,
      )

    heartbeatTask.run()

    assertTrue(abort.get())
  }

  @Test
  fun `should set abort when heartbeat timeout duration is exceeded`() {
    val sidecarInput = mockk<SidecarInput>()
    val workloadApiClient = mockk<WorkloadApiClient>()
    val sidecarLogContextFactory = mockk<SidecarLogContextFactory>()
    val abort = AtomicBoolean(false)
    val heartbeatTimeoutDuration = Duration.ofMinutes(5)

    val initialInstant = Instant.parse("2021-01-01T00:00:00Z")
    val clock = mockk<Clock>()

    every { sidecarInput.logPath } returns ""
    every { sidecarInput.workloadId } returns ""
    every { sidecarLogContextFactory.create(any()) } returns mapOf()
    every { workloadApiClient.workloadHeartbeat(any()) } throws RuntimeException("Network error")

    every { clock.instant() } returns initialInstant andThen initialInstant.plus(Duration.ofMinutes(6))

    val heartbeatTask =
      HeartbeatMonitor.HeartbeatTask(
        sidecarInput,
        sidecarLogContextFactory,
        workloadApiClient,
        clock,
        heartbeatTimeoutDuration,
        abort,
      )

    heartbeatTask.run()
    heartbeatTask.run()

    assertTrue(abort.get())
  }

  @Test
  fun `should not set abort on transient exceptions within timeout duration`() {
    val sidecarInput = mockk<SidecarInput>()
    val workloadApiClient =
      mockk<WorkloadApiClient> {
        every { workloadHeartbeat(any()) } throws RuntimeException("Network error") andThen Unit
      }
    val sidecarLogContextFactory = mockk<SidecarLogContextFactory>()
    val abort = AtomicBoolean(false)
    val heartbeatTimeoutDuration = Duration.ofMinutes(5)

    val initialInstant = Instant.parse("2021-01-01T00:00:00Z")
    val clock = mockk<Clock>()

    every { sidecarInput.logPath } returns ""
    every { sidecarInput.workloadId } returns ""
    every { sidecarLogContextFactory.create(any()) } returns mapOf()

    every { clock.instant() } returnsMany
      listOf(
        initialInstant,
        initialInstant.plusSeconds(30),
        initialInstant.plusSeconds(60),
      )

    val heartbeatTask =
      HeartbeatMonitor.HeartbeatTask(
        sidecarInput,
        sidecarLogContextFactory,
        workloadApiClient,
        clock,
        heartbeatTimeoutDuration,
        abort,
      )

    heartbeatTask.run()
    heartbeatTask.run()

    assertFalse(abort.get())
  }
}
