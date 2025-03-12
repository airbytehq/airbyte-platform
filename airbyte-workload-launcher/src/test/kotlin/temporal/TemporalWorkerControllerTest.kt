/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package temporal

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workload.launcher.temporal.TemporalLauncherWorker
import io.airbyte.workload.launcher.temporal.TemporalWorkerController
import io.mockk.Ordering
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TemporalWorkerControllerTest {
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var temporalWorkerController: TemporalWorkerController
  private lateinit var temporalLauncherWorker: TemporalLauncherWorker

  @BeforeEach
  fun setup() {
    featureFlagClient = mockk<FeatureFlagClient>()
    temporalLauncherWorker = mockk()
    temporalWorkerController =
      TemporalWorkerController(
        "US",
        "test-plane",
        "queue",
        "high-prio-queue",
        mockk(),
        featureFlagClient,
        temporalLauncherWorker,
      )
  }

  @Test
  fun `does interact with workerFactories until started and start will enable pollers if active`() {
    temporalWorkerController.checkWorkerStatus()

    every { featureFlagClient.boolVariation(any(), any()) } returns true
    every { temporalLauncherWorker.initialize(any(), any()) } returns Unit
    every { temporalLauncherWorker.resumePolling() } returns Unit

    temporalWorkerController.start()
    verify(ordering = Ordering.ORDERED) {
      temporalLauncherWorker.initialize(any(), any())
      temporalLauncherWorker.resumePolling()
    }
  }

  @Test
  fun `does interact with workerFactories until started and start will disable pollers if disabled`() {
    temporalWorkerController.checkWorkerStatus()

    every { featureFlagClient.boolVariation(any(), any()) } returns false
    every { temporalLauncherWorker.initialize(any(), any()) } returns Unit
    every { temporalLauncherWorker.suspendPolling() } returns Unit

    temporalWorkerController.start()
    verify(ordering = Ordering.ORDERED) {
      temporalLauncherWorker.initialize(any(), any())
      temporalLauncherWorker.suspendPolling()
    }
  }
}
