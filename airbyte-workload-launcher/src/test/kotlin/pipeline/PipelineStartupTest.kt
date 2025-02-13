/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline

import io.airbyte.workload.launcher.ClaimProcessorTracker
import io.airbyte.workload.launcher.ClaimedProcessor
import io.airbyte.workload.launcher.StartupApplicationEventListener
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.temporal.TemporalWorkerController
import io.mockk.Ordering
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test

class PipelineStartupTest {
  @Test
  fun `should process claimed workloads`() {
    val claimedProcessor: ClaimedProcessor = mockk()
    val claimProcessorTracker: ClaimProcessorTracker = mockk()
    val metricPublisher: CustomMetricPublisher = mockk()
    val temporalWorkerController: TemporalWorkerController = mockk()

    every { claimedProcessor.retrieveAndProcess() } returns Unit
    every { claimProcessorTracker.await() } returns Unit
    every { temporalWorkerController.start() } returns Unit

    val listener =
      StartupApplicationEventListener(
        claimedProcessor,
        claimProcessorTracker,
        metricPublisher,
        temporalWorkerController,
        mockk(),
      )

    listener.onApplicationEvent(null)
    listener.processorThread?.join()
    listener.trackerThread?.join()

    verify { claimedProcessor.retrieveAndProcess() }
    verify(ordering = Ordering.ORDERED) {
      claimProcessorTracker.await()
      temporalWorkerController.start()
    }
  }
}
