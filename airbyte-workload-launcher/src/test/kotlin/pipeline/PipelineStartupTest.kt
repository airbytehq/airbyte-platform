/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline

import io.airbyte.metrics.MetricClient
import io.airbyte.workload.launcher.ClaimProcessorTracker
import io.airbyte.workload.launcher.ClaimedProcessor
import io.airbyte.workload.launcher.StartupApplicationEventListener
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
import io.airbyte.workload.launcher.temporal.TemporalWorkerController
import io.mockk.Ordering
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test

class PipelineStartupTest {
  @Test
  fun `should process claimed workloads`() {
    val claimedProcessor: ClaimedProcessor = mockk()
    val claimProcessorTracker: ClaimProcessorTracker = mockk()
    val metricClient: MetricClient = mockk()
    val temporalWorkerController: TemporalWorkerController = mockk()
    val identityService: DataplaneIdentityService = mockk()
    val dataplaneId = "dataplane-1"

    every { identityService.initialize() } just Runs
    every { identityService.getDataplaneId() } returns dataplaneId
    every { claimedProcessor.retrieveAndProcess(dataplaneId) } returns Unit
    every { claimProcessorTracker.await() } returns Unit
    every { temporalWorkerController.start() } returns Unit

    val listener =
      StartupApplicationEventListener(
        claimedProcessor,
        claimProcessorTracker,
        metricClient,
        temporalWorkerController,
        mockk(),
        identityService,
      )

    listener.onApplicationEvent(null)
    listener.processorThread?.join()
    listener.trackerThread?.join()

    verify { claimedProcessor.retrieveAndProcess(dataplaneId) }
    verify(ordering = Ordering.ORDERED) {
      claimProcessorTracker.await()
      temporalWorkerController.start()
    }
  }
}
