/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline

import io.airbyte.workload.launcher.ClaimedProcessor
import io.airbyte.workload.launcher.StartupApplicationEventListener
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.temporal.worker.WorkerFactory
import org.junit.jupiter.api.Test

class PipelineStartupTest {
  @Test
  fun `should process claimed workloads`() {
    val workerFactory: WorkerFactory = mockk()
    val claimedProcessor: ClaimedProcessor = mockk()

    every { claimedProcessor.retrieveAndProcess() } returns Unit
    every { workerFactory.start() } returns Unit

    val listener =
      StartupApplicationEventListener(
        claimedProcessor,
        workerFactory,
      )

    listener.onApplicationEvent(null)
    listener.mainThread?.join()

    verify { claimedProcessor.retrieveAndProcess() }
    verify { workerFactory.start() }
  }
}
