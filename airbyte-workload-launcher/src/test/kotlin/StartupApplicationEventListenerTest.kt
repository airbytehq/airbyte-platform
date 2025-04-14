/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.workload.launcher.ClaimedProcessor
import io.airbyte.workload.launcher.LauncherShutdownHelper
import io.airbyte.workload.launcher.StartupApplicationEventListener
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class StartupApplicationEventListenerTest {
  @Test
  fun `ensure launcher exits on retrieveAndProcess exceptions`() {
    val identityService =
      mockk<DataplaneIdentityService> {
        every { initialize() } just Runs
        every { getDataplaneId() } returns "dataplane1"
      }
    val claimedProcessor =
      mockk<ClaimedProcessor> {
        every { retrieveAndProcess("dataplane1") } throws IllegalStateException("artifical failure")
      }
    val launcherShutdownHelper = mockk<LauncherShutdownHelper>(relaxed = true) {}
    val eventListener =
      StartupApplicationEventListener(
        claimedProcessor = claimedProcessor,
        claimProcessorTracker = mockk(relaxed = true),
        metricClient = mockk(relaxed = true),
        queueConsumerController = mockk(relaxed = true),
        launcherShutdownHelper = launcherShutdownHelper,
        identityService = identityService,
      )

    eventListener.onApplicationEvent(null)
    eventListener.processorThread?.join(1.seconds.toJavaDuration())

    verify(exactly = 1) { launcherShutdownHelper.shutdown(2) }
  }
}
