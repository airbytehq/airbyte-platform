/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package temporal

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.WorkloadLauncherConsumerEnabled
import io.airbyte.featureflag.WorkloadLauncherUseDataPlaneAuthNFlow
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueueConsumer
import io.airbyte.workload.launcher.temporal.TemporalLauncherWorker
import io.airbyte.workload.launcher.temporal.TemporalWorkerController
import io.mockk.Ordering
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class TemporalWorkerControllerTest {
  companion object {
    val EnabledConfig =
      DataplaneConfig(
        dataplaneId = UUID.randomUUID(),
        dataplaneName = "plane-name",
        dataplaneEnabled = true,
        dataplaneGroupId = UUID.randomUUID(),
        dataplaneGroupName = "group-name",
        temporalConsumerEnabled = true,
      )
    val DisabledConfig = EnabledConfig.copy(dataplaneEnabled = false)
  }

  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var temporalWorkerController: TemporalWorkerController
  private lateinit var temporalLauncherWorker: TemporalLauncherWorker
  private lateinit var workloadApiQueueConsumer: WorkloadApiQueueConsumer

  @BeforeEach
  fun setup() {
    featureFlagClient = mockk<FeatureFlagClient>()
    workloadApiQueueConsumer = mockk(relaxed = true)
    temporalLauncherWorker = mockk(relaxed = true)
    temporalWorkerController =
      TemporalWorkerController(
        "US",
        "test-plane",
        "queue",
        "high-prio-queue",
        mockk(),
        featureFlagClient,
        workloadApiQueueConsumer,
        temporalLauncherWorker,
      )
  }

  @Test
  fun `does interact with workerFactories until started and start will enable pollers if active`() {
    every { featureFlagClient.boolVariation(WorkloadLauncherUseDataPlaneAuthNFlow, any()) } returns false

    temporalWorkerController.checkWorkerStatus()

    every { featureFlagClient.boolVariation(WorkloadLauncherConsumerEnabled, any()) } returns true

    temporalWorkerController.start()
    verify(ordering = Ordering.ORDERED) {
      workloadApiQueueConsumer.initialize(any(), any())
      temporalLauncherWorker.initialize(any(), any())
      workloadApiQueueConsumer.resumePolling()
      temporalLauncherWorker.resumePolling()
    }
  }

  @Test
  fun `does interact with workerFactories until started and start will disable pollers if disabled`() {
    every { featureFlagClient.boolVariation(WorkloadLauncherUseDataPlaneAuthNFlow, any()) } returns false

    temporalWorkerController.checkWorkerStatus()

    every { featureFlagClient.boolVariation(WorkloadLauncherConsumerEnabled, any()) } returns false

    temporalWorkerController.start()
    verify(ordering = Ordering.ORDERED) {
      workloadApiQueueConsumer.initialize(any(), any())
      temporalLauncherWorker.initialize(any(), any())
      workloadApiQueueConsumer.suspendPolling()
      temporalLauncherWorker.suspendPolling()
    }
  }

  @Test
  fun `start will resume launcherWorker if last known config says enabled`() {
    every { featureFlagClient.boolVariation(WorkloadLauncherUseDataPlaneAuthNFlow, any()) } returns true

    temporalWorkerController.onApplicationEvent(EnabledConfig)
    temporalWorkerController.start()
    verify {
      temporalLauncherWorker.initialize(any(), any())
      workloadApiQueueConsumer.initialize(any(), any())
      temporalLauncherWorker.resumePolling()
      workloadApiQueueConsumer.resumePolling()
    }
  }

  @Test
  fun `start will suspend launcherWorker if last known config says disabled`() {
    every { featureFlagClient.boolVariation(WorkloadLauncherUseDataPlaneAuthNFlow, any()) } returns true

    temporalWorkerController.onApplicationEvent(DisabledConfig)
    temporalWorkerController.start()
    verify {
      temporalLauncherWorker.initialize(any(), any())
      workloadApiQueueConsumer.initialize(any(), any())
      temporalLauncherWorker.suspendPolling()
      workloadApiQueueConsumer.suspendPolling()
    }
  }

  @Test
  fun `doesn't interact with launcherWorker if not started`() {
    temporalWorkerController.onApplicationEvent(EnabledConfig)
    temporalWorkerController.onApplicationEvent(DisabledConfig)
    verify(exactly = 0) {
      temporalLauncherWorker.resumePolling()
      workloadApiQueueConsumer.resumePolling()
      temporalLauncherWorker.suspendPolling()
      workloadApiQueueConsumer.suspendPolling()
    }
  }

  @Test
  fun `calls suspend or resume according to the enabled changes`() {
    every { featureFlagClient.boolVariation(WorkloadLauncherUseDataPlaneAuthNFlow, any()) } returns true
    temporalWorkerController.start()
    clearMocks(temporalLauncherWorker)
    clearMocks(workloadApiQueueConsumer)

    temporalWorkerController.onApplicationEvent(EnabledConfig)
    verify { temporalLauncherWorker.resumePolling() }
    verify { workloadApiQueueConsumer.resumePolling() }
    clearMocks(temporalLauncherWorker)
    clearMocks(workloadApiQueueConsumer)

    // Sending enabled twice in a row result in only one invocation
    temporalWorkerController.onApplicationEvent(EnabledConfig)
    verify(exactly = 0) { temporalLauncherWorker.resumePolling() }
    verify(exactly = 0) { workloadApiQueueConsumer.resumePolling() }
    clearMocks(temporalLauncherWorker)
    clearMocks(workloadApiQueueConsumer)

    temporalWorkerController.onApplicationEvent(DisabledConfig)
    verify { temporalLauncherWorker.suspendPolling() }
    verify { workloadApiQueueConsumer.suspendPolling() }
    clearMocks(temporalLauncherWorker)
    clearMocks(workloadApiQueueConsumer)

    // Sending disabled twice in a row result in only one invocation
    temporalWorkerController.onApplicationEvent(DisabledConfig)
    verify(exactly = 0) { temporalLauncherWorker.suspendPolling() }
    verify(exactly = 0) { workloadApiQueueConsumer.suspendPolling() }
    clearMocks(temporalLauncherWorker)
    clearMocks(workloadApiQueueConsumer)

    temporalWorkerController.onApplicationEvent(EnabledConfig)
    verify { temporalLauncherWorker.resumePolling() }
    verify { workloadApiQueueConsumer.resumePolling() }
  }

  @Test
  fun `calls suspend or resume temporal according to the enabled changes`() {
    every { featureFlagClient.boolVariation(WorkloadLauncherUseDataPlaneAuthNFlow, any()) } returns true
    temporalWorkerController.start()
    clearMocks(temporalLauncherWorker)
    clearMocks(workloadApiQueueConsumer)

    temporalWorkerController.onApplicationEvent(EnabledConfig.copy(temporalConsumerEnabled = false))
    verify { temporalLauncherWorker.suspendPolling() }
    verify { workloadApiQueueConsumer.resumePolling() }
    clearMocks(temporalLauncherWorker)
    clearMocks(workloadApiQueueConsumer)

    // Sending enabled twice in a row result in only one invocation
    temporalWorkerController.onApplicationEvent(EnabledConfig)
    verify { temporalLauncherWorker.resumePolling() }
    verify(exactly = 0) { workloadApiQueueConsumer.resumePolling() }
    clearMocks(temporalLauncherWorker)
    clearMocks(workloadApiQueueConsumer)

    temporalWorkerController.onApplicationEvent(DisabledConfig)
    verify { temporalLauncherWorker.suspendPolling() }
    verify { workloadApiQueueConsumer.suspendPolling() }
    clearMocks(temporalLauncherWorker)
    clearMocks(workloadApiQueueConsumer)

    // Sending disabled twice in a row result in only one invocation
    temporalWorkerController.onApplicationEvent(DisabledConfig)
    verify(exactly = 0) { temporalLauncherWorker.suspendPolling() }
    verify(exactly = 0) { workloadApiQueueConsumer.suspendPolling() }
    clearMocks(temporalLauncherWorker)
    clearMocks(workloadApiQueueConsumer)

    temporalWorkerController.onApplicationEvent(EnabledConfig)
    verify { temporalLauncherWorker.resumePolling() }
    verify { workloadApiQueueConsumer.resumePolling() }
  }
}
