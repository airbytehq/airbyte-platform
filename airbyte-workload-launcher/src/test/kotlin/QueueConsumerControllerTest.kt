/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.workload.launcher.QueueConsumerController
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueueConsumer
import io.mockk.clearMocks
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class QueueConsumerControllerTest {
  companion object {
    val EnabledConfig =
      DataplaneConfig(
        dataplaneId = UUID.randomUUID(),
        dataplaneName = "plane-name",
        dataplaneEnabled = true,
        dataplaneGroupId = UUID.randomUUID(),
        dataplaneGroupName = "group-name",
      )
    val DisabledConfig = EnabledConfig.copy(dataplaneEnabled = false)
  }

  private lateinit var queueConsumerController: QueueConsumerController
  private lateinit var workloadApiQueueConsumer: WorkloadApiQueueConsumer

  @BeforeEach
  fun setup() {
    workloadApiQueueConsumer = mockk(relaxed = true)
    queueConsumerController = QueueConsumerController(workloadApiQueueConsumer)
  }

  @Test
  fun `start will resume launcherWorker if last known config says enabled`() {
    queueConsumerController.onApplicationEvent(EnabledConfig)
    queueConsumerController.start()
    verify {
      workloadApiQueueConsumer.initialize(any())
      workloadApiQueueConsumer.resumePolling()
    }
  }

  @Test
  fun `start will suspend launcherWorker if last known config says disabled`() {
    queueConsumerController.onApplicationEvent(DisabledConfig)
    queueConsumerController.start()
    verify {
      workloadApiQueueConsumer.initialize(any())
      workloadApiQueueConsumer.suspendPolling()
    }
  }

  @Test
  fun `doesn't interact with launcherWorker if not started`() {
    queueConsumerController.onApplicationEvent(EnabledConfig)
    queueConsumerController.onApplicationEvent(DisabledConfig)
    verify(exactly = 0) {
      workloadApiQueueConsumer.resumePolling()
      workloadApiQueueConsumer.suspendPolling()
    }
  }

  @Test
  fun `calls suspend or resume according to the enabled changes`() {
    queueConsumerController.start()
    clearMocks(workloadApiQueueConsumer)

    queueConsumerController.onApplicationEvent(EnabledConfig)
    verify { workloadApiQueueConsumer.resumePolling() }
    clearMocks(workloadApiQueueConsumer)

    // Sending enabled twice in a row result in only one invocation
    queueConsumerController.onApplicationEvent(EnabledConfig)
    verify(exactly = 0) { workloadApiQueueConsumer.resumePolling() }
    clearMocks(workloadApiQueueConsumer)

    queueConsumerController.onApplicationEvent(DisabledConfig)
    verify { workloadApiQueueConsumer.suspendPolling() }
    clearMocks(workloadApiQueueConsumer)

    // Sending disabled twice in a row result in only one invocation
    queueConsumerController.onApplicationEvent(DisabledConfig)
    verify(exactly = 0) { workloadApiQueueConsumer.suspendPolling() }
    clearMocks(workloadApiQueueConsumer)

    queueConsumerController.onApplicationEvent(EnabledConfig)
    verify { workloadApiQueueConsumer.resumePolling() }
  }
}
