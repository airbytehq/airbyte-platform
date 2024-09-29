package temporal

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workload.launcher.temporal.TemporalWorkerController
import io.mockk.Ordering
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.temporal.worker.WorkerFactory
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TemporalWorkerControllerTest {
  private lateinit var workerFactory: WorkerFactory
  private lateinit var highPriorityWorkerFactory: WorkerFactory
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var temporalWorkerController: TemporalWorkerController

  @BeforeEach
  fun setup() {
    workerFactory = mockk<WorkerFactory>()
    highPriorityWorkerFactory = mockk<WorkerFactory>()
    featureFlagClient = mockk<FeatureFlagClient>()
    temporalWorkerController =
      TemporalWorkerController(
        "US",
        "test-plane",
        "queue",
        "high-prio-queue",
        mockk(),
        featureFlagClient,
        workerFactory,
        highPriorityWorkerFactory,
      )
  }

  @Test
  fun `does interact with workerFactories until started and start will enable pollers if active`() {
    temporalWorkerController.checkWorkerStatus()
    verifyNoInteractionWithWorkerFactories()

    every { featureFlagClient.boolVariation(any(), any()) } returns true
    every { workerFactory.resumePolling() } returns Unit
    every { workerFactory.start() } returns Unit
    every { workerFactory.suspendPolling() } returns Unit
    every { highPriorityWorkerFactory.resumePolling() } returns Unit
    every { highPriorityWorkerFactory.start() } returns Unit
    every { highPriorityWorkerFactory.suspendPolling() } returns Unit

    temporalWorkerController.start()
    verify(ordering = Ordering.ORDERED) {
      workerFactory.start()
      workerFactory.suspendPolling()
      highPriorityWorkerFactory.start()
      highPriorityWorkerFactory.suspendPolling()

      workerFactory.resumePolling()
      highPriorityWorkerFactory.resumePolling()
    }
  }

  @Test
  fun `does interact with workerFactories until started and start will disable pollers if disabled`() {
    temporalWorkerController.checkWorkerStatus()
    verifyNoInteractionWithWorkerFactories()

    every { featureFlagClient.boolVariation(any(), any()) } returns false
    every { workerFactory.start() } returns Unit
    every { workerFactory.suspendPolling() } returns Unit
    every { highPriorityWorkerFactory.start() } returns Unit
    every { highPriorityWorkerFactory.suspendPolling() } returns Unit

    temporalWorkerController.start()
    verify(ordering = Ordering.ORDERED) {
      workerFactory.start()
      workerFactory.suspendPolling()
      highPriorityWorkerFactory.start()
      highPriorityWorkerFactory.suspendPolling()
    }
  }

  fun verifyNoInteractionWithWorkerFactories() {
    verify(exactly = 0) {
      workerFactory.start()
      workerFactory.resumePolling()
      workerFactory.suspendPolling()
      highPriorityWorkerFactory.start()
      highPriorityWorkerFactory.resumePolling()
      highPriorityWorkerFactory.suspendPolling()
    }
  }
}
