import io.airbyte.workload.launcher.ClaimedProcessor
import io.airbyte.workload.launcher.LauncherShutdownHelper
import io.airbyte.workload.launcher.StartupApplicationEventListener
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class StartupApplicationEventListenerTest {
  @Test
  fun `ensure launcher exits on retrieveAndProcess exceptions`() {
    val claimedProcessor =
      mockk<ClaimedProcessor> {
        every { retrieveAndProcess() } throws IllegalStateException("artifical failure")
      }
    val launcherShutdownHelper = mockk<LauncherShutdownHelper>(relaxed = true) {}
    val eventListener =
      StartupApplicationEventListener(
        claimedProcessor = claimedProcessor,
        claimProcessorTracker = mockk(relaxed = true),
        customMetricPublisher = mockk(relaxed = true),
        temporalWorkerController = mockk(relaxed = true),
        launcherShutdownHelper = launcherShutdownHelper,
      )

    eventListener.onApplicationEvent(null)
    eventListener.processorThread?.join(1.seconds.toJavaDuration())

    verify(exactly = 1) { launcherShutdownHelper.shutdown(2) }
  }
}
