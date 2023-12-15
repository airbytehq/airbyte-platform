package io.airbyte.workload.launcher.pipeline.handlers

import fixtures.RecordFixtures
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.util.Optional
import java.util.function.Function
import javax.ws.rs.ClientErrorException

class SuccessHandlerTest {
  @Test
  fun `workload status updated to launched`() {
    val apiClient: WorkloadApiClient = mockk()
    val metricClient: CustomMetricPublisher = mockk()
    val logMsgTmp: Optional<Function<String, String>> = Optional.empty()

    val handler = SuccessHandler(apiClient, metricClient, logMsgTmp)

    val workloadId = "1337"
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadId = workloadId))

    every {
      metricClient.count(
        any(), any(), any(),
      )
    } returns Unit

    every {
      apiClient.updateStatusToLaunched(
        workloadId,
      )
    } returns Unit

    handler.accept(io)

    verify { apiClient.updateStatusToLaunched(workloadId) }
  }

  @Test
  fun `does not throw if update to launched call fails`() {
    val apiClient: WorkloadApiClient = mockk()
    val metricClient: CustomMetricPublisher = mockk()
    val logMsgTmp: Optional<Function<String, String>> = Optional.empty()

    val handler = SuccessHandler(apiClient, metricClient, logMsgTmp)

    val workloadId = "1337"
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadId = workloadId))

    every {
      metricClient.count(
        any(), any(), any(),
      )
    } returns Unit

    every {
      apiClient.updateStatusToLaunched(
        workloadId,
      )
    } throws ClientErrorException(400)

    assertDoesNotThrow { handler.accept(io) }
  }
}
