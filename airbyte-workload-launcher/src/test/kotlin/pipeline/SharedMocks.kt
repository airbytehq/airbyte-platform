package pipeline

import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.mockk.every
import io.mockk.mockk

class SharedMocks {
  companion object {
    val metricPublisher: CustomMetricPublisher =
      mockk {
        every {
          count(any(), any())
        } returns Unit
        every {
          gauge<Any>(any(), any(), any(), any())
        } returns Unit
      }
  }
}
