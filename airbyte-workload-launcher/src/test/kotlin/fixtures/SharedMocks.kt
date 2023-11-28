package io.airbyte.workload.launcher.fixtures

import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.mockk.every
import io.mockk.mockk
import reactor.core.scheduler.Scheduler

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

    val processClaimedScheduler: Scheduler = mockk()
  }
}
