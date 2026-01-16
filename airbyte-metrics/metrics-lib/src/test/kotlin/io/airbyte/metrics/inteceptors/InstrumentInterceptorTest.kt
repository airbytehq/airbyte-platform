/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.inteceptors

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.MetricsRegistry
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micronaut.context.ApplicationContext
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Singleton
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@Singleton
open class InstrumentExample {
  @Instrument(
    start = "WORKLOAD_MONITOR_RUN",
    end = "WORKLOAD_MONITOR_DONE",
    duration = "WORKLOAD_MONITOR_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = "1")],
  )
  open fun doSomething(with: String) {
    println("Doing something with $with")
  }

  @Instrument(
    start = "WORKLOAD_MONITOR_RUN",
    end = "WORKLOAD_MONITOR_DONE",
    duration = "WORKLOAD_MONITOR_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = "2")],
  )
  open fun failToDoSomething(with: String): Unit = throw RuntimeException("oops")
}

class InstrumentInterceptorTest {
  @Test
  fun testAnnotation() {
    val applicationContext = ApplicationContext.run()
    val metricClient =
      mockk<MetricClient> {
        every { count(any<MetricsRegistry>(), any(), *anyVararg()) } returns mockk<Counter>()
        every { distribution(any<MetricsRegistry>(), any(), *anyVararg()) } returns mockk<DistributionSummary>()
      }
    applicationContext.registerSingleton(metricClient)
    val exampleBean = applicationContext.getBean(InstrumentExample::class.java)

    exampleBean.doSomething("a hammer")
    assertThrows<RuntimeException> { exampleBean.failToDoSomething("") }

    verify {
      metricClient.count(OssMetricsRegistry.WORKLOAD_MONITOR_RUN, 1, MetricAttribute(MetricTags.CRON_TYPE, "1"))
      metricClient.count(
        OssMetricsRegistry.WORKLOAD_MONITOR_DONE,
        1,
        MetricAttribute(MetricTags.STATUS, "ok"),
        MetricAttribute(MetricTags.CRON_TYPE, "1"),
      )
      metricClient.distribution(
        OssMetricsRegistry.WORKLOAD_MONITOR_DURATION,
        any(),
        MetricAttribute(MetricTags.STATUS, "ok"),
        MetricAttribute(MetricTags.CRON_TYPE, "1"),
      )

      metricClient.count(OssMetricsRegistry.WORKLOAD_MONITOR_RUN, 1, MetricAttribute(MetricTags.CRON_TYPE, "2"))
      metricClient.count(
        OssMetricsRegistry.WORKLOAD_MONITOR_DONE,
        1,
        MetricAttribute(MetricTags.STATUS, "error"),
        MetricAttribute(MetricTags.CRON_TYPE, "2"),
      )
      metricClient.distribution(
        OssMetricsRegistry.WORKLOAD_MONITOR_DURATION,
        any(),
        MetricAttribute(MetricTags.STATUS, "error"),
        MetricAttribute(MetricTags.CRON_TYPE, "2"),
      )
    }

    applicationContext.close()
  }
}
