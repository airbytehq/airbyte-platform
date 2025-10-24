/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package metrics

import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.workload.launcher.metrics.DataplaneAwareMetricClient
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.search.Search
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.ToDoubleFunction

class DataplaneAwareMetricClientTest {
  private lateinit var metricClient: MetricClient
  private lateinit var meterRegistry: MeterRegistry
  private lateinit var dataplaneAwareMetricClient: DataplaneAwareMetricClient

  @BeforeEach
  fun setup() {
    metricClient = mockk(relaxed = true)
    meterRegistry = mockk(relaxed = true)
    dataplaneAwareMetricClient = DataplaneAwareMetricClient(metricClient, meterRegistry)
  }

  @Test
  fun `registerDataplaneGauge registers gauge with metric client`() {
    val stateObject = AtomicInteger(5)
    val function: ToDoubleFunction<AtomicInteger> = ToDoubleFunction { it.get().toDouble() }

    every { metricClient.gauge(any(), any<AtomicInteger>(), any()) } returns stateObject

    val result =
      dataplaneAwareMetricClient.registerDataplaneGauge(
        metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_ACTIVE_LAUNCH,
        stateObject = stateObject,
        function = function,
      )

    assertEquals(stateObject, result)
    verify(exactly = 1) {
      metricClient.gauge(
        OssMetricsRegistry.WORKLOAD_LAUNCHER_ACTIVE_LAUNCH,
        stateObject,
        function,
      )
    }
  }

  @Test
  fun `onApplicationEvent removes old meter and re-registers gauge`() {
    val stateObject = AtomicInteger(10)
    val function: ToDoubleFunction<AtomicInteger> = ToDoubleFunction { it.get().toDouble() }
    val metricName = OssMetricsRegistry.WORKLOAD_LAUNCHER_ACTIVE_LAUNCH.getMetricName()

    // Mock the meter search and removal
    val oldMeter: Meter = mockk(relaxed = true)
    val search: Search = mockk(relaxed = true)
    every { meterRegistry.find(metricName) } returns search
    every { search.meters() } returns listOf(oldMeter)
    every { meterRegistry.remove(oldMeter) } returns oldMeter
    every { metricClient.gauge(any(), any<AtomicInteger>(), any()) } returns stateObject

    // Register the gauge first
    dataplaneAwareMetricClient.registerDataplaneGauge(
      metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_ACTIVE_LAUNCH,
      stateObject = stateObject,
      function = function,
    )

    // Fire DataplaneConfig event
    val dataplaneConfig =
      DataplaneConfig(
        dataplaneId = UUID.randomUUID(),
        dataplaneName = "test-plane",
        dataplaneEnabled = true,
        dataplaneGroupId = UUID.randomUUID(),
        dataplaneGroupName = "test-group",
        organizationId = UUID.randomUUID(),
      )

    dataplaneAwareMetricClient.onApplicationEvent(dataplaneConfig)

    // Verify old meter was removed
    verify(exactly = 1) { meterRegistry.remove(oldMeter) }

    // Verify gauge was re-registered
    verify(exactly = 2) {
      metricClient.gauge(
        OssMetricsRegistry.WORKLOAD_LAUNCHER_ACTIVE_LAUNCH,
        stateObject,
        function,
      )
    }
  }

  @Test
  fun `onApplicationEvent handles meter not found gracefully`() {
    val stateObject = AtomicInteger(7)
    val function: ToDoubleFunction<AtomicInteger> = ToDoubleFunction { it.get().toDouble() }
    val metricName = OssMetricsRegistry.WORKLOAD_LAUNCHER_ACTIVE_LAUNCH.getMetricName()

    // Mock the meter search returning no meters
    val search: Search = mockk(relaxed = true)
    every { meterRegistry.find(metricName) } returns search
    every { search.meters() } returns emptyList()
    every { metricClient.gauge(any(), any<AtomicInteger>(), any()) } returns stateObject

    // Register gauge
    dataplaneAwareMetricClient.registerDataplaneGauge(
      metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_ACTIVE_LAUNCH,
      stateObject = stateObject,
      function = function,
    )

    // Fire event
    val dataplaneConfig =
      DataplaneConfig(
        dataplaneId = UUID.randomUUID(),
        dataplaneName = "test-plane",
        dataplaneEnabled = true,
        dataplaneGroupId = UUID.randomUUID(),
        dataplaneGroupName = "test-group",
        organizationId = UUID.randomUUID(),
      )

    dataplaneAwareMetricClient.onApplicationEvent(dataplaneConfig)

    // Should still re-register even if old meter not found
    verify(exactly = 2) {
      metricClient.gauge(
        OssMetricsRegistry.WORKLOAD_LAUNCHER_ACTIVE_LAUNCH,
        stateObject,
        function,
      )
    }
  }

  @Test
  fun `onApplicationEvent re-registers multiple gauges`() {
    val stateObject1 = AtomicInteger(1)
    val function: ToDoubleFunction<AtomicInteger> = ToDoubleFunction { it.get().toDouble() }

    // Use different metrics for this test (using the same metric since we only have one available)
    val metric1 = OssMetricsRegistry.WORKLOAD_LAUNCHER_ACTIVE_LAUNCH
    val metricName1 = metric1.getMetricName()

    val oldMeter1: Meter = mockk(relaxed = true)
    val search1: Search = mockk(relaxed = true)

    every { meterRegistry.find(metricName1) } returns search1
    every { search1.meters() } returns listOf(oldMeter1)
    every { meterRegistry.remove(oldMeter1) } returns oldMeter1
    every { metricClient.gauge(any(), any<AtomicInteger>(), any()) } returnsArgument 1

    // Register first gauge
    dataplaneAwareMetricClient.registerDataplaneGauge(
      metric = metric1,
      stateObject = stateObject1,
      function = function,
    )

    // Fire event
    val dataplaneConfig =
      DataplaneConfig(
        dataplaneId = UUID.randomUUID(),
        dataplaneName = "test-plane",
        dataplaneEnabled = true,
        dataplaneGroupId = UUID.randomUUID(),
        dataplaneGroupName = "test-group",
        organizationId = UUID.randomUUID(),
      )

    dataplaneAwareMetricClient.onApplicationEvent(dataplaneConfig)

    // Verify re-registration happened
    verify(exactly = 1) { meterRegistry.remove(oldMeter1) }
    verify(exactly = 2) { metricClient.gauge(metric1, stateObject1, function) }
  }
}
