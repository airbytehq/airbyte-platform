/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.metrics

import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.MetricsRegistry
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.configuration.metrics.annotation.RequiresMetrics
import io.micronaut.context.event.ApplicationEventListener
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.function.ToDoubleFunction

/**
 * Wrapper around MetricClient that handles re-registering gauges when DataplaneConfig changes.
 * This ensures gauges pick up correct dataplane tags after the config is available.
 */
@Singleton
@RequiresMetrics
class DataplaneAwareMetricClient(
  private val metricClient: MetricClient,
  private val meterRegistry: MeterRegistry,
) : ApplicationEventListener<DataplaneConfig> {
  // Track registered gauges so we can re-register them when dataplane config updates
  private data class GaugeRegistration<T>(
    val metric: MetricsRegistry,
    val stateObject: T,
    val function: ToDoubleFunction<T>,
  )

  private val registeredGauges = ConcurrentHashMap<String, GaugeRegistration<*>>()

  /**
   * Register a gauge that will automatically update its dataplane tags when config is available.
   */
  fun <T> registerDataplaneGauge(
    metric: MetricsRegistry,
    stateObject: T,
    function: ToDoubleFunction<T>,
  ): T {
    // Store the registration
    registeredGauges[metric.getMetricName()] = GaugeRegistration(metric, stateObject, function)

    // Register the gauge
    return metricClient.gauge(metric, stateObject, function)
  }

  override fun onApplicationEvent(event: DataplaneConfig) {
    // Re-register all gauges to pick up updated dataplane tags
    registeredGauges.forEach { (metricName, registration) ->
      // Remove old gauge with UNDEFINED dataplane tags
      meterRegistry.remove(
        meterRegistry.find(metricName).meters().firstOrNull(),
      )

      // Re-register with updated tags
      @Suppress("UNCHECKED_CAST")
      val reg = registration as GaugeRegistration<Any>
      metricClient.gauge(reg.metric, reg.stateObject, reg.function)
    }
  }
}
