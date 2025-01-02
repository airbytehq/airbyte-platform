/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.config.MeterFilter
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Custom [MeterFilter] that ensures that all metrics produced by the Airbyte
 * platform have a prefix of "airbyte." on the metric name.
 */
@Singleton
@Named("metricAirbytePrefixMeterFilter")
@io.micronaut.configuration.metrics.annotation.RequiresMetrics
class MetricAirbytePrefixMeterFilter : MeterFilter {
  companion object {
    const val PREFIX = "airbyte"
  }

  override fun map(id: Meter.Id): Meter.Id {
    return if (!id.name.startsWith("$PREFIX\\.")) {
      id.withName("$PREFIX.${id.name}")
    } else {
      id
    }
  }
}
