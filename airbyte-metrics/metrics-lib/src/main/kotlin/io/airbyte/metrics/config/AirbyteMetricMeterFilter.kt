/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.metrics.MetricVisibility
import io.airbyte.metrics.MetricsResolver
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.config.MeterFilter
import io.micrometer.core.instrument.config.MeterFilterReply
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

const val METRIC_PREFIX = "airbyte."

/**
 * Custom [MeterFilter] that ensures that all metrics produced by the Airbyte
 * platform have a prefix of "airbyte." on the metric name and that only metrics
 * of the appropriate visibility are published.
 */
@Singleton
@Named("airbyteMetricMeterFilter")
@io.micronaut.configuration.metrics.annotation.RequiresMetrics
class AirbyteMetricMeterFilter(
  private val airbyteEdition: AirbyteEdition,
  private val metricsResolvers: List<MetricsResolver>,
) : MeterFilter {
  override fun map(id: Meter.Id): Meter.Id =
    if (!id.name.startsWith(METRIC_PREFIX)) {
      id.withName("$METRIC_PREFIX${id.name}")
    } else {
      id
    }

  override fun accept(id: Meter.Id): MeterFilterReply {
    val metric = metricsResolvers.map { it.resolve(id.name.removePrefix(METRIC_PREFIX)) }.firstOrNull()
    logger.debug { "Resolved metric ID $id to $metric" }
    return when (airbyteEdition) {
      AirbyteEdition.AIRBYTE -> MeterFilterReply.ACCEPT
      AirbyteEdition.COMMUNITY -> MeterFilterReply.DENY
      AirbyteEdition.ENTERPRISE ->
        if (metric?.getMetricVisibility() == MetricVisibility.PUBLIC) {
          MeterFilterReply.ACCEPT
        } else {
          MeterFilterReply.DENY
        }
    }
  }
}
