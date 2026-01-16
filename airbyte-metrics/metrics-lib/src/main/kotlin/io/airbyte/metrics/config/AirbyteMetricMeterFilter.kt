/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.metrics.MetricVisibility
import io.airbyte.metrics.MetricsRegistry
import io.airbyte.metrics.MetricsResolver
import io.airbyte.metrics.OssMetricsRegistry
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.config.MeterFilter
import io.micrometer.core.instrument.config.MeterFilterReply
import io.micronaut.configuration.metrics.binder.web.config.HttpServerMeterConfig
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
  private val cache: LoadingCache<Meter.Id, MetricsRegistry?> =
    Caffeine
      .newBuilder()
      .maximumSize(10000)
      .build { id -> metricsResolvers.map { it.resolve(id.name.removePrefix(METRIC_PREFIX)) }.firstOrNull() }

  override fun map(id: Meter.Id): Meter.Id = applyMetricNamePrefix(mapMetricName(id = id))

  override fun accept(id: Meter.Id): MeterFilterReply {
    val metric = cache.get(id)
    logger.debug { "Resolved metric ID $id to $metric" }
    return when (airbyteEdition) {
      AirbyteEdition.CLOUD -> MeterFilterReply.ACCEPT
      AirbyteEdition.COMMUNITY -> MeterFilterReply.DENY
      AirbyteEdition.ENTERPRISE ->
        if (metric?.getMetricVisibility() == MetricVisibility.PUBLIC) {
          MeterFilterReply.ACCEPT
        } else {
          MeterFilterReply.DENY
        }
    }
  }

  /**
   * Ensures that all metrics have the required prefix name.
   *
   * @param id A [Meter.Id] to apply the prefix to its name.
   * @return The potentially modified [Meter.Id].
   */
  private fun applyMetricNamePrefix(id: Meter.Id): Meter.Id =
    if (!id.name.startsWith(METRIC_PREFIX)) {
      id.withName("$METRIC_PREFIX${id.name}")
    } else {
      id
    }

  /**
   * Maps metric names to new metrics names.  This can be used to expose
   * built-in metrics exposed by Micronaut, etc. under different names for
   * public metric purposes, etc.
   *
   * @param id A [Meter.Id] that represents a metric name.
   * @return The potentially re-mapped [Meter.Id].
   */
  private fun mapMetricName(id: Meter.Id): Meter.Id =
    // Map the built-in Micronaut HTTP server request metric to our public metric name
    when (id.name) {
      HttpServerMeterConfig.REQUESTS_METRIC -> id.withName(OssMetricsRegistry.API_REQUESTS.getMetricName())
      else -> id
    }
}
