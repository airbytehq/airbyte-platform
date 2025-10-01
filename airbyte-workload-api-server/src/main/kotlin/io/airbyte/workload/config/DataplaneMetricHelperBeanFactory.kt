/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.config

import io.airbyte.commons.server.metrics.MetricTagsPrettifierCache
import io.airbyte.commons.server.metrics.PrettifyDataplaneMetricTagsMeterFilterBuilder
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.configuration.metrics.annotation.RequiresMetrics
import io.micronaut.context.annotation.Factory
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.runtime.event.ApplicationStartupEvent
import jakarta.inject.Singleton

@Factory
class DataplaneMetricHelperBeanFactory {
  @Singleton
  fun metricPrettifierCache(
    dataplaneService: DataplaneService,
    dataplaneGroupService: DataplaneGroupService,
  ): MetricTagsPrettifierCache = MetricTagsPrettifierCache(dataplaneService, dataplaneGroupService)

  @Singleton
  @RequiresMetrics
  fun prettifyDataplaneMetricTagsMeterFilterBuilder(
    cache: MetricTagsPrettifierCache,
    meterRegistry: MeterRegistry?,
  ): ApplicationEventListener<ApplicationStartupEvent> {
    val bean = PrettifyDataplaneMetricTagsMeterFilterBuilder(cache, meterRegistry)
    // Manually calling the PostConstruct because @PostConstruct does not work for beans created using @Factory
    bean.registerFilter()
    return bean
  }
}
