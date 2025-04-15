/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.metrics

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.toTags
import io.micrometer.core.instrument.MeterRegistry
import jakarta.inject.Singleton
import reactor.core.observability.micrometer.Micrometer
import reactor.core.scheduler.Scheduler

// This could be moved into airbyte-metrics however, it would require moving the dependency on reactor as well.
@Singleton
class ReactorMetricsWrapper(
  private val meterRegistry: MeterRegistry?,
) {
  fun asTimedScheduler(
    scheduler: Scheduler,
    metricPrefix: String,
    vararg attributes: MetricAttribute?,
  ): Scheduler =
    meterRegistry?.let {
      Micrometer.timedScheduler(scheduler, it, metricPrefix, toTags(attributes))
    } ?: scheduler
}
