/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import datadog.trace.api.Trace
import io.airbyte.config.Configs
import io.airbyte.config.init.ApplyDefinitionsHelper
import io.airbyte.cron.SCHEDULED_TRACE_OPERATION_NAME
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

@Singleton
@Requires(property = "airbyte.cron.update-definitions.enabled", value = "true")
class DefinitionsUpdater(
  private val applyDefinitionsHelper: ApplyDefinitionsHelper,
  private val airbyteEdition: Configs.AirbyteEdition,
  private val metricClient: MetricClient,
) {
  init {
    log.info { "Creating connector definitions updater " }
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "30s", initialDelay = "1m")
  fun updateDefinitions() {
    log.info { "Updating definitions" }
    metricClient.count(
      metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "definitions_updater")),
    )
    applyDefinitionsHelper.apply(airbyteEdition == Configs.AirbyteEdition.CLOUD)
    log.info { "Done updating definitions" }
  }
}
