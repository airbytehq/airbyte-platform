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

/**
 * Updates the catalog for an Airbyte instance. For Airbyte Cloud, we want this update to happen very
 * quickly, so we have to split into two singletons with different schedules. At the time of writing,
 * we are spending nearly $7K/month on egress costs from the connector metadata service due to all
 * these updates.
 */
@Singleton
class BaseDefinitionsUpdater(
  private val applyDefinitionsHelper: ApplyDefinitionsHelper,
  private val airbyteEdition: Configs.AirbyteEdition,
  private val metricClient: MetricClient,
) {
  fun doUpdate() {
    log.info { "Updating definitions" }
    metricClient.count(
      metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "definitions_updater")),
    )
    applyDefinitionsHelper.apply(airbyteEdition == Configs.AirbyteEdition.CLOUD)
    log.info { "Done updating definitions" }
  }
}

@Singleton
@Requires(property = "airbyte.cron.update-definitions.enabled", value = "true")
@Requires(property = "airbyte.edition", pattern = "(?i)^cloud$")
class CloudDefinitionsUpdater(
  private val baseDefinitionsUpdater: BaseDefinitionsUpdater,
) {
  init {
    log.info { "Creating connector definitions updater for CLOUD" }
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "1m", initialDelay = "5m")
  fun updateDefinitions() = baseDefinitionsUpdater.doUpdate()
}

@Singleton
@Requires(property = "airbyte.cron.update-definitions.enabled", value = "true")
@Requires(property = "airbyte.edition", pattern = "(?i)^(?!cloud$).*$")
class CommunityDefinitionsUpdater(
  private val baseDefinitionsUpdater: BaseDefinitionsUpdater,
) {
  init {
    log.info { "Creating connector definitions updater for COMMUNITY/ENTERPRISE" }
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "1d", initialDelay = "1m")
  fun updateDefinitions() = baseDefinitionsUpdater.doUpdate()
}
