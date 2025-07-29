/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import datadog.trace.api.Trace
import io.airbyte.config.init.DeclarativeSourceUpdater
import io.airbyte.cron.SCHEDULED_TRACE_OPERATION_NAME
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.RunDeclarativeSourcesUpdater
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Named
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

@Singleton
@Requires(property = "airbyte.cron.declarative-sources-updater.enabled", value = "true")
class DeclarativeSourcesUpdater(
  @Named("remoteDeclarativeSourceUpdater") private val declarativeSourceUpdater: DeclarativeSourceUpdater,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
) {
  init {
    log.info { "Creating declarative source updater" }
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "10m")
  fun updateDefinitions() {
    if (!featureFlagClient.boolVariation(RunDeclarativeSourcesUpdater, Workspace(ANONYMOUS))) {
      log.info { "Declarative sources update feature flag is disabled. Skipping updating declarative sources." }
      return
    }

    log.info { "Getting latest CDK versions and updating declarative sources..." }
    metricClient.count(
      metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "declarative_sources_updater")),
    )
    declarativeSourceUpdater.apply()
    log.info { "Done updating declarative sources." }
  }
}
