/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs;

import static io.airbyte.cron.MicronautCronRunner.SCHEDULED_TRACE_OPERATION_NAME;
import static io.airbyte.featureflag.ContextKt.ANONYMOUS;

import datadog.trace.api.Trace;
import io.airbyte.config.init.DeclarativeSourceUpdater;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.RunDeclarativeSourcesUpdater;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Declarative Sources Updater.
 *
 * Calls the DeclarativeSourceUpdater to update the declarative sources at an interval (10m).
 */
@Singleton
public class DeclarativeSourcesUpdater {

  private static final Logger log = LoggerFactory.getLogger(DeclarativeSourcesUpdater.class);

  private final DeclarativeSourceUpdater declarativeSourceUpdater;
  private final MetricClient metricClient;
  private final FeatureFlagClient featureFlagClient;

  public DeclarativeSourcesUpdater(@Named("remoteDeclarativeSourceUpdater") final DeclarativeSourceUpdater declarativeSourceUpdater,
                                   final MetricClient metricClient,
                                   final FeatureFlagClient featureFlagClient) {
    log.info("Creating declarative source updater");
    this.declarativeSourceUpdater = declarativeSourceUpdater;
    this.metricClient = metricClient;
    this.featureFlagClient = featureFlagClient;
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "10m")
  void updateDefinitions() {
    if (!featureFlagClient.boolVariation(RunDeclarativeSourcesUpdater.INSTANCE, new Workspace(ANONYMOUS))) {
      log.info("Declarative sources update feature flag is disabled. Skipping updating declarative sources.");
      return;
    }

    log.info("Getting latest CDK versions and updating declarative sources...");
    metricClient.count(OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE, 1, new MetricAttribute(MetricTags.CRON_TYPE, "declarative_sources_updater"));
    declarativeSourceUpdater.apply();
    log.info("Done updating declarative sources.");
  }

}
