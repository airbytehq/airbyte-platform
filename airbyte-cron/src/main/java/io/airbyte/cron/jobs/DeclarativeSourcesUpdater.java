/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs;

import static io.airbyte.cron.MicronautCronRunner.SCHEDULED_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.config.init.DeclarativeSourceUpdater;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Declarative Sources Updater.
 *
 * Calls the DeclarativeSourceUpdater to update the declarative sources at an interval (10m).
 */
@Singleton
@Slf4j
public class DeclarativeSourcesUpdater {

  private final DeclarativeSourceUpdater declarativeSourceUpdater;
  private final MetricClient metricClient;

  public DeclarativeSourcesUpdater(@Named("remoteDeclarativeSourceUpdater") final DeclarativeSourceUpdater declarativeSourceUpdater,
                                   final MetricClient metricClient) {
    log.info("Creating declarative source updater");
    this.declarativeSourceUpdater = declarativeSourceUpdater;
    this.metricClient = metricClient;
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "10m")
  void updateDefinitions() {
    log.info("Getting latest CDK versions and updating declarative sources...");
    metricClient.count(OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE, 1, new MetricAttribute(MetricTags.CRON_TYPE, "declarative_sources_updater"));
    declarativeSourceUpdater.apply();
    log.info("Done updating declarative sources.");
  }

}
