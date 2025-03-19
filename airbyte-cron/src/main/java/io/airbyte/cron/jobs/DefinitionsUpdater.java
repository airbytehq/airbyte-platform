/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs;

import static io.airbyte.cron.MicronautCronRunner.SCHEDULED_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.config.Configs;
import io.airbyte.config.init.ApplyDefinitionsHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.metrics.MetricAttribute;
import io.airbyte.metrics.MetricClient;
import io.airbyte.metrics.OssMetricsRegistry;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Automatically updates connector definitions from a remote catalog at an interval (30s). This can
 * be enabled by setting UPDATE_DEFINITIONS_CRON_ENABLED=true.
 */
@Singleton
@Requires(property = "airbyte.cron.update-definitions.enabled",
          value = "true")
public class DefinitionsUpdater {

  private static final Logger log = LoggerFactory.getLogger(DefinitionsUpdater.class);

  private final ApplyDefinitionsHelper applyDefinitionsHelper;
  private final Configs.AirbyteEdition airbyteEdition;
  private final MetricClient metricClient;

  public DefinitionsUpdater(final ApplyDefinitionsHelper applyDefinitionsHelper,
                            final Configs.AirbyteEdition airbyteEdition,
                            final MetricClient metricClient) {
    log.info("Creating connector definitions updater");

    this.applyDefinitionsHelper = applyDefinitionsHelper;
    this.airbyteEdition = airbyteEdition;
    this.metricClient = metricClient;
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "30s",
             initialDelay = "1m")
  void updateDefinitions() throws JsonValidationException, ConfigNotFoundException, IOException {
    log.info("Updating definitions...");
    metricClient.count(OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE, new MetricAttribute(MetricTags.CRON_TYPE, "definitions_updater"));
    applyDefinitionsHelper.apply(airbyteEdition == Configs.AirbyteEdition.CLOUD);
    log.info("Done applying remote connector definitions");
  }

}
