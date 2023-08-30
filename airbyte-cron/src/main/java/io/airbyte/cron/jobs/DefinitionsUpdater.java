/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs;

import static io.airbyte.cron.MicronautCronRunner.SCHEDULED_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.init.ApplyDefinitionsHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/**
 * DefinitionsUpdater
 *
 * Automatically updates connector definitions from a remote catalog at an interval (30s). This can
 * be enabled by setting UPDATE_DEFINITIONS_CRON_ENABLED=true.
 */
@Singleton
@Slf4j
@Requires(property = "airbyte.cron.update-definitions.enabled",
          value = "true")
public class DefinitionsUpdater {

  private final ApplyDefinitionsHelper applyDefinitionsHelper;
  private final DeploymentMode deploymentMode;
  private final MetricClient metricClient;

  public DefinitionsUpdater(final ApplyDefinitionsHelper applyDefinitionsHelper,
                            final DeploymentMode deploymentMode,
                            final MetricClient metricClient) {
    log.info("Creating connector definitions updater");

    this.applyDefinitionsHelper = applyDefinitionsHelper;
    this.deploymentMode = deploymentMode;
    this.metricClient = metricClient;
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "30s",
             initialDelay = "1m")
  void updateDefinitions() throws JsonValidationException, ConfigNotFoundException, IOException {
    log.info("Updating definitions...");
    metricClient.count(OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE, 1, new MetricAttribute(MetricTags.CRON_TYPE, "definitions_updater"));
    applyDefinitionsHelper.apply(deploymentMode == DeploymentMode.CLOUD);
    log.info("Done applying remote connector definitions");
  }

}
