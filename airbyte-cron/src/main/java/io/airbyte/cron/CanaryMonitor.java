/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron;

import static io.airbyte.cron.MicronautCronRunner.SCHEDULED_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.config.persistence.ConfigRepository;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * The CanaryMonitor cron periodically examines configured workspace IDs and/or connection IDs for
 * failures.
 */
@Singleton
@Slf4j
public class CanaryMonitor {

  private final ConfigRepository configRepository;

  CanaryMonitor(final ConfigRepository configRepository) {
    this.configRepository = configRepository;
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "30s")
  public void detectFailures() {
    log.info("Starting detectFailures scheduled operationffffff");

    try {
      final boolean healthy = configRepository.healthCheck();
      log.info("ConfigRepository healthCheck: {}", healthy);
    } catch (final Exception e) {
      log.error("Error occurred while detecting failures", e);
    }
  }

}
