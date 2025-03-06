/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron;

import io.airbyte.commons.temporal.TemporalInitializationUtils;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.discovery.event.ServiceReadyEvent;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Wait until temporal is ready before saying the app is ready.
 */
@Singleton
@Requires(notEnv = {Environment.TEST})
public class ApplicationInitializer implements ApplicationEventListener<ServiceReadyEvent> {

  @Inject
  private TemporalInitializationUtils temporalInitializationUtils;

  @Override
  public void onApplicationEvent(ServiceReadyEvent event) {
    temporalInitializationUtils.waitForTemporalNamespace();
  }

}
