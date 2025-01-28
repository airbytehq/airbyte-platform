/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server;

import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.event.ApplicationShutdownEvent;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens for shutdown signal and keeps server alive for 20 seconds to process requests on the fly.
 */
@Singleton
public class ShutdownEventListener implements
    ApplicationEventListener<ApplicationShutdownEvent> {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Value("${airbyte.shutdown.delay_ms}")
  private int shutdownDelayMillis;

  @Override
  public void onApplicationEvent(ApplicationShutdownEvent event) {
    logger.info("ShutdownEvent before wait");
    try {
      // Sleep 20 seconds to make sure server is wrapping up last remaining requests before
      // closing the connections.
      Thread.sleep(shutdownDelayMillis);
    } catch (final Exception ex) {
      // silently fail at this stage because server is terminating.
      logger.warn("exception: " + ex);
    }
    logger.info("ShutdownEvent after wait");
  }

}
