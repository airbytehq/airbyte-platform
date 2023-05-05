/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.HealthCheckRead;
import io.airbyte.config.persistence.ConfigRepository;
import io.micronaut.context.event.ShutdownEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HealthCheckHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@SuppressWarnings("MissingJavadocMethod")
@Singleton
public class HealthCheckHandler {

  private final ConfigRepository repository;

  private final AtomicBoolean ready = new AtomicBoolean(false);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public HealthCheckHandler(@Named("configRepository") final ConfigRepository repository) {
    this.repository = repository;
  }

  @EventListener
  public void onServerStartupEvent(ServerStartupEvent event) {
    log.info("ServerStartupEvent received: setting health status to READY");
    this.ready.set(true);
  }

  @EventListener
  public void onShutdownEvent(ShutdownEvent event) {
    log.info("ShutdownEvent received: setting health status to NOT READY");
    this.ready.set(false);
  }

  public HealthCheckRead health() {
    return new HealthCheckRead().available(repository.healthCheck());
  }

  public boolean isReady() {
    return this.ready.get();
  }

  public void setReady(boolean ready) {
    this.ready.set(ready);
  }

}
