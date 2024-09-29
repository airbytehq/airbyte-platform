/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server;

import io.airbyte.commons.logging.LogClientManager;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.discovery.event.ServiceReadyEvent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;

/**
 * Initializes the logging client on startup.
 */

@Singleton
public class LoggingEventListener implements ApplicationEventListener<ServiceReadyEvent> {

  static final String SERVER_LOGS = "server/logs";

  @Inject
  @Named("workspaceRoot")
  private Path workspaceRoot;
  @Inject
  private LogClientManager logClientManager;

  @Override
  public void onApplicationEvent(final ServiceReadyEvent event) {
    // Configure logging client
    logClientManager.setWorkspaceMdc(workspaceRoot.resolve(SERVER_LOGS));
  }

}
