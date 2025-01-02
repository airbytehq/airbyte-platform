/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application entry point responsible for setting up the Keycloak server with an Airbyte
 * client.
 */
public class Application {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void main(final String[] args) {
    try {
      final ApplicationContext applicationContext = Micronaut.run(Application.class, args);
      final KeycloakSetup keycloakSetup = applicationContext.getBean(KeycloakSetup.class);
      keycloakSetup.run();
      System.exit(0);
    } catch (final Exception e) {
      log.error("Unable to setup Keycloak.", e);
      System.exit(-1);
    }
  }

}
