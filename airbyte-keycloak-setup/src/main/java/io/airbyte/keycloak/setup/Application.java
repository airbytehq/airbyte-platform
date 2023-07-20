/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import lombok.extern.slf4j.Slf4j;

/**
 * Main application entry point responsible for setting up the Keycloak server with an Airbyte
 * client.
 */
@Slf4j
public class Application {

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
