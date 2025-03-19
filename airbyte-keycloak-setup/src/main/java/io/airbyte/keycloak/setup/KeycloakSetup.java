/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for setting up the Keycloak server. It initializes and configures the
 * server according to the provided specifications.
 */
@Singleton
@SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
public class KeycloakSetup {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final HttpClient httpClient;
  private final KeycloakServer keycloakServer;
  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final ConfigDbResetHelper configDbResetHelper;

  public KeycloakSetup(
                       final HttpClient httpClient,
                       final KeycloakServer keycloakServer,
                       final AirbyteKeycloakConfiguration keycloakConfiguration,
                       final ConfigDbResetHelper configDbResetHelper) {
    this.httpClient = httpClient;
    this.keycloakServer = keycloakServer;
    this.keycloakConfiguration = keycloakConfiguration;
    this.configDbResetHelper = configDbResetHelper;
  }

  public void run() {
    try {
      final String keycloakUrl = keycloakServer.getKeycloakServerUrl();
      final HttpResponse<String> response = httpClient.toBlocking()
          .exchange(HttpRequest.GET(keycloakUrl), String.class);

      log.info("Keycloak server response: {}", response.getStatus());
      log.info("Starting admin Keycloak client with url: {}", keycloakUrl);

      if (keycloakConfiguration.getResetRealm()) {
        keycloakServer.destroyAndRecreateAirbyteRealm();
        log.info("Successfully destroyed and recreated Airbyte Realm. Now deleting Airbyte User/Permission records...");
        try {
          configDbResetHelper.deleteConfigDbUsers();
        } catch (SQLException e) {
          log.error("Encountered an error while cleaning up Airbyte User/Permission records. "
              + "You likely need to re-run this KEYCLOAK_RESET_REALM operation.", e);
          throw new RuntimeException(e);
        }
        log.info("Successfully cleaned existing Airbyte User/Permission records. Reset finished successfully.");
      } else {
        keycloakServer.setupAirbyteRealm();
      }
    } finally {
      keycloakServer.closeKeycloakAdminClient();
    }
  }

}
