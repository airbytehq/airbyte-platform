/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is responsible for setting up the Keycloak server. It initializes and configures the
 * server according to the provided specifications.
 */
@Singleton
@Slf4j
@SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
public class KeycloakSetup {

  private final HttpClient httpClient;
  private final KeycloakServer keycloakServer;

  public KeycloakSetup(
                       final HttpClient httpClient,
                       final KeycloakServer keycloakServer) {
    this.httpClient = httpClient;
    this.keycloakServer = keycloakServer;
  }

  public void run() {
    try {
      final String keycloakUrl = keycloakServer.getKeycloakServerUrl();
      final HttpResponse<String> response = httpClient.toBlocking()
          .exchange(HttpRequest.GET(keycloakUrl), String.class);

      log.info("Keycloak server response: {}", response.getStatus());
      log.info("Starting admin Keycloak client with url: {}", keycloakUrl);

      keycloakServer.createAirbyteRealm();
    } finally {
      keycloakServer.closeKeycloakAdminClient();
    }
  }

}
