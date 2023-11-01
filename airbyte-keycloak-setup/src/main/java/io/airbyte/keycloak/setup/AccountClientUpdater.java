/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;

/**
 * This class provides functionality to update account client settings. It includes a method to
 * change home url for the account client.
 */
@Singleton
@Slf4j
public class AccountClientUpdater {

  private final String webappUrl;
  private final AirbyteKeycloakConfiguration keycloakConfiguration;

  public AccountClientUpdater(@Value("${airbyte.webapp-url}") final String webappUrl,
                              final AirbyteKeycloakConfiguration keycloakConfiguration) {
    this.webappUrl = webappUrl;
    this.keycloakConfiguration = keycloakConfiguration;
  }

  public void updateAccountClientHomeUrl(final RealmResource airbyteRealm) {
    List<ClientRepresentation> clients = airbyteRealm.clients().findAll();
    ClientRepresentation clientRepresentation = clients.stream()
        .filter(client -> keycloakConfiguration.getAccountClientId().equals(client.getClientId()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Account client not found"));
    clientRepresentation.setBaseUrl(webappUrl);

    airbyteRealm.clients().get(clientRepresentation.getId()).update(clientRepresentation);
  }

}
