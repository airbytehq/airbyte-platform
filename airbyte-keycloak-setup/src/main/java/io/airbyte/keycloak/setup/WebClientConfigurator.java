/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a web client. It can create and configure the client based on specified
 * parameters.
 */
@Singleton
public class WebClientConfigurator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int HTTP_STATUS_CREATED = 201;
  private static final String LOCAL_OSS_DEV_URI = "https://localhost:3000/*";
  private static final String LOCAL_CLOUD_DEV_URI = "https://localhost:3001/*";

  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final String airbyteUrl;

  public WebClientConfigurator(@Named("airbyteUrl") final String airbyteUrl,
                               final AirbyteKeycloakConfiguration keycloakConfiguration) {
    this.airbyteUrl = airbyteUrl;
    this.keycloakConfiguration = keycloakConfiguration;
  }

  public void configureWebClient(final RealmResource keycloakRealm) {
    final ClientRepresentation clientConfig = getClientRepresentationFromConfig();

    final Optional<ClientRepresentation> existingClient = keycloakRealm.clients().findByClientId(clientConfig.getClientId())
        .stream()
        .findFirst();

    if (existingClient.isPresent()) {
      keycloakRealm.clients().get(existingClient.get().getId()).update(applyConfigToExistingClientRepresentation(existingClient.get()));
      log.info(clientConfig.getClientId() + " client updated successfully.");
    } else {
      try (final Response response = keycloakRealm.clients().create(clientConfig)) {
        if (response.getStatus() == HTTP_STATUS_CREATED) {
          log.info(clientConfig.getClientId() + " client created successfully. Status: " + response.getStatusInfo());
        } else {
          final String errorMessage = String.format("Failed to create %s client.\nReason: %s\nResponse: %s", clientConfig.getClientId(),
              response.getStatusInfo().getReasonPhrase(), response.readEntity(String.class));
          log.error(errorMessage);
          throw new RuntimeException(errorMessage);
        }
      }
    }
  }

  ClientRepresentation getClientRepresentationFromConfig() {
    final ClientRepresentation client = new ClientRepresentation();
    client.setClientId(keycloakConfiguration.getWebClientId());
    client.setPublicClient(true); // Client authentication disabled
    client.setDirectAccessGrantsEnabled(true); // Standard flow authentication
    client.setRedirectUris(getWebClientRedirectUris(airbyteUrl));
    client.setAttributes(getClientAttributes());

    return client;
  }

  private ClientRepresentation applyConfigToExistingClientRepresentation(final ClientRepresentation clientRepresentation) {
    // only change the attributes that come from external configuration
    clientRepresentation.setRedirectUris(getWebClientRedirectUris(airbyteUrl));
    return clientRepresentation;
  }

  private Map<String, String> getClientAttributes() {
    final Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("access.token.lifespan", "180");
    return attributeMap;
  }

  private List<String> getWebClientRedirectUris(final String airbyteUrl) {
    final String normalizedWebappUrl = airbyteUrl.endsWith("/") ? airbyteUrl : airbyteUrl + "/";
    return List.of(normalizedWebappUrl + "*", LOCAL_OSS_DEV_URI, LOCAL_CLOUD_DEV_URI);
  }

}
