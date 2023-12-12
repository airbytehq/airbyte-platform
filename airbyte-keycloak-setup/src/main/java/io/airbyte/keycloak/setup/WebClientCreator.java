/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;

/**
 * This class provides a web client. It can create and configure the client based on specified
 * parameters.
 */
@Singleton
@Slf4j
public class WebClientCreator {

  public static final int HTTP_STATUS_CREATED = 201;
  private static final String LOCAL_OSS_DEV_URI = "https://localhost:3000/*";
  private static final String LOCAL_CLOUD_DEV_URI = "https://localhost:3001/*";

  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final String webappUrl;

  public WebClientCreator(@Value("${airbyte.webapp-url}") final String webappUrl,
                          final AirbyteKeycloakConfiguration keycloakConfiguration) {
    this.webappUrl = webappUrl;
    this.keycloakConfiguration = keycloakConfiguration;
  }

  public void createWebClient(RealmResource keycloakRealm) {
    final ClientRepresentation client = createClientRepresentation();
    final Response clientResponse = keycloakRealm.clients().create(client);
    handleClientCreationResponse(clientResponse, client.getClientId());
  }

  ClientRepresentation createClientRepresentation() {
    final ClientRepresentation client = new ClientRepresentation();
    client.setClientId(keycloakConfiguration.getWebClientId());
    client.setPublicClient(true); // Client authentication disabled
    client.setDirectAccessGrantsEnabled(true); // Standard flow authentication
    client.setRedirectUris(getWebClientRedirectUris(webappUrl));
    client.setBaseUrl(webappUrl);
    client.setAttributes(getClientAttributes());

    return client;
  }

  private Map<String, String> getClientAttributes() {
    final Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("access.token.lifespan", "180");
    return attributeMap;
  }

  private void handleClientCreationResponse(Response clientResponse, String webClientId) {
    if (clientResponse.getStatus() == HTTP_STATUS_CREATED) {
      log.info(webClientId + " client created successfully. Status: " + clientResponse.getStatusInfo());
    } else {
      log.info("Failed to create " + webClientId + " client. Status: " + clientResponse.getStatusInfo().getReasonPhrase());
    }
  }

  private List<String> getWebClientRedirectUris(final String webappUrl) {
    final String normalizedWebappUrl = webappUrl.endsWith("/") ? webappUrl : webappUrl + "/";
    return List.of(normalizedWebappUrl + "*", LOCAL_OSS_DEV_URI, LOCAL_CLOUD_DEV_URI);
  }

}
