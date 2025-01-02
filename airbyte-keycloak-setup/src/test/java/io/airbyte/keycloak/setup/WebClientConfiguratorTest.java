/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.admin.client.resource.ClientsResource;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WebClientConfiguratorTest {

  private static final String WEBAPP_URL = "http://localhost:8000";
  private static final String WEB_CLIENT_ID = "airbyte-okta";
  @Mock
  private AirbyteKeycloakConfiguration keycloakConfiguration;
  @Mock
  private RealmResource realmResource;
  @Mock
  private ClientsResource clientsResource;
  @Mock
  private Response response;
  @InjectMocks
  private WebClientConfigurator webClientConfigurator;

  @BeforeEach
  void setUp() {
    when(keycloakConfiguration.getWebClientId()).thenReturn(WEB_CLIENT_ID);
    webClientConfigurator = new WebClientConfigurator(WEBAPP_URL, keycloakConfiguration);
  }

  @Test
  void testCreateWebClient() {
    when(realmResource.clients()).thenReturn(clientsResource);
    when(clientsResource.create(any(ClientRepresentation.class))).thenReturn(response);
    when(response.getStatus()).thenReturn(201);

    webClientConfigurator.configureWebClient(realmResource);

    verify(clientsResource).create(any(ClientRepresentation.class));
  }

  @Test
  void testCreateClientRepresentation() {
    when(keycloakConfiguration.getWebClientId()).thenReturn(WEB_CLIENT_ID);

    final ClientRepresentation clientRepresentation = webClientConfigurator.getClientRepresentationFromConfig();

    assertEquals(WEB_CLIENT_ID, clientRepresentation.getClientId());
    assertTrue(clientRepresentation.isPublicClient());
    assertTrue(clientRepresentation.isDirectAccessGrantsEnabled());
    assertEquals("180", clientRepresentation.getAttributes().get("access.token.lifespan"));
  }

}
