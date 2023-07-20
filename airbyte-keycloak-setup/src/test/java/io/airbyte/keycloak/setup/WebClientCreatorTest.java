/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import javax.ws.rs.core.Response;
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
class WebClientCreatorTest {

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
  private WebClientCreator webClientCreator;

  @BeforeEach
  void setUp() {
    when(keycloakConfiguration.getWebClientId()).thenReturn(WEB_CLIENT_ID);
    webClientCreator = new WebClientCreator(WEBAPP_URL, keycloakConfiguration);
  }

  @Test
  void testCreateWebClient() {
    when(realmResource.clients()).thenReturn(clientsResource);
    when(clientsResource.create(any(ClientRepresentation.class))).thenReturn(response);
    when(response.getStatus()).thenReturn(201);

    webClientCreator.createWebClient(realmResource);

    verify(clientsResource).create(any(ClientRepresentation.class));
  }

  @Test
  void testCreateClientRepresentation() {
    when(keycloakConfiguration.getWebClientId()).thenReturn(WEB_CLIENT_ID);

    ClientRepresentation clientRepresentation = webClientCreator.createClientRepresentation();

    assertEquals(WEB_CLIENT_ID, clientRepresentation.getClientId());
    assertTrue(clientRepresentation.isPublicClient());
    assertTrue(clientRepresentation.isDirectAccessGrantsEnabled());
    assertEquals(WEBAPP_URL, clientRepresentation.getBaseUrl());
    assertEquals("180", clientRepresentation.getAttributes().get("access.token.lifespan"));
  }

}
