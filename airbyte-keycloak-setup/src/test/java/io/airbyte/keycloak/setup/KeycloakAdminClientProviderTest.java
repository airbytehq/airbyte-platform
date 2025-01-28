/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.admin.client.Keycloak;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KeycloakAdminClientProviderTest {

  private static final String KEYCLOAK_URL = "http://localhost:8180/auth";
  private static final String REALM = "airbyte";
  private static final String CLIENT_ID = "admin-cli";
  private static final String USERNAME = "admin";
  private static final String PASSWORD = "admin";

  @Mock
  private AirbyteKeycloakConfiguration keycloakConfiguration;

  @InjectMocks
  private KeycloakAdminClientProvider keycloakAdminClientProvider;

  @BeforeEach
  void setUp() {
    when(keycloakConfiguration.getRealm()).thenReturn(REALM);
    when(keycloakConfiguration.getClientId()).thenReturn(CLIENT_ID);
    when(keycloakConfiguration.getUsername()).thenReturn(USERNAME);
    when(keycloakConfiguration.getPassword()).thenReturn(PASSWORD);
  }

  @Test
  void testCreateKeycloakAdminClient() {
    Keycloak keycloak = keycloakAdminClientProvider.createKeycloakAdminClient(KEYCLOAK_URL);

    assertNotNull(keycloak);
    verify(keycloakConfiguration, times(1)).getRealm();
    verify(keycloakConfiguration, times(1)).getClientId();
    verify(keycloakConfiguration, times(1)).getUsername();
    verify(keycloakConfiguration, times(1)).getPassword();
  }

}
