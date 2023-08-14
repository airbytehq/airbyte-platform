/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.admin.client.resource.RealmsResource;
import org.keycloak.representations.idm.RealmRepresentation;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class KeycloakServerTest {

  private static final String REALM_NAME = "airbyte";

  @Mock
  private KeycloakAdminClientProvider keycloakAdminClientProvider;
  @Mock
  private AirbyteKeycloakConfiguration keycloakConfiguration;
  @Mock
  private UserCreator userCreator;
  @Mock
  private WebClientCreator webClientCreator;
  @Mock
  private IdentityProvidersCreator identityProvidersCreator;
  @Mock
  private AccountClientUpdater accountClientUpdater;
  @Mock
  private Keycloak keycloakAdminClient;
  @Mock
  private RealmsResource realmsResource;
  @Mock
  private RealmResource realmResource;
  private KeycloakServer keycloakServer;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);

    when(keycloakConfiguration.getBasePath()).thenReturn("/auth");
    when(keycloakConfiguration.getProtocol()).thenReturn("http");
    when(keycloakConfiguration.getHost()).thenReturn("localhost");
    when(keycloakConfiguration.getAirbyteRealm()).thenReturn(REALM_NAME);
    when(keycloakAdminClientProvider.createKeycloakAdminClient(anyString())).thenReturn(keycloakAdminClient);
    when(keycloakAdminClient.realms()).thenReturn(realmsResource);
    when(realmsResource.findAll()).thenReturn(Collections.emptyList());
    when(keycloakAdminClient.realm(anyString())).thenReturn(realmResource);

    keycloakServer = new KeycloakServer(keycloakAdminClientProvider,
        keycloakConfiguration,
        userCreator,
        webClientCreator,
        identityProvidersCreator,
        accountClientUpdater);
  }

  @Test
  void testCreateAirbyteRealm() {
    keycloakServer.createAirbyteRealm();

    verify(realmsResource, times(1)).findAll();
    verify(realmsResource, times(1)).create(any());
    verify(userCreator, times(1)).createUser(realmResource);
    verify(webClientCreator, times(1)).createWebClient(realmResource);
    verify(identityProvidersCreator, times(1)).createIdps(realmResource);
    verify(accountClientUpdater, times(1)).updateAccountClientHomeUrl(realmResource);
  }

  @Test
  void testCreateAirbyteRealmWhenRealmAlreadyExists() {
    final RealmRepresentation existingRealm = new RealmRepresentation();
    existingRealm.setRealm(REALM_NAME);

    when(realmsResource.findAll()).thenReturn(Collections.singletonList(existingRealm));
    when(keycloakConfiguration.getAirbyteRealm()).thenReturn(REALM_NAME);

    keycloakServer.createAirbyteRealm();

    verify(realmsResource, times(1)).findAll();
    verify(realmsResource, times(0)).create(any());
    verify(userCreator, times(0)).createUser(any());
    verify(webClientCreator, times(0)).createWebClient(any());
    verify(identityProvidersCreator, times(0)).createIdps(any());
    verify(accountClientUpdater, times(0)).updateAccountClientHomeUrl(any());
  }

  @Test
  void testBuildRealmRepresentation() {
    keycloakServer.createAirbyteRealm();

    final ArgumentCaptor<RealmRepresentation> realmRepresentationCaptor = ArgumentCaptor.forClass(RealmRepresentation.class);
    verify(realmsResource).create(realmRepresentationCaptor.capture());

    final RealmRepresentation createdRealm = realmRepresentationCaptor.getValue();
    assertEquals(REALM_NAME, createdRealm.getRealm());
    assertTrue(createdRealm.isEnabled());
    assertEquals("airbyte-keycloak-theme", createdRealm.getLoginTheme());
  }

  @Test
  void testCreateAirbyteRealmWhenRealmAlreadyExistsAndResetUserIsTrue() {
    final RealmRepresentation existingRealm = new RealmRepresentation();
    existingRealm.setRealm(REALM_NAME);

    when(realmsResource.findAll()).thenReturn(Collections.singletonList(existingRealm));
    when(keycloakConfiguration.getAirbyteRealm()).thenReturn(REALM_NAME);
    when(keycloakConfiguration.getResetRealm()).thenReturn(true);

    keycloakServer.createAirbyteRealm();

    verify(realmsResource, times(1)).findAll();
    verify(realmsResource, times(0)).create(any());
    verify(userCreator, times(1)).resetUser(any());
    verify(identityProvidersCreator, times(1)).resetIdentityProviders(any());
  }

}
