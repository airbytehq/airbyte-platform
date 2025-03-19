/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator;
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
  private static final String WEBAPP_URL = "http://localhost:8000";
  private static final String AUTH_PATH = "/io/airbyte/api/client/auth";
  private static final String FRONTEND_URL_ATTRIBUTE = "frontendUrl";

  @Mock
  private KeycloakAdminClientProvider keycloakAdminClientProvider;
  @Mock
  private AirbyteKeycloakConfiguration keycloakConfiguration;
  @Mock
  private UserConfigurator userConfigurator;
  @Mock
  private WebClientConfigurator webClientConfigurator;
  @Mock
  private IdentityProvidersConfigurator identityProvidersConfigurator;
  @Mock
  private ClientScopeConfigurator clientScopeConfigurator;
  @Mock
  private Keycloak keycloakAdminClient;
  @Mock
  private RealmsResource realmsResource;
  @Mock
  private RealmResource airbyteRealm;
  @Mock
  private RealmRepresentation airbyteRealmRep;

  private KeycloakServer keycloakServer;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);

    when(keycloakConfiguration.getBasePath()).thenReturn("/io/airbyte/api/client/auth");
    when(keycloakConfiguration.getProtocol()).thenReturn("http");
    when(keycloakConfiguration.getHost()).thenReturn("localhost");
    when(keycloakConfiguration.getAirbyteRealm()).thenReturn(REALM_NAME);
    when(keycloakAdminClientProvider.createKeycloakAdminClient(anyString())).thenReturn(keycloakAdminClient);
    when(keycloakAdminClient.realms()).thenReturn(realmsResource);
    when(realmsResource.findAll()).thenReturn(Collections.emptyList());
    when(keycloakAdminClient.realm(anyString())).thenReturn(airbyteRealm);
    when(airbyteRealm.toRepresentation()).thenReturn(airbyteRealmRep);

    keycloakServer = new KeycloakServer(keycloakAdminClientProvider,
        keycloakConfiguration,
        userConfigurator,
        webClientConfigurator,
        identityProvidersConfigurator,
        clientScopeConfigurator,
        WEBAPP_URL);
  }

  @Test
  void testSetupAirbyteRealmWhenRealmDoesNotExist() {
    keycloakServer.setupAirbyteRealm();

    verify(realmsResource, times(1)).findAll();
    verify(realmsResource, times(1)).create(any());
    verify(userConfigurator, times(1)).configureUser(airbyteRealm);
    verify(webClientConfigurator, times(1)).configureWebClient(airbyteRealm);
    verify(identityProvidersConfigurator, times(1)).configureIdp(airbyteRealm);
    verify(airbyteRealmRep, times(1)).setAttributes(argThat(map -> map.get(FRONTEND_URL_ATTRIBUTE).equals(WEBAPP_URL + AUTH_PATH)));
    verify(airbyteRealm, times(1)).update(airbyteRealmRep);
  }

  @Test
  void testCreateAirbyteRealmWhenRealmAlreadyExists() {
    final RealmRepresentation existingRealm = new RealmRepresentation();
    existingRealm.setRealm(REALM_NAME);

    when(realmsResource.findAll()).thenReturn(Collections.singletonList(existingRealm));
    when(keycloakConfiguration.getAirbyteRealm()).thenReturn(REALM_NAME);

    keycloakServer.setupAirbyteRealm();

    verify(realmsResource, times(1)).findAll();
    verify(realmsResource, times(0)).create(any()); // create not called, but other configuration methods should be called every time
    verify(userConfigurator, times(1)).configureUser(any());
    verify(webClientConfigurator, times(1)).configureWebClient(any());
    verify(identityProvidersConfigurator, times(1)).configureIdp(any());
    verify(airbyteRealmRep, times(1)).setAttributes(argThat(map -> map.get(FRONTEND_URL_ATTRIBUTE).equals(WEBAPP_URL + AUTH_PATH)));
    verify(airbyteRealm, times(1)).update(airbyteRealmRep);
  }

  @Test
  void testBuildRealmRepresentation() {
    keycloakServer.setupAirbyteRealm();

    final ArgumentCaptor<RealmRepresentation> realmRepresentationCaptor = ArgumentCaptor.forClass(RealmRepresentation.class);
    verify(realmsResource).create(realmRepresentationCaptor.capture());

    final RealmRepresentation createdRealm = realmRepresentationCaptor.getValue();
    assertEquals(REALM_NAME, createdRealm.getRealm());
    assertTrue(createdRealm.isEnabled());
    assertEquals("airbyte-keycloak-theme", createdRealm.getLoginTheme());
  }

  @Test
  void testRecreateAirbyteRealm() {
    final RealmRepresentation existingRealm = new RealmRepresentation();
    existingRealm.setRealm(REALM_NAME);
    when(realmsResource.findAll()).thenReturn(Collections.singletonList(existingRealm));

    keycloakServer.destroyAndRecreateAirbyteRealm();

    verify(airbyteRealm, times(1)).remove();
    verify(realmsResource, times(1)).create(any());
    verify(userConfigurator, times(1)).configureUser(airbyteRealm);
    verify(webClientConfigurator, times(1)).configureWebClient(airbyteRealm);
    verify(identityProvidersConfigurator, times(1)).configureIdp(airbyteRealm);
    verify(airbyteRealmRep, times(1)).setAttributes(argThat(map -> map.get(FRONTEND_URL_ATTRIBUTE).equals(WEBAPP_URL + AUTH_PATH)));
    verify(airbyteRealm, times(1)).update(airbyteRealmRep);
  }

  @Test
  void testRecreateAirbyteRealmWhenRealmDoesNotExist() {
    when(realmsResource.findAll()).thenReturn(Collections.emptyList());

    keycloakServer.destroyAndRecreateAirbyteRealm();

    // should behave the same as createAirbyteRealm in this case.
    verify(airbyteRealm, times(0)).remove();
    verify(realmsResource, times(1)).create(any());
    verify(userConfigurator, times(1)).configureUser(airbyteRealm);
    verify(webClientConfigurator, times(1)).configureWebClient(airbyteRealm);
    verify(identityProvidersConfigurator, times(1)).configureIdp(airbyteRealm);
    verify(airbyteRealmRep, times(1)).setAttributes(argThat(map -> map.get(FRONTEND_URL_ATTRIBUTE).equals(WEBAPP_URL + AUTH_PATH)));
    verify(airbyteRealm, times(1)).update(airbyteRealmRep);
  }

}
