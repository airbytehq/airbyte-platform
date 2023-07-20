/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.admin.client.resource.ClientResource;
import org.keycloak.admin.client.resource.ClientsResource;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AccountClientUpdaterTest {

  private static final String WEBAPP_URL = "http://localhost:8000";
  private static final String ACCOUNT_CLIENT_ID = "account";

  @Mock
  private RealmResource realmResource;
  @Mock
  private ClientsResource clientsResource;
  @Mock
  private ClientResource clientResource;
  @Mock
  private AirbyteKeycloakConfiguration keycloakConfiguration;
  @InjectMocks
  private AccountClientUpdater accountClientUpdater;

  @BeforeEach
  void setUp() {
    when(keycloakConfiguration.getAccountClientId()).thenReturn(ACCOUNT_CLIENT_ID);
    accountClientUpdater = new AccountClientUpdater(WEBAPP_URL, keycloakConfiguration);
  }

  @Test
  void testUpdateAccountClientHomeUrl() {
    ClientRepresentation accountClient = new ClientRepresentation();
    accountClient.setClientId(ACCOUNT_CLIENT_ID);

    ClientRepresentation anotherClient = new ClientRepresentation();
    anotherClient.setClientId("another-account");

    List<ClientRepresentation> clients = Arrays.asList(accountClient, anotherClient);

    when(realmResource.clients()).thenReturn(clientsResource);
    when(clientsResource.findAll()).thenReturn(clients);
    when(clientsResource.get(accountClient.getId())).thenReturn(clientResource);

    doNothing().when(clientResource).update(accountClient);

    assertDoesNotThrow(() -> accountClientUpdater.updateAccountClientHomeUrl(realmResource));

    verify(realmResource, times(2)).clients();
    verify(clientsResource).findAll();
    verify(clientsResource).get(accountClient.getId());
    verify(clientResource).update(accountClient);
  }

  @Test
  void testUpdateAccountClientHomeUrl_ClientNotFound() {
    ClientRepresentation clientRepresentation = new ClientRepresentation();
    clientRepresentation.setClientId("differentClientId");

    when(realmResource.clients()).thenReturn(clientsResource);
    when(clientsResource.findAll()).thenReturn(List.of(clientRepresentation));

    assertThrows(RuntimeException.class, () -> accountClientUpdater.updateAccountClientHomeUrl(realmResource));
  }

}
