/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.InitialUserConfig;
import jakarta.ws.rs.core.Response;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.admin.client.resource.UserResource;
import org.keycloak.admin.client.resource.UsersResource;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@SuppressWarnings("PMD.LiteralsFirstInComparisons")
class UserConfiguratorTest {

  private static final String EMAIL = "jon@airbyte.io";
  private static final String FIRST_NAME = "Jon";
  private static final String LAST_NAME = "Smith";
  private static final String PASSWORD = "airbytePassword";
  private static final String KEYCLOAK_USER_ID = "some-id";

  // set up a static Keycloak UserRepresentation based on the constants above
  private static final UserRepresentation USER_REPRESENTATION = new UserRepresentation();

  static {
    USER_REPRESENTATION.setId(KEYCLOAK_USER_ID);
    USER_REPRESENTATION.setUsername(EMAIL);
    USER_REPRESENTATION.setEmail(EMAIL);
    USER_REPRESENTATION.setFirstName(FIRST_NAME);
    USER_REPRESENTATION.setLastName(LAST_NAME);
    USER_REPRESENTATION.setEnabled(true);

    final CredentialRepresentation credentialRepresentation = new CredentialRepresentation();
    credentialRepresentation.setType(CredentialRepresentation.PASSWORD);
    credentialRepresentation.setValue(PASSWORD);
    credentialRepresentation.setTemporary(false);

    USER_REPRESENTATION.setCredentials(Collections.singletonList(credentialRepresentation));
  }

  private UserConfigurator userConfigurator;
  @Mock
  private InitialUserConfig initialUserConfig;
  @Mock
  private RealmResource realmResource;
  @Mock
  private UsersResource usersResource;
  @Mock
  private UserResource userResource;
  @Mock
  private Response response;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);

    when(initialUserConfig.getEmail()).thenReturn(EMAIL);
    when(initialUserConfig.getFirstName()).thenReturn(FIRST_NAME);
    when(initialUserConfig.getLastName()).thenReturn(LAST_NAME);
    when(initialUserConfig.getPassword()).thenReturn(PASSWORD);

    when(realmResource.users()).thenReturn(usersResource);
    when(usersResource.create(any(UserRepresentation.class))).thenReturn(response);

    when(usersResource.get(KEYCLOAK_USER_ID)).thenReturn(userResource);
    when(response.getStatusInfo()).thenReturn(Response.Status.OK);

    userConfigurator = new UserConfigurator(initialUserConfig);
  }

  @Test
  void testConfigureUser() {
    when(response.getStatus()).thenReturn(201);

    userConfigurator.configureUser(realmResource);

    verify(usersResource).create(argThat(userRepresentation -> userRepresentation.getId() == null
        && userRepresentation.getUsername().equals(EMAIL)
        && userRepresentation.getEmail().equals(EMAIL)
        && userRepresentation.getFirstName().equals(FIRST_NAME)
        && userRepresentation.getLastName().equals(LAST_NAME)
        && userRepresentation.isEnabled()
        && userRepresentation.getCredentials().size() == 1
        && userRepresentation.getCredentials().getFirst().getType().equals(CredentialRepresentation.PASSWORD)
        && userRepresentation.getCredentials().getFirst().getValue().equals(PASSWORD)
        && !userRepresentation.getCredentials().getFirst().isTemporary()
        && userRepresentation.getCredentials().equals(USER_REPRESENTATION.getCredentials())));
  }

  @Test
  void testConfigureUserAlreadyExists() {
    when(usersResource.searchByEmail(EMAIL, true)).thenReturn(Collections.singletonList(USER_REPRESENTATION));

    userConfigurator.configureUser(realmResource);

    verify(usersResource, never()).create(any());
    verify(userResource).update(argThat(userRepresentation -> userRepresentation.getId().equals(USER_REPRESENTATION.getId())
        && userRepresentation.getUsername().equals(USER_REPRESENTATION.getUsername())
        && userRepresentation.getEmail().equals(USER_REPRESENTATION.getEmail())
        && userRepresentation.getFirstName().equals(USER_REPRESENTATION.getFirstName())
        && userRepresentation.getLastName().equals(USER_REPRESENTATION.getLastName())
        && userRepresentation.isEnabled().equals(USER_REPRESENTATION.isEnabled())
        && userRepresentation.getCredentials().equals(USER_REPRESENTATION.getCredentials())));
  }

  @Test
  void testConfigureUserRepresentation() {
    when(initialUserConfig.getEmail()).thenReturn(EMAIL);
    when(initialUserConfig.getFirstName()).thenReturn(FIRST_NAME);
    when(initialUserConfig.getLastName()).thenReturn(LAST_NAME);

    final UserRepresentation userRepresentation = userConfigurator.getUserRepresentationFromConfig();

    assertEquals(EMAIL, userRepresentation.getUsername()); // we want to set the username to the configured email
    assertEquals(EMAIL, userRepresentation.getEmail());
    assertEquals(FIRST_NAME, userRepresentation.getFirstName());
    assertEquals(LAST_NAME, userRepresentation.getLastName());
  }

  @Test
  void testCreateCredentialRepresentation() {
    when(initialUserConfig.getPassword()).thenReturn(PASSWORD);

    final CredentialRepresentation credentialRepresentation = userConfigurator.createCredentialRepresentation();

    assertFalse(credentialRepresentation.isTemporary());
    assertEquals(CredentialRepresentation.PASSWORD, credentialRepresentation.getType());
    assertEquals(PASSWORD, credentialRepresentation.getValue());
  }

}
