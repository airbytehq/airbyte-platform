/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.InitialUserConfiguration;
import java.util.Collections;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.admin.client.resource.UserResource;
import org.keycloak.admin.client.resource.UsersResource;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class UserCreatorTest {

  private static final String USERNAME = "jon";
  private static final String EMAIL = "jon@airbyte.io";
  private static final String FIRST_NAME = "Jon";
  private static final String LAST_NAME = "Smith";
  private static final String PASSWORD = "airbytePassword";
  private UserCreator userCreator;
  @Mock
  private InitialUserConfiguration initialUserConfiguration;
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

    when(initialUserConfiguration.getUsername()).thenReturn(USERNAME);
    when(initialUserConfiguration.getEmail()).thenReturn(EMAIL);
    when(initialUserConfiguration.getFirstName()).thenReturn(FIRST_NAME);
    when(initialUserConfiguration.getLastName()).thenReturn(LAST_NAME);
    when(initialUserConfiguration.getPassword()).thenReturn(PASSWORD);

    when(realmResource.users()).thenReturn(usersResource);
    when(usersResource.create(any(UserRepresentation.class))).thenReturn(response);

    when(usersResource.get(anyString())).thenReturn(userResource);
    when(usersResource.search(anyString(), anyInt(), anyInt())).thenReturn(Collections.singletonList(new UserRepresentation()));
    when(response.getStatusInfo()).thenReturn(Response.Status.OK);

    userCreator = new UserCreator(initialUserConfiguration);
  }

  @Test
  void testCreateUser() {
    when(response.getStatus()).thenReturn(201);

    userCreator.createUser(realmResource);

    verify(usersResource).create(any(UserRepresentation.class));
  }

  @Test
  void testCreateUserRepresentation() {
    when(initialUserConfiguration.getUsername()).thenReturn(USERNAME);
    when(initialUserConfiguration.getEmail()).thenReturn(EMAIL);
    when(initialUserConfiguration.getFirstName()).thenReturn(FIRST_NAME);
    when(initialUserConfiguration.getLastName()).thenReturn(LAST_NAME);

    UserRepresentation userRepresentation = userCreator.createUserRepresentation();

    assertEquals(USERNAME, userRepresentation.getUsername());
    assertEquals(EMAIL, userRepresentation.getEmail());
    assertEquals(FIRST_NAME, userRepresentation.getFirstName());
    assertEquals(LAST_NAME, userRepresentation.getLastName());
  }

  @Test
  void testCreateCredentialRepresentation() {
    when(initialUserConfiguration.getPassword()).thenReturn(PASSWORD);

    CredentialRepresentation credentialRepresentation = userCreator.createCredentialRepresentation();

    assertFalse(credentialRepresentation.isTemporary());
    assertEquals(CredentialRepresentation.PASSWORD, credentialRepresentation.getType());
    assertEquals(PASSWORD, credentialRepresentation.getValue());
  }

  @Test
  void testResetUser() {
    UserRepresentation userRepresentation = new UserRepresentation();
    userRepresentation.setId("id1");
    when(usersResource.list()).thenReturn(Collections.singletonList(userRepresentation));

    when(response.getStatus()).thenReturn(201);

    userCreator.resetUser(realmResource);

    verify(usersResource).list();
    verify(usersResource).delete("id1");
    verify(usersResource).create(any(UserRepresentation.class));
  }

}
