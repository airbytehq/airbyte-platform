/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.InitialUserConfiguration;
import jakarta.inject.Singleton;
import java.util.Arrays;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.admin.client.resource.UsersResource;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.UserRepresentation;

/**
 * This class is responsible for user creation. It includes methods to create user credentials.
 */
@Singleton
@Slf4j
public class UserCreator {

  public static final int HTTP_STATUS_CREATED = 201;

  private final InitialUserConfiguration initialUserConfiguration;

  public UserCreator(final InitialUserConfiguration initialUserConfiguration) {
    this.initialUserConfiguration = initialUserConfiguration;
  }

  public void createUser(final RealmResource keycloakRealm) {
    final UserRepresentation user = createUserRepresentation();
    final Response response = keycloakRealm.users().create(user);

    if (response.getStatus() == HTTP_STATUS_CREATED) {
      log.info("User {} created successfully.", user.getFirstName());
    } else {
      log.info("Failed to create user. Status: " + response.getStatusInfo().getReasonPhrase());
    }
  }

  UserRepresentation createUserRepresentation() {
    final UserRepresentation user = new UserRepresentation();
    user.setUsername(initialUserConfiguration.getUsername());
    user.setEnabled(true);
    user.setEmail(initialUserConfiguration.getEmail());
    user.setFirstName(initialUserConfiguration.getFirstName());
    user.setLastName(initialUserConfiguration.getLastName());
    user.setCredentials(Arrays.asList(createCredentialRepresentation()));
    return user;
  }

  CredentialRepresentation createCredentialRepresentation() {
    final CredentialRepresentation password = new CredentialRepresentation();
    password.setTemporary(false);
    password.setType(CredentialRepresentation.PASSWORD);
    password.setValue(initialUserConfiguration.getPassword());
    return password;
  }

  /**
   * This method resets the user by deleting all users and re-creating them.
   */
  public void resetUser(final RealmResource realmResource) {
    UsersResource usersResource = realmResource.users();

    usersResource.list().forEach(user -> usersResource.delete(user.getId()));

    createUser(realmResource);
  }

}
