/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.InitialUserConfiguration;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.UserRepresentation;

/**
 * This class is responsible for user creation. It includes methods to create user credentials.
 */
@Singleton
@Slf4j
public class UserConfigurator {

  public static final int HTTP_STATUS_CREATED = 201;

  private final InitialUserConfiguration initialUserConfiguration;

  public UserConfigurator(final InitialUserConfiguration initialUserConfiguration) {
    this.initialUserConfiguration = initialUserConfiguration;
  }

  public void configureUser(final RealmResource keycloakRealm) {
    final UserRepresentation userConfig = getUserRepresentationFromConfig();

    final Optional<UserRepresentation> existingUser = keycloakRealm.users().searchByEmail(userConfig.getEmail(), true)
        .stream()
        .findFirst();

    if (existingUser.isPresent()) {
      userConfig.setId(existingUser.get().getId());
      keycloakRealm.users().get(existingUser.get().getId()).update(userConfig);
    } else {
      try (final Response response = keycloakRealm.users().create(userConfig)) {
        if (response.getStatus() == HTTP_STATUS_CREATED) {
          log.info(userConfig.getUsername() + " user created successfully. Status: " + response.getStatusInfo());
        } else {
          final String errorMessage = String.format("Failed to create %s user.\nReason: %s\nResponse: %s", userConfig.getUsername(),
              response.getStatusInfo().getReasonPhrase(), response.readEntity(String.class));
          log.error(errorMessage);
          throw new RuntimeException(errorMessage);
        }
      }
    }
  }

  UserRepresentation getUserRepresentationFromConfig() {
    final UserRepresentation user = new UserRepresentation();
    user.setUsername(initialUserConfiguration.getEmail());
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

}
