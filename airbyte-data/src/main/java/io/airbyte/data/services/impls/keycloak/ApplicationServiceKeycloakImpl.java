/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
import io.airbyte.config.Application;
import io.airbyte.config.User;
import io.airbyte.data.services.ApplicationService;
import jakarta.annotation.Nonnull;
import jakarta.inject.Singleton;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.representations.idm.ClientRepresentation;

/**
 * Application Service for Keycloak.
 * <p>
 * An Application for a user or non-user entity i.e. an organization.
 *
 */
@Singleton
@RequiresAirbyteProEnabled
public class ApplicationServiceKeycloakImpl implements ApplicationService {

  // This number should be kept low or this code will start to do a lot of work.
  public static final int MAX_CREDENTIALS = 2;
  public static final String USER_ID = "user_id";
  public static final String CLIENT_ID = "client_id";
  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final Keycloak keycloakAdminClient;

  public ApplicationServiceKeycloakImpl(
                                        final Keycloak keycloakAdminClient,
                                        final AirbyteKeycloakConfiguration keycloakConfiguration) {
    this.keycloakAdminClient = keycloakAdminClient;
    this.keycloakConfiguration = keycloakConfiguration;
  }

  /**
   * An ID that uniquely identifies the Application in the downstream service. Is used for deletion.
   *
   * @param user The user to create the Application for.
   * @param name The name of the Application.
   * @return The created Application.
   */
  @Override
  @SuppressWarnings("PMD.PreserveStackTrace")
  public Application createApplication(final User user, final String name) {
    try {
      final var existingClients = listApplicationsByUser(user);
      if (existingClients.size() >= MAX_CREDENTIALS) {
        throw new BadRequestException("User already has 2 Applications");
      }
      if (existingClients
          .stream()
          .anyMatch(clientRepresentation -> clientRepresentation.getName().equals(name))) {
        throw new BadRequestException("User already has a key with this name");
      }
      final var clientRepresentation = buildClientRepresentation(user, name, existingClients.size());
      final var response = keycloakAdminClient
          .realm(keycloakConfiguration.getClientRealm())
          .clients()
          .create(clientRepresentation);

      if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
        throw new BadRequestException("Unable to create Application");
      }

      final var client = keycloakAdminClient
          .realm(keycloakConfiguration.getClientRealm())
          .clients()
          .findByClientId(clientRepresentation.getClientId())
          .getFirst();

      final var serviceAccountUser = keycloakAdminClient
          .realm(keycloakConfiguration.getClientRealm())
          .clients()
          .get(client.getId())
          .getServiceAccountUser();

      serviceAccountUser.setAttributes(
          Map.of(
              USER_ID, List.of(String.valueOf(user.getAuthUserId())),
              CLIENT_ID, List.of(client.getClientId())));
      keycloakAdminClient
          .realm(keycloakConfiguration.getClientRealm())
          .users()
          .get(serviceAccountUser.getId())
          .update(serviceAccountUser);

      return toApplication(client);
    } catch (final Exception e) {
      throw new BadRequestException("An unknown exception has occurred: " + e.getMessage());
    }
  }

  /**
   * List all Applications for a user.
   *
   * @param user The user to list Applications for.
   * @return The list of Applications for the user.
   */
  @Override
  public List<Application> listApplicationsByUser(final User user) {
    final var clientUsers = keycloakAdminClient
        .realm(keycloakConfiguration.getClientRealm())
        .users()
        .searchByAttributes(USER_ID + ":" + user.getAuthUserId());

    final var existingClient = new ArrayList<ClientRepresentation>();
    for (final var clientUser : clientUsers) {
      final var client = keycloakAdminClient
          .realm(keycloakConfiguration.getClientRealm())
          .clients()
          .findByClientId(clientUser.getAttributes().get(CLIENT_ID).getFirst())
          .stream()
          .findFirst();

      client.ifPresent(existingClient::add);
    }

    return existingClient
        .stream()
        .map(ApplicationServiceKeycloakImpl::toApplication)
        .toList();
  }

  /**
   * Delete an Application for a user.
   *
   * @param applicationId The ID of the Application to delete.
   * @return The deleted Application.
   */
  @Override
  public Optional<Application> deleteApplication(final User user, final String applicationId) {
    final var client = keycloakAdminClient
        .realm(keycloakConfiguration.getClientRealm())
        .clients()
        .findByClientId(applicationId)
        .stream()
        .findFirst();

    if (client.isEmpty()) {
      return Optional.empty();
    }

    final var userApplications = listApplicationsByUser(user);

    // Only allow the user to delete their own Applications.
    if (userApplications.stream().noneMatch(application -> application.getClientId().equals(applicationId))) {
      throw new BadRequestException("You do not have permission to delete this Application");
    }

    keycloakAdminClient
        .realm(keycloakConfiguration.getClientRealm())
        .clients()
        .get(client.get().getId())
        .remove();

    return Optional.empty();
  }

  /**
   * Build a JWT for a clientId and clientSecret.
   *
   * @param clientId The clientId to build the JWT for.
   * @param clientSecret The clientSecret to build the JWT for.
   * @return The built JWT.
   */
  @Override
  public String getToken(final String clientId, final String clientSecret) {
    final var keycloakClient = KeycloakBuilder
        .builder()
        .serverUrl(keycloakConfiguration.getServerUrl())
        .realm(keycloakConfiguration.getClientRealm())
        .grantType("client_credentials")
        .clientId(clientId)
        .clientSecret(clientSecret)
        .build();

    final var token = keycloakClient
        .tokenManager()
        .getAccessTokenString();

    keycloakClient.close();

    return token;
  }

  /**
   * Build a client representation for a user.
   *
   * @param user The user to build the client representation for.
   * @param name The name of the client.
   * @param index The index of the client.
   * @return The built client representation.
   */
  @Nonnull
  private ClientRepresentation buildClientRepresentation(final User user, final String name, final int index) {
    final var client = new ClientRepresentation();
    client.setClientId(String.valueOf(UUID.randomUUID()));
    client.setServiceAccountsEnabled(true);
    client.setStandardFlowEnabled(false);
    client.setDefaultClientScopes(
        List.of(
            "web-origins",
            "acr",
            "openid",
            "profile",
            "roles",
            "email",
            "airbyte-user"));
    client.setName(name);

    final var attributes = new HashMap<String, String>();
    attributes.put(USER_ID, String.valueOf(user.getAuthUserId()));
    attributes.put("user_id", String.valueOf(user.getUserId()));
    attributes.put("access.token.signed.response.alg", "RS256");
    attributes.put("access.token.lifespan", "31536000");
    attributes.put("use.refresh.tokens", "false");
    client.setAttributes(attributes);

    return client;
  }

  /**
   * Convert a client representation to an Application.
   *
   * @param client The client representation to convert.
   * @return The converted Application.
   */
  private static Application toApplication(final ClientRepresentation client) {
    return new Application()
        .withId(client.getClientId())
        .withName(client.getName())
        .withClientId(client.getClientId())
        .withClientSecret(client.getSecret())
        .withCreatedOn(
            OffsetDateTime.ofInstant(
                Instant.ofEpochSecond(
                    Long.parseLong(client.getAttributes().get("client.secret.creation.time"))),
                ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
  }

}
