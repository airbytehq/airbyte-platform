/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.config.Application;
import io.airbyte.config.User;
import io.airbyte.data.services.ApplicationService;
import jakarta.inject.Singleton;
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
import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Response;
import org.jetbrains.annotations.NotNull;
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
public class ApplicationServiceKeycloakImpl implements ApplicationService {

  // This number should be kept low or this code will start to do a lot of work.
  public static final int MAX_CREDENTIALS = 2;
  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final Keycloak keycloakAdminClient;

  public ApplicationServiceKeycloakImpl(
                                        Keycloak keycloakAdminClient,
                                        AirbyteKeycloakConfiguration keycloakConfiguration) {
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
  public Application createApplication(User user, String name) {
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
      var response = keycloakAdminClient
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
              "user_id", List.of(String.valueOf(user.getUserId())),
              "client_id", List.of(client.getClientId())));
      keycloakAdminClient
          .realm(keycloakConfiguration.getClientRealm())
          .users()
          .get(serviceAccountUser.getId())
          .update(serviceAccountUser);

      return toApplication(client);
    } catch (Exception e) {
      throw new BadRequestException("An unknown exception has occurred: " + e.getMessage());
    }
  }

  /**
   * List all Applications for a user.
   *
   * @param userId The user to list Applications for.
   * @return The list of Applications for the user.
   */
  @Override
  public List<Application> listApplicationsByUser(User userId) {
    final var users = keycloakAdminClient
        .realm(keycloakConfiguration.getClientRealm())
        .users()
        .searchByAttributes("user_id:" + userId.getUserId());

    final var existingClient = new ArrayList<ClientRepresentation>();
    for (final var user : users) {
      final var client = keycloakAdminClient
          .realm(keycloakConfiguration.getClientRealm())
          .clients()
          .findByClientId(user.getAttributes().get("client_id").getFirst())
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
  public Optional<Application> deleteApplication(User user, String applicationId) {
    final var client = keycloakAdminClient
        .realm(keycloakConfiguration.getClientRealm())
        .clients()
        .findByClientId(applicationId)
        .stream()
        .findFirst();

    if (client.isEmpty()) {
      return Optional.empty();
    }

    // Get the user_id attribute from the client
    final var userId = client.get().getAttributes().getOrDefault("user_id", null);
    if (userId == null) {
      throw new BadRequestException("Client does not have a user_id attribute");
    }

    if (!userId.equals(String.valueOf(user.getUserId()))) {
      throw new BadRequestException("");
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
  public String getToken(String clientId, String clientSecret) {
    final var keycloakClient = KeycloakBuilder
        .builder()
        .serverUrl(keycloakConfiguration.getProtocol() + "://" + keycloakConfiguration.getHost())
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
  @NotNull
  private ClientRepresentation buildClientRepresentation(User user, String name, int index) {
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

    var attributes = new HashMap<String, String>();
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
  private static Application toApplication(ClientRepresentation client) {
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
