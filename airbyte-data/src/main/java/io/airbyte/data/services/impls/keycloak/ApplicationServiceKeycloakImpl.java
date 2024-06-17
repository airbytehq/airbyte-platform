/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak;

import io.airbyte.commons.auth.RequiresAuthMode;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.auth.config.AuthMode;
import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator;
import io.airbyte.commons.auth.support.UserAuthenticationResolver;
import io.airbyte.config.Application;
import io.airbyte.config.User;
import io.airbyte.data.services.ApplicationService;
import jakarta.annotation.Nonnull;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
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
import org.keycloak.admin.client.resource.ClientsResource;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.admin.client.resource.UsersResource;
import org.keycloak.representations.idm.ClientRepresentation;

/**
 * Application Service for Keycloak.
 * <p>
 * An Application for a user or non-user entity i.e. an organization.
 */
@Singleton
@RequiresAuthMode(AuthMode.OIDC)
public class ApplicationServiceKeycloakImpl implements ApplicationService {

  // This number should be kept low or this code will start to do a lot of work.
  public static final int MAX_CREDENTIALS = 2;
  public static final String USER_ID = "user_id";
  public static final String CLIENT_ID = "client_id";
  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final Keycloak keycloakAdminClient;
  private final UserAuthenticationResolver userAuthenticationResolver;
  private final ClientScopeConfigurator clientScopeConfigurator;
  private final Duration accessTokenExpirationTime;

  public ApplicationServiceKeycloakImpl(
                                        final Keycloak keycloakAdminClient,
                                        final AirbyteKeycloakConfiguration keycloakConfiguration,
                                        final UserAuthenticationResolver userAuthenticationResolver,
                                        final ClientScopeConfigurator clientScopeConfigurator,
                                        @Named("access-token-expiration-time") final Duration accessTokenExpirationTime) {
    this.keycloakAdminClient = keycloakAdminClient;
    this.keycloakConfiguration = keycloakConfiguration;
    this.userAuthenticationResolver = userAuthenticationResolver;
    this.clientScopeConfigurator = clientScopeConfigurator;
    this.accessTokenExpirationTime = accessTokenExpirationTime;
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
      final String userRealmName = getCurrentUserRealmName();
      final RealmResource realmResource = keycloakAdminClient.realm(userRealmName);
      final ClientsResource clientsResource = realmResource.clients();
      final UsersResource usersResource = realmResource.users();

      // Ensure realm is configured with the correct client scopes and mappers. For now,
      // we call this every time a new application is created, even if the realm is already
      // configured. It is an idempotent operation.
      clientScopeConfigurator.configureClientScope(realmResource);

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

      try (var response = realmResource.clients().create(clientRepresentation)) {
        if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
          throw new BadRequestException("Unable to create Application");
        }
      }

      final var client = realmResource
          .clients()
          .findByClientId(clientRepresentation.getClientId())
          .getFirst();

      final var serviceAccountUser = clientsResource
          .get(client.getId())
          .getServiceAccountUser();

      serviceAccountUser.setAttributes(
          Map.of(
              USER_ID, List.of(String.valueOf(user.getAuthUserId())),
              CLIENT_ID, List.of(client.getClientId())));

      usersResource
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
        .realm(getCurrentUserRealmName())
        .users()
        .searchByAttributes(USER_ID + ":" + user.getAuthUserId());

    final var existingClient = new ArrayList<ClientRepresentation>();
    for (final var clientUser : clientUsers) {
      final var client = keycloakAdminClient
          .realm(getCurrentUserRealmName())
          .clients()
          .findByClientId(clientUser
              .getAttributes()
              .get(CLIENT_ID)
              .getFirst())
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
    final var userRealm = getCurrentUserRealmName();
    final var client = keycloakAdminClient
        .realm(getCurrentUserRealmName())
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
        .realm(userRealm)
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
    final var userRealm = getCurrentUserRealmName();
    final var keycloakClient = KeycloakBuilder
        .builder()
        .serverUrl(keycloakConfiguration.getServerUrl())
        .realm(userRealm)
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
    attributes.put("access.token.signed.response.alg", "RS256");
    // Note: No matter the configured value, this is limited to keycloak's Realm settings -> sessions ->
    // SSO Session Max
    attributes.put("access.token.lifespan", String.valueOf(accessTokenExpirationTime.getSeconds()));
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

  private String getCurrentUserRealmName() {
    return userAuthenticationResolver.resolveSsoRealm().orElseThrow(
        () -> new BadRequestException("Could not determine realm for current user"));
  }

}
