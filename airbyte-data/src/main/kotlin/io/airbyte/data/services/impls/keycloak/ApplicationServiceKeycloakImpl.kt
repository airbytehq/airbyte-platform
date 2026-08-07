/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.commons.auth.RequiresAuthMode
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator
import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.ScimAuthUserOwnershipService
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import jakarta.ws.rs.BadRequestException
import jakarta.ws.rs.NotAuthorizedException
import jakarta.ws.rs.client.ClientBuilder
import jakarta.ws.rs.core.Response
import org.keycloak.admin.client.ClientBuilderWrapper
import org.keycloak.admin.client.JacksonProvider
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.KeycloakBuilder
import org.keycloak.representations.idm.ClientRepresentation
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Application Service for Keycloak.
 *
 *
 * An Application for a user or non-user entity i.e. an organization.
 */
@Singleton
@RequiresAuthMode(AuthMode.OIDC)
class ApplicationServiceKeycloakImpl(
  private val keycloakAdminClient: Keycloak,
  private val keycloakConfiguration: AirbyteKeycloakConfig,
  private val clientScopeConfigurator: ClientScopeConfigurator,
  private val airbyteAuthConfig: AirbyteAuthConfig,
  private val authUserOwnershipService: ScimAuthUserOwnershipService,
) : ApplicationService {
  /**
   * An ID that uniquely identifies the Application in the downstream service. Is used for deletion.
   *
   * @param user The user to create the Application for.
   * @param name The name of the Application.
   * @return The created Application.
   */
  override fun createApplication(
    user: AuthenticatedUser,
    name: String,
  ): Application =
    authUserOwnershipService.withUniqueOwner(user.authUserId, user.userId) {
      val realmResource =
        keycloakAdminClient.realm(keycloakConfiguration.clientRealm)
          ?: throw BadRequestException("Could not retrieve a realm for ${keycloakConfiguration.clientRealm}")
      val clientsResource = realmResource.clients() ?: throw BadRequestException("No clients found for ${keycloakConfiguration.clientRealm}")
      val usersResource = realmResource.users() ?: throw BadRequestException("No users found for ${keycloakConfiguration.clientRealm}")

      // Ensure realm is configured with the correct client scopes and mappers. For now,
      // we call this every time a new application is created, even if the realm is already
      // configured. It is an idempotent operation.
      clientScopeConfigurator.configureClientScope(realmResource)

      val existingClients = listApplicationsByAuthUserId(user.authUserId)
      if (existingClients.size >= MAX_CREDENTIALS) {
        throw BadRequestException("User already has $MAX_CREDENTIALS Applications")
      }
      if (existingClients
          .any { clientRepresentation: Application -> clientRepresentation.name == name }
      ) {
        throw BadRequestException("User already has a key with this name")
      }
      val clientRepresentation = buildClientRepresentation(name)

      realmResource.clients().create(clientRepresentation).use { response ->
        if (response.status != Response.Status.CREATED.statusCode) {
          throw BadRequestException("Unable to create Application")
        }
      }
      val client =
        realmResource
          .clients()
          .findByClientId(clientRepresentation.clientId)
          .first()

      val serviceAccountUser =
        clientsResource[client.id]
          .serviceAccountUser

      serviceAccountUser.attributes =
        mapOf(
          USER_ID to listOf(user.authUserId.toString()),
          CLIENT_ID to listOf(client.clientId),
        )

      usersResource[serviceAccountUser.id]
        .update(serviceAccountUser)

      toApplication(client)
    }

  /**
   * List all Applications for a user.
   *
   * @param user The user to list Applications for.
   * @return The list of Applications for the user.
   */
  override fun listApplicationsByUser(user: AuthenticatedUser): List<Application> =
    authUserOwnershipService.withUniqueOwner(user.authUserId, user.userId) {
      listApplicationsByAuthUserId(user.authUserId)
    }

  private fun listApplicationsByAuthUserId(authUserId: String): List<Application> {
    val clientRealm = keycloakConfiguration.clientRealm
    val clientUsers =
      keycloakAdminClient
        .realm(clientRealm)
        .users()
        .searchByAttributes(USER_ID + ":" + authUserId)

    val existingClient = ArrayList<ClientRepresentation>()
    for (clientUser in clientUsers) {
      val client =
        keycloakAdminClient
          .realm(clientRealm)
          .clients()
          .findByClientId(
            clientUser
              .attributes[CLIENT_ID]
              ?.first(),
          ).first()

      existingClient.add(client)
    }

    return existingClient
      .map { toApplication(it) }
      .toList()
  }

  /**
   * Delete an Application for a user.
   *
   * @param applicationId The ID of the Application to delete.
   * @return The deleted Application.
   */
  override fun deleteApplication(
    user: AuthenticatedUser,
    applicationId: String,
  ): Application =
    authUserOwnershipService.withUniqueOwner(user.authUserId, user.userId) {
      val clientRealm = keycloakConfiguration.clientRealm
      val client =
        keycloakAdminClient
          .realm(clientRealm)
          .clients()
          .findByClientId(applicationId)
          .first()

      val userApplications = listApplicationsByAuthUserId(user.authUserId)

      // Only allow the user to delete their own Applications.
      if (userApplications.none { application: Application -> application.clientId == applicationId }) {
        throw BadRequestException("You do not have permission to delete this Application")
      }

      keycloakAdminClient
        .realm(clientRealm)
        .clients()[client.id]
        .remove()

      toApplication(client)
    }

  /**
   * Build a JWT for a clientId and clientSecret.
   *
   * @param clientId The clientId to build the JWT for.
   * @param clientSecret The clientSecret to build the JWT for.
   * @return The built JWT.
   */
  override fun getToken(
    clientId: String,
    clientSecret: String,
  ): String {
    val authUserId = authUserIdForClient(clientId)
    val ownerBeforeMint =
      try {
        authUserOwnershipService.uniqueOwner(authUserId)
      } catch (e: IllegalStateException) {
        throw InvalidClientCredentialsException("Invalid client_id or client_secret", e)
      }

    val clientBuilder: ClientBuilder = ClientBuilderWrapper.create(null, false)
    clientBuilder.register(JacksonProvider::class.java, 100)
    val httpClient =
      clientBuilder
        .connectTimeout(keycloakConfiguration.connectTimeout.toMillis(), TimeUnit.MILLISECONDS)
        .readTimeout(keycloakConfiguration.readTimeout.toMillis(), TimeUnit.MILLISECONDS)
        .build()
    val token =
      try {
        KeycloakBuilder
          .builder()
          .serverUrl(keycloakConfiguration.getServerUrl())
          .realm(keycloakConfiguration.clientRealm)
          .grantType("client_credentials")
          .clientId(clientId)
          .clientSecret(clientSecret)
          .resteasyClient(httpClient)
          .build()
          .use {
            it
              .tokenManager()
              .accessTokenString
          }
      } catch (e: NotAuthorizedException) {
        throw InvalidClientCredentialsException("Invalid client_id or client_secret", e)
      }

    val ownerAfterMint =
      try {
        authUserOwnershipService.uniqueOwner(authUserId)
      } catch (e: IllegalStateException) {
        throw InvalidClientCredentialsException("Invalid client_id or client_secret", e)
      }
    if (ownerAfterMint != ownerBeforeMint) {
      throw InvalidClientCredentialsException("Invalid client_id or client_secret")
    }
    return token
  }

  private fun authUserIdForClient(clientId: String): String {
    if (clientId.isBlank()) {
      throw InvalidClientCredentialsException("Invalid client_id or client_secret")
    }

    val clients = keycloakAdminClient.realm(keycloakConfiguration.clientRealm).clients()
    // Kotlin can't use named arguments against this Java interface method, so the positions are:
    // findAll(clientId, viewableOnly, search=false, first=0, max=2). search=false requires an exact
    // clientId match, not a substring match; flipping it to true would let a different client's
    // clientId resolve here (max=2 still bounds the result set, so it wouldn't reopen the prior
    // unbounded-result outage, but it could still resolve the wrong client's owner).
    val client =
      clients.findAll(clientId, false, false, 0, 2).singleOrNull()
        ?: throw InvalidClientCredentialsException("Invalid client_id or client_secret")
    return clients[client.id]
      .serviceAccountUser
      .attributes[USER_ID]
      ?.singleOrNull()
      ?: throw InvalidClientCredentialsException("Invalid client_id or client_secret")
  }

  /**
   * Build a client representation for a user.
   *
   * @param name The name of the client.
   * @return The built client representation.
   */
  private fun buildClientRepresentation(clientName: String): ClientRepresentation =
    ClientRepresentation().apply {
      clientId = UUID.randomUUID().toString()
      isServiceAccountsEnabled = true
      isStandardFlowEnabled = false
      defaultClientScopes =
        listOf(
          "web-origins",
          "acr",
          "openid",
          "profile",
          "roles",
          "email",
          "airbyte-user",
        )
      name = clientName
      attributes =
        mapOf(
          "access.token.signed.response.alg" to "RS256",
          // Note: No matter the configured value, this is limited to keycloak's Realm settings -> sessions ->
          // SSO Session Max
          "access.token.lifespan" to (airbyteAuthConfig.tokenExpiration.applicationTokenExpirationInMinutes * 60).toString(),
          "use.refresh.tokens" to "false",
        )
    }

  companion object {
    private val logger = KotlinLogging.logger {}

    // This number should be kept low or this code will start to do a lot of work.
    private const val MAX_CREDENTIALS: Int = 2
    private const val USER_ID: String = "user_id"
    private const val CLIENT_ID: String = "client_id"

    /**
     * Convert a client representation to an Application.
     *
     * @param client The client representation to convert.
     * @return The converted Application.
     */
    private fun toApplication(client: ClientRepresentation): Application =
      Application()
        .withId(client.clientId)
        .withName(client.name)
        .withClientId(client.clientId)
        .withClientSecret(client.secret)
        .withCreatedOn(
          OffsetDateTime
            .ofInstant(
              Instant.ofEpochSecond(
                client.attributes["client.secret.creation.time"]?.toLong()
                  ?: "0".toLong(),
              ),
              ZoneOffset.UTC,
            ).format(DateTimeFormatter.ISO_DATE_TIME),
        )
  }
}
