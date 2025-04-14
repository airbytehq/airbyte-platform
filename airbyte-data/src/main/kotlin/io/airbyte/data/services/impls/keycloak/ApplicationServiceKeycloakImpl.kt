/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.commons.auth.RequiresAuthMode
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator
import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.services.ApplicationService
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import jakarta.ws.rs.BadRequestException
import jakarta.ws.rs.core.Response
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.KeycloakBuilder
import org.keycloak.representations.idm.ClientRepresentation
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID

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
  private val keycloakConfiguration: AirbyteKeycloakConfiguration,
  private val clientScopeConfigurator: ClientScopeConfigurator,
  private val tokenExpirationConfig: TokenExpirationConfig,
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
  ): Application {
    val realmResource =
      keycloakAdminClient.realm(keycloakConfiguration.clientRealm)
        ?: throw BadRequestException("Could not retrieve a realm for ${keycloakConfiguration.clientRealm}")
    val clientsResource = realmResource.clients() ?: throw BadRequestException("No clients found for ${keycloakConfiguration.clientRealm}")
    val usersResource = realmResource.users() ?: throw BadRequestException("No users found for ${keycloakConfiguration.clientRealm}")

    // Ensure realm is configured with the correct client scopes and mappers. For now,
    // we call this every time a new application is created, even if the realm is already
    // configured. It is an idempotent operation.
    clientScopeConfigurator.configureClientScope(realmResource)

    val existingClients = listApplicationsByUser(user)
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

    return toApplication(client)
  }

  /**
   * List all Applications for a user.
   *
   * @param user The user to list Applications for.
   * @return The list of Applications for the user.
   */
  override fun listApplicationsByUser(user: AuthenticatedUser): List<Application> {
    val clientRealm = keycloakConfiguration.clientRealm
    val clientUsers =
      keycloakAdminClient
        .realm(clientRealm)
        .users()
        .searchByAttributes(USER_ID + ":" + user.authUserId)

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
  ): Application {
    val clientRealm = keycloakConfiguration.clientRealm
    val client =
      keycloakAdminClient
        .realm(clientRealm)
        .clients()
        .findByClientId(applicationId)
        .first()

    val userApplications = listApplicationsByUser(user)

    // Only allow the user to delete their own Applications.
    if (userApplications.none { application: Application -> application.clientId == applicationId }) {
      throw BadRequestException("You do not have permission to delete this Application")
    }

    keycloakAdminClient
      .realm(clientRealm)
      .clients()[client.id]
      .remove()

    return toApplication(client)
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
  ): String =
    KeycloakBuilder
      .builder()
      .serverUrl(keycloakConfiguration.getServerUrl())
      .realm(keycloakConfiguration.clientRealm)
      .grantType("client_credentials")
      .clientId(clientId)
      .clientSecret(clientSecret)
      .build()
      .use {
        return it
          .tokenManager()
          .accessTokenString
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
          "access.token.lifespan" to (tokenExpirationConfig.applicationTokenExpirationInMinutes * 60).toString(),
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
