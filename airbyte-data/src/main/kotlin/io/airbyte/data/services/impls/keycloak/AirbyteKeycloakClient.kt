/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import jakarta.ws.rs.core.Response
import org.keycloak.admin.client.Keycloak
import org.keycloak.representations.idm.ClientRepresentation
import org.keycloak.representations.idm.IdentityProviderRepresentation
import org.keycloak.representations.idm.RealmRepresentation
import java.util.UUID

private val logger = KotlinLogging.logger { "airbyte-keycloak" }

@Singleton
class AirbyteKeycloakClient(
  keycloakAdminClientProvider: AirbyteKeycloakAdminClientProvider,
  @Value("\${airbyte.airbyte-url}") private val airbyteUrl: String,
) {
  private val keycloakAdminClient: Keycloak = keycloakAdminClientProvider.createKeycloakAdminClient()

  /**
   * Retrieves SSO configuration data for a specific organization and realm.
   * Finds the default identity provider and extracts client credentials from it.
   * @throws IdpNotFoundException if the default identity provider does not exist in the realm.
   */
  fun getSsoConfigData(
    organizationId: UUID,
    realmName: String,
  ): SsoKeycloakIdpCredentials {
    val currentRealm = keycloakAdminClient.realms().realm(realmName)
    val currentIdpRepresentation =
      currentRealm
        .identityProviders()
        .findAll()
        .filter { it.alias == DEFAULT_IDP_ALIAS }
        .getOrNull(0)

    if (currentIdpRepresentation == null) {
      throw IdpNotFoundException("IDP $DEFAULT_IDP_ALIAS does not exist")
    }

    return SsoKeycloakIdpCredentials(
      organizationId = organizationId,
      clientId = currentIdpRepresentation.config["clientId"].toString(),
      clientSecret = currentIdpRepresentation.config["clientSecret"].toString(),
    )
  }

  /**
   * Creates a complete OIDC SSO configuration including realm, identity provider, and client.
   * Sets up the full authentication flow for an organization's SSO integration.
   * @throws RealmCreationException, IdpCreationException, CreateClientException, or ImportConfigException on failures.
   */
  fun createOidcSsoConfig(request: SsoConfig) {
    createRealm(
      RealmRepresentation().apply {
        id = request.companyIdentifier
        realm = request.companyIdentifier
        displayName = request.companyIdentifier
        displayNameHtml = "<b>${request.companyIdentifier}</b>"
        isEnabled = true
        accessCodeLifespan = 3
        loginTheme = AIRBYTE_LOGIN_THEME
      },
    )

    val idpDiscoveryResult = importIdpConfig(request.companyIdentifier, request.discoveryUrl)

    // see https://www.keycloak.org/docs/latest/server_admin/index.html#_identity_broker_oidc
    // and https://www.keycloak.org/docs-api/latest/rest-api/index.html#IdentityProviderRepresentation
    // The idpDiscovery result contains more fields than we pass in the following config, however we only require
    // the auth url and the token url to enable sso.
    val idp =
      IdentityProviderRepresentation().apply {
        alias = DEFAULT_IDP_ALIAS
        providerId = "oidc"
        config =
          mapOf(
            "clientId" to request.clientId,
            "clientSecret" to request.clientSecret,
            "authorizationUrl" to idpDiscoveryResult["authorizationUrl"],
            "tokenUrl" to idpDiscoveryResult["tokenUrl"],
            "clientAuthMethod" to CLIENT_AUTH_METHOD,
            "defaultScope" to DEFAULT_SCOPE,
          )
      }
    createIdpForRealm(request.companyIdentifier, idp)

    val airbyteWebappClient =
      ClientRepresentation().apply {
        clientId = AIRBYTE_WEBAPP_CLIENT_ID
        name = AIRBYTE_WEBAPP_CLIENT_NAME
        protocol = "openid-connect"
        redirectUris = listOf("$airbyteUrl/*")
        webOrigins = listOf(airbyteUrl)
        baseUrl = airbyteUrl
        isEnabled = true
        isDirectAccessGrantsEnabled = false
        isStandardFlowEnabled = true
        isServiceAccountsEnabled = false
        authorizationServicesEnabled = false
        isFrontchannelLogout = false
        isImplicitFlowEnabled = false
        isPublicClient = true
      }
    createClientForRealm(request.companyIdentifier, airbyteWebappClient)
  }

  /**
   * Creates a new Keycloak realm with the provided configuration.
   * Handles creation errors and provides meaningful exception messages.
   * @throws RealmValuesExistException if realm values already exist, RealmCreationException for other failures.
   */
  fun createRealm(realm: RealmRepresentation) {
    try {
      keycloakAdminClient.realms().create(realm)
    } catch (e: Exception) {
      logger.error(e) { "Create SSO config request failed" }
      // Keycloak doesn't give us good error messages regarding failures. If the realm name already exists,
      // we get nothing but a 400 Bad Request here, so we attempt to convert it into something else so we can
      // at least return a 400 to the user instead of a 500.
      if (e.message?.lowercase()?.contains("bad request") == true) {
        throw RealmValuesExistException("Issue creating realm: some values already exist in realm config: $e")
      }

      throw RealmCreationException("Create SSO config request failed! Server error: $e")
    }
  }

  /**
   * Imports identity provider configuration from an OIDC discovery URL.
   * Returns the imported configuration map containing auth and token URLs.
   * @throws ImportConfigException if the discovery URL is invalid or import fails.
   */
  fun importIdpConfig(
    realmName: String,
    discoveryUrl: String,
  ): Map<String, String> {
    try {
      val importedIdpConfig =
        keycloakAdminClient
          .realms()
          .realm(realmName)
          .identityProviders()
          .importFrom(
            mapOf(
              "fromUrl" to discoveryUrl,
              "providerId" to "oidc",
            ),
          )
      logger.info { "Imported IDP config: $importedIdpConfig" }
      return importedIdpConfig
    } catch (e: Exception) {
      logger.error(e) { "Import SSO config request failed" }
      throw ImportConfigException("Import SSO config request failed! Server error: $e")
    }
  }

  /**
   * Creates an identity provider within a specific realm.
   * Validates the response status and throws exceptions on failure.
   * @throws IdpCreationException if creation fails or returns non-successful status.
   */
  fun createIdpForRealm(
    realmName: String,
    idp: IdentityProviderRepresentation,
  ) {
    try {
      val response =
        keycloakAdminClient
          .realms()
          .realm(realmName)
          .identityProviders()
          .create(idp)

      if (response.statusInfo.family != Response.Status.Family.SUCCESSFUL) {
        logger.info { "Created IDP response status entity: ${response.readEntity(String::class.java)}" }
        throw IdpCreationException("Create IDP request failed with ${response.status} response")
      }
    } catch (e: Exception) {
      logger.error(e) { "Create IDP request failed" }
      throw IdpCreationException("Create IDP request failed! Server error: $e")
    }
  }

  /**
   * Creates a client application within a specific realm.
   * Handles response validation and provides error handling for failed requests.
   * @throws CreateClientException if creation fails or returns non-successful status.
   */
  fun createClientForRealm(
    realmName: String,
    client: ClientRepresentation,
  ) {
    try {
      val response =
        keycloakAdminClient
          .realms()
          .realm(realmName)
          .clients()
          .create(client)

      if (response.statusInfo.family != Response.Status.Family.SUCCESSFUL) {
        logger.info { "Created client response status entity: ${response.readEntity(String::class.java)}" }
        throw CreateClientException("Create client request failed with ${response.status} response")
      }
    } catch (e: Exception) {
      logger.error(e) { "Create client request failed" }
      throw CreateClientException("Create client request failed! Server error: $e")
    }
  }

  /**
   * Deletes a Keycloak realm by name.
   * Removes all associated configurations, clients, and identity providers.
   * This method does not throw custom exceptions but may throw Keycloak client exceptions.
   */
  fun deleteRealm(realmName: String) {
    keycloakAdminClient
      .realms()
      .realm(realmName)
      .remove()
  }

  /**
   * Updates client credentials for an existing identity provider.
   * Finds the default IDP in the realm and updates its client ID and secret.
   * @throws IdpNotFoundException if the default identity provider does not exist in the realm.
   */
  fun updateIdpClientCredentials(
    clientConfig: SsoKeycloakIdpCredentials,
    realmName: String,
  ) {
    val currentRealm = keycloakAdminClient.realms().realm(realmName)
    val currentIdpRepresentation =
      currentRealm
        .identityProviders()
        .findAll()
        .filter { it.alias == DEFAULT_IDP_ALIAS }
        .getOrNull(0)

    if (currentIdpRepresentation == null) {
      throw IdpNotFoundException("IDP $DEFAULT_IDP_ALIAS does not exist")
    }

    currentIdpRepresentation.config["clientId"] = clientConfig.clientId
    currentIdpRepresentation.config["clientSecret"] = clientConfig.clientSecret

    currentRealm
      .identityProviders()
      .get(DEFAULT_IDP_ALIAS)
      .update(currentIdpRepresentation)
  }

  companion object {
    private const val AIRBYTE_LOGIN_THEME = "airbyte-keycloak-theme"
    private const val DEFAULT_SCOPE = "openid profile email"
    private const val DEFAULT_IDP_ALIAS = "default"
    private const val CLIENT_AUTH_METHOD = "client_secret_post"
    private const val AIRBYTE_WEBAPP_CLIENT_ID = "airbyte-webapp"
    private const val AIRBYTE_WEBAPP_CLIENT_NAME = "Airbyte Webapp"
  }
}

class RealmCreationException(
  message: String,
) : Exception(message)

class RealmValuesExistException(
  message: String,
) : Exception(message)

class IdpCreationException(
  message: String,
) : Exception(message)

class ImportConfigException(
  message: String,
) : Exception(message)

class CreateClientException(
  message: String,
) : Exception(message)

class IdpNotFoundException(
  message: String,
) : Exception(message)
