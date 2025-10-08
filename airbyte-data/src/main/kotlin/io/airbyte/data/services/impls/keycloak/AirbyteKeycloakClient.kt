/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_SSO_REALM
import io.airbyte.commons.auth.support.JwtTokenParser.tokenToAttributes
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.common.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import jakarta.ws.rs.core.Response
import okhttp3.OkHttpClient
import okhttp3.Request
import org.apache.http.HttpHeaders
import org.keycloak.admin.client.Keycloak
import org.keycloak.representations.idm.ClientRepresentation
import org.keycloak.representations.idm.IdentityProviderRepresentation
import org.keycloak.representations.idm.RealmRepresentation
import java.util.UUID

private val logger = KotlinLogging.logger { "airbyte-keycloak" }

@Singleton
class AirbyteKeycloakClient(
  keycloakAdminClientProvider: AirbyteKeycloakAdminClientProvider,
  private val airbyteConfig: AirbyteConfig,
  private val keycloakConfiguration: AirbyteKeycloakConfig,
  @Named("keycloakHttpClient") private val httpClient: OkHttpClient,
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
   * If any step fails after the realm is created, the realm is deleted before throwing the exception.
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

    try {
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
          redirectUris = listOf("${airbyteConfig.airbyteUrl}/*")
          webOrigins = listOf(airbyteConfig.airbyteUrl)
          baseUrl = airbyteConfig.airbyteUrl
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
    } catch (e: Exception) {
      try {
        deleteRealm(request.companyIdentifier)
      } catch (cleanupEx: Exception) {
        logger.error(cleanupEx) { "Failed to cleanup Keycloak realm ${request.companyIdentifier} after configuration failure" }
      }
      throw e
    }
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
   * Checks if a Keycloak realm exists with the given name.
   * @return true if the realm exists, false otherwise.
   */
  fun realmExists(realmName: String): Boolean =
    try {
      keycloakAdminClient.realms().realm(realmName).toRepresentation()
      true
    } catch (e: Exception) {
      logger.debug(e) { "Realm $realmName does not exist or is not accessible" }
      false
    }

  /**
   * Deletes a Keycloak realm by name.
   * Removes all associated configurations, clients, and identity providers.
   * @throws RealmDeletionException if deletion fails.
   */
  fun deleteRealm(realmName: String) {
    try {
      keycloakAdminClient
        .realms()
        .realm(realmName)
        .remove()
    } catch (e: Exception) {
      logger.error(e) { "Delete realm request failed" }
      throw RealmDeletionException("Delete realm request failed! Server error: $e")
    }
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

  /**
   * Deletes the default identity provider from a realm.
   * This removes the IDP configuration but preserves the realm and any existing users.
   * Idempotent - if the IDP doesn't exist, this is considered a success.
   * @throws IdpDeletionException if deletion fails.
   */
  fun deleteIdpFromRealm(realmName: String) {
    try {
      val realm = keycloakAdminClient.realms().realm(realmName)
      val idpExists = realm.identityProviders().findAll().any { it.alias == DEFAULT_IDP_ALIAS }

      if (!idpExists) {
        logger.info { "IDP $DEFAULT_IDP_ALIAS does not exist in realm $realmName, nothing to delete" }
        return
      }

      realm.identityProviders().get(DEFAULT_IDP_ALIAS).remove()
      logger.info { "Successfully deleted IDP $DEFAULT_IDP_ALIAS from realm $realmName" }
    } catch (e: Exception) {
      logger.error(e) { "Delete IDP request failed" }
      throw IdpDeletionException("Delete IDP request failed! Server error: $e")
    }
  }

  /**
   * Updates the complete IDP configuration for an existing realm.
   * If an IDP exists, updates it with new configuration to preserve user links.
   * If no IDP exists, creates a new one.
   * This preserves the realm and any existing users while updating the SSO settings.
   * @throws IdpDeletionException, ImportConfigException, or IdpCreationException on failures.
   */
  fun replaceOidcIdpConfig(ssoConfig: SsoConfig) {
    val realm = keycloakAdminClient.realms().realm(ssoConfig.companyIdentifier)
    val existingIdp =
      realm
        .identityProviders()
        .findAll()
        .filter { it.alias == DEFAULT_IDP_ALIAS }
        .getOrNull(0)

    val idpDiscoveryResult = importIdpConfig(ssoConfig.companyIdentifier, ssoConfig.discoveryUrl)
    val idpConfig =
      mapOf(
        "clientId" to ssoConfig.clientId,
        "clientSecret" to ssoConfig.clientSecret,
        "authorizationUrl" to idpDiscoveryResult["authorizationUrl"],
        "tokenUrl" to idpDiscoveryResult["tokenUrl"],
        "clientAuthMethod" to CLIENT_AUTH_METHOD,
        "defaultScope" to DEFAULT_SCOPE,
      )

    if (existingIdp != null) {
      // Update existing IDP to preserve user links
      existingIdp.config = idpConfig
      realm.identityProviders().get(DEFAULT_IDP_ALIAS).update(existingIdp)
      logger.info { "Updated existing IDP $DEFAULT_IDP_ALIAS in realm ${ssoConfig.companyIdentifier}" }
    } else {
      // Create new IDP
      val idp =
        IdentityProviderRepresentation().apply {
          alias = DEFAULT_IDP_ALIAS
          providerId = "oidc"
          config = idpConfig
        }
      createIdpForRealm(ssoConfig.companyIdentifier, idp)
      logger.info { "Created new IDP $DEFAULT_IDP_ALIAS in realm ${ssoConfig.companyIdentifier}" }
    }
  }

  /**
   * Validates a token by extracting its realm and calling the userinfo endpoint.
   * @param token The JWT token to validate
   * @throws InvalidTokenException if the token has no realm claim
   * @throws TokenExpiredException if the token is expired or invalid
   * @throws MalformedTokenResponseException if the response is malformed
   * @throws KeycloakServiceException if there's an error communicating with Keycloak
   */
  fun validateToken(token: String) {
    val realm =
      extractRealmFromToken(token)
        ?: throw InvalidTokenException("Token does not contain a realm claim")
    validateTokenWithRealm(token, realm)
  }

  /**
   * Validates a token against a specific Keycloak realm.
   * @param token The JWT token to validate
   * @param realm The Keycloak realm to validate against
   * @throws TokenExpiredException if the token is expired or invalid (401 response)
   * @throws InvalidTokenException if the token validation failed (non-200 response)
   * @throws MalformedTokenResponseException if the response is malformed or missing required claims
   * @throws KeycloakServiceException if there's an error communicating with Keycloak
   */
  fun validateTokenWithRealm(
    token: String,
    realm: String,
  ) {
    val userInfoEndpoint = keycloakConfiguration.getKeycloakUserInfoEndpointForRealm(realm)
    logger.debug { "Validating token with Keycloak userinfo endpoint: $userInfoEndpoint" }

    val request =
      Request
        .Builder()
        .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        .addHeader(HttpHeaders.AUTHORIZATION, "Bearer $token")
        .url(userInfoEndpoint)
        .get()
        .build()

    try {
      httpClient.newCall(request).execute().use { response ->
        when {
          response.code == 401 -> {
            logger.debug { "Token is invalid or expired (401 response)" }
            throw TokenExpiredException("Token is invalid or expired")
          }
          !response.isSuccessful -> {
            logger.debug { "Non-200 response from userinfo endpoint: ${response.code}" }
            throw InvalidTokenException("Token validation failed with status ${response.code}")
          }
          else -> {
            val responseBody = response.body?.string()
            if (responseBody.isNullOrEmpty()) {
              logger.debug { "Received null or empty userinfo response" }
              throw MalformedTokenResponseException("Empty response from Keycloak userinfo endpoint")
            }
            logger.debug { "Received userinfo response (${responseBody.length} bytes)" }
            if (!isValidUserInfoResponse(responseBody)) {
              throw MalformedTokenResponseException("Token response missing required 'sub' claim")
            }
          }
        }
      }
    } catch (e: TokenValidationException) {
      // Re-throw our specific exceptions
      throw e
    } catch (e: Exception) {
      logger.error(e) { "Failed to validate access token" }
      throw KeycloakServiceException("Failed to communicate with Keycloak", e)
    }
  }

  /**
   * Extracts the Keycloak realm from a JWT token.
   * @param token The JWT token
   * @return The realm string, or null if extraction fails
   */
  fun extractRealmFromToken(token: String): String? =
    try {
      val jwtAttributes = tokenToAttributes(token)
      val realm = jwtAttributes[JWT_SSO_REALM] as String?
      logger.debug { "Extracted realm $realm" }
      realm
    } catch (e: Exception) {
      logger.debug(e) { "Failed to parse realm from JWT token" }
      null
    }

  /**
   * Validates the userinfo response from Keycloak.
   * @param responseBody The JSON response from the userinfo endpoint
   * @return true if the response contains a valid subject, false otherwise
   */
  private fun isValidUserInfoResponse(responseBody: String): Boolean =
    try {
      val userInfo = objectMapper.readTree(responseBody)
      val sub = userInfo.path("sub").asText()
      logger.debug { "Validated Keycloak user subject claim" }
      StringUtils.isNotBlank(sub)
    } catch (e: JsonProcessingException) {
      logger.error(e) { "Failed to process JSON." }
      false
    }

  companion object {
    private const val AIRBYTE_LOGIN_THEME = "airbyte-keycloak-theme"
    private const val DEFAULT_SCOPE = "openid profile email"
    private const val DEFAULT_IDP_ALIAS = "default"
    private const val CLIENT_AUTH_METHOD = "client_secret_post"
    private const val AIRBYTE_WEBAPP_CLIENT_ID = "airbyte-webapp"
    private const val AIRBYTE_WEBAPP_CLIENT_NAME = "Airbyte Webapp"

    private val objectMapper = ObjectMapper()
  }
}

class RealmCreationException(
  message: String,
) : Exception(message)

class RealmDeletionException(
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

class IdpDeletionException(
  message: String,
) : Exception(message)

open class TokenValidationException(
  message: String,
  cause: Throwable? = null,
) : RuntimeException(message, cause)

class InvalidTokenException(
  message: String,
) : TokenValidationException(message)

class TokenExpiredException(
  message: String,
) : TokenValidationException(message)

class KeycloakServiceException(
  message: String,
  cause: Throwable,
) : TokenValidationException(message, cause)

class MalformedTokenResponseException(
  message: String,
) : TokenValidationException(message)
