/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.keycloak.admin.client.Keycloak
import org.keycloak.representations.idm.ClientRepresentation
import org.keycloak.representations.idm.IdentityProviderRepresentation
import org.keycloak.representations.idm.RealmRepresentation
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
class AirbyteKeycloakClient(
  private val keycloakAdminClientProvider: AirbyteKeycloakAdminClientProvider,
  @Value("\${airbyte.airbyte-url}") private val airbyteUrl: String,
) {
  private val keycloakAdminClient: Keycloak = keycloakAdminClientProvider.createKeycloakAdminClient()

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
    val idp =
      IdentityProviderRepresentation().apply {
        alias = DEFAULT_IDP_ALIAS
        displayName = "${request.companyIdentifier} OIDC Login"
        providerId = "oidc"
        config =
          mapOf(
            "clientId" to request.clientId,
            "clientSecret" to request.clientSecret,
            "useJwksUrl" to "true",
            "jwksUrl" to idpDiscoveryResult["jwksUrl"],
            "issuer" to idpDiscoveryResult["issuer"],
            "authorizationUrl" to idpDiscoveryResult["authorizationUrl"],
            "logoutUrl" to idpDiscoveryResult["logoutUrl"],
            "tokenUrl" to idpDiscoveryResult["tokenUrl"],
            "metadataDescriptorUrl" to idpDiscoveryResult["metadataDescriptorUrl"],
            "userInfoUrl" to idpDiscoveryResult["userInfoUrl"],
            "validateSignature" to idpDiscoveryResult["validateSignature"],
            "defaultScope" to DEFAULT_SCOPE,
            "clientAuthMethod" to CLIENT_AUTH_METHOD,
            "pkceEnabled" to "false",
            "clientAssertionSigningAlg" to "",
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
    createClient(request.companyIdentifier, airbyteWebappClient)
  }

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

  fun importIdpConfig(
    realmName: String,
    discoveryUrl: String,
  ): Map<String, String> {
    try {
      return keycloakAdminClient
        .realms()
        .realm(realmName)
        .identityProviders()
        .importFrom(
          mapOf(
            "fromUrl" to discoveryUrl,
            "providerId" to "oidc",
          ),
        )
    } catch (e: Exception) {
      logger.error(e) { "Import SSO config request failed" }
      throw ImportConfigException("Import SSO config request failed! Server error: $e")
    }
  }

  fun createIdpForRealm(
    realmName: String,
    idp: IdentityProviderRepresentation,
  ) {
    try {
      keycloakAdminClient
        .realms()
        .realm(realmName)
        .identityProviders()
        .create(idp)
    } catch (e: Exception) {
      logger.error(e) { "Create IDP request failed" }
      throw IdpCreationException("Create IDP request failed! Server error: $e")
    }
  }

  fun createClient(
    realmName: String,
    client: ClientRepresentation,
  ) {
    try {
      keycloakAdminClient
        .realms()
        .realm(realmName)
        .clients()
        .create(client)
    } catch (e: Exception) {
      logger.error(e) { "Create client request failed" }
      throw CreateClientException("Create client request failed! Server error: $e")
    }
  }

  fun deleteRealm(realmName: String) {
    keycloakAdminClient
      .realms()
      .realm(realmName)
      .remove()
  }

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
