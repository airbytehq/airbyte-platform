/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.OidcConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import jakarta.ws.rs.core.Response
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.representations.idm.IdentityProviderRepresentation
import java.util.Optional

private val log = KotlinLogging.logger {}

/**
 * This class is responsible for configuring an identity provider. It creates and manages various
 * identity providers for authentication purposes.
 */
@Singleton
class IdentityProvidersConfigurator(
  private val configurationMapService: ConfigurationMapService,
  private val oidcConfig: Optional<OidcConfig>,
) {
  fun configureIdp(keycloakRealm: RealmResource) {
    if (oidcConfig.isEmpty) {
      log.info("No identity provider configuration found. Skipping IDP setup.")
      return
    }

    val idp = buildIdpFromConfig(keycloakRealm, oidcConfig.get())

    val existingIdps = keycloakRealm.identityProviders().findAll()
    // if no IDPs exist, create one and mark it as airbyte-managed
    if (existingIdps.isEmpty()) {
      log.info("No existing identity providers found. Creating new IDP.")
      createNewIdp(keycloakRealm, idp)
      return
    }

    // Look for an IDP with the AIRBYTE_MANAGED_IDP_KEY/VALUE in its config. This allows keycloak-setup
    // to programmatically
    // configure a specific IDP, even if the realm contains multiple.
    val existingManagedIdps =
      existingIdps
        .stream()
        .filter { existingIdp: IdentityProviderRepresentation ->
          existingIdp.config.getOrDefault(
            AIRBYTE_MANAGED_IDP_KEY,
            "false",
          ) == AIRBYTE_MANAGED_IDP_VALUE
        }.toList()

    val expNumManagedIdp = 1
    if (existingManagedIdps.size > expNumManagedIdp) {
      log.warn(
        "Found multiple IDPs with Config entry {}={}. This isn't supported, as keycloak-setup only supports one managed IDP. Skipping IDP update.",
        AIRBYTE_MANAGED_IDP_KEY,
        AIRBYTE_MANAGED_IDP_VALUE,
      )
      return
    }

    if (existingManagedIdps.size == expNumManagedIdp) {
      log.info("Found existing managed IDP. Updating it.")
      updateExistingIdp(keycloakRealm, existingManagedIdps.first(), idp)
      return
    }

    // if no managed IDPs exist, but there is exactly one IDP, update it and mark it as airbyte-managed
    if (existingIdps.size == expNumManagedIdp) {
      log.info("Found exactly one existing IDP. Updating it and marking it as airbyte-managed.")
      updateExistingIdp(keycloakRealm, existingIdps.first(), idp)
      return
    }

    // if there are multiple IDPs and none are managed, log a warning and do nothing.
    log.warn(
      "Multiple identity providers exist and none are marked as airbyte-managed. Skipping IDP update. If you want your OIDC configuration to " +
        "apply to a specific IDP, please add a Config entry with key {} and value {} to that IDP and try again.",
      AIRBYTE_MANAGED_IDP_KEY,
      AIRBYTE_MANAGED_IDP_VALUE,
    )
  }

  private fun createNewIdp(
    keycloakRealm: RealmResource,
    idp: IdentityProviderRepresentation,
  ) {
    keycloakRealm.identityProviders().create(idp).use { response ->
      if (response.status == Response.Status.CREATED.statusCode) {
        log.info("Identity Provider {} created successfully!", idp.alias)
      } else {
        val errorMessage =
          String.format(
            "Failed to create Identity Provider.\nReason: %s\nResponse: %s",
            response.statusInfo.reasonPhrase,
            response.readEntity(String::class.java),
          )
        log.error(errorMessage)
        throw RuntimeException(errorMessage)
      }
    }
  }

  private fun updateExistingIdp(
    keycloakRealm: RealmResource,
    existingIdp: IdentityProviderRepresentation,
    updatedIdp: IdentityProviderRepresentation,
  ) {
    // In order to apply the updated IDP configuration to the existing IDP within Keycloak, we need to
    // set the internal ID of the existing IDP.
    updatedIdp.internalId = existingIdp.internalId
    keycloakRealm.identityProviders()[existingIdp.alias].update(updatedIdp)
  }

  private fun buildIdpFromConfig(
    keycloakRealm: RealmResource,
    oidcConfig: OidcConfig,
  ): IdentityProviderRepresentation {
    val idp = IdentityProviderRepresentation()
    idp.displayName = oidcConfig.displayName
    idp.alias = oidcConfig.appName
    idp.providerId = KEYCLOAK_PROVIDER_ID
    idp.isEnabled = true

    val configMap = configurationMapService.importProviderFrom(keycloakRealm, oidcConfig, idp.providerId)
    val config = configurationMapService.setupProviderConfig(oidcConfig, configMap)

    // mark the IDP as airbyte-managed so that it can be programmatically updated in the future.
    config[AIRBYTE_MANAGED_IDP_KEY] = AIRBYTE_MANAGED_IDP_VALUE
    idp.config = config

    return idp
  }

  companion object {
    const val AIRBYTE_MANAGED_IDP_KEY: String = "airbyte-managed-idp"
    const val AIRBYTE_MANAGED_IDP_VALUE: String = "true"
    private const val KEYCLOAK_PROVIDER_ID = "oidc" // OIDC is the only supported provider ID for now
  }
}
