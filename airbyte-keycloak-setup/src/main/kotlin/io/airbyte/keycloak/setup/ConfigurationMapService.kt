/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.airbyte.commons.auth.config.OidcConfig
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.keycloak.admin.client.resource.RealmResource

/**
 * This class provides services for managing configuration maps. It includes methods for adding,
 * removing, and updating configuration settings.
 */
@Singleton
class ConfigurationMapService(
  @param:Named("airbyteUrl") private val airbyteUrl: String,
  private val keycloakConfiguration: AirbyteKeycloakConfiguration,
) {
  fun importProviderFrom(
    keycloakRealm: RealmResource,
    oidcConfig: OidcConfig,
    keycloakProviderId: String,
  ): Map<String, String> {
    val map: MutableMap<String, Any> = HashMap()
    map["providerId"] = keycloakProviderId
    map["fromUrl"] = getProviderDiscoveryUrl(oidcConfig)
    return keycloakRealm.identityProviders().importFrom(map)
  }

  fun setupProviderConfig(
    oidcConfig: OidcConfig,
    configMap: Map<String, String>,
  ): MutableMap<String, String> {
    // Copy all keys from configMap to the result map
    val config: MutableMap<String, String> = HashMap(configMap)

    // Explicitly set required keys
    config["clientId"] = oidcConfig.clientId
    config["clientSecret"] = oidcConfig.clientSecret
    config["defaultScope"] = "openid email profile"
    config["redirectUris"] = getProviderRedirectUrl(oidcConfig)
    config["backchannelSupported"] = "true"
    config["backchannel_logout_session_supported"] = "true"

    return config
  }

  private fun getProviderRedirectUrl(oidcConfig: OidcConfig): String {
    val airbyteUrlWithTrailingSlash = if (airbyteUrl.endsWith("/")) airbyteUrl else "$airbyteUrl/"
    return (
      airbyteUrlWithTrailingSlash + "auth/realms/" + keycloakConfiguration.airbyteRealm + "/broker/" + oidcConfig.appName +
        "/endpoint"
    )
  }

  private fun getProviderDiscoveryUrl(oidcConfig: OidcConfig): String {
    var domain = oidcConfig.domain
    if (!domain.startsWith(HTTPS_PREFIX)) {
      domain = HTTPS_PREFIX + domain
    }
    if (!domain.endsWith(WELL_KNOWN_OPENID_CONFIGURATION_SUFFIX)) {
      domain = if (domain.endsWith("/")) domain else "$domain/"
      domain = domain + WELL_KNOWN_OPENID_CONFIGURATION_SUFFIX
    }
    return domain
  }

  companion object {
    const val HTTPS_PREFIX: String = "https://"
    const val WELL_KNOWN_OPENID_CONFIGURATION_SUFFIX: String = ".well-known/openid-configuration"
  }
}
