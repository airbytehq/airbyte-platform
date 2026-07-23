/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Factory
class OidcConfigFactory {
  /**
   * Returns the OidcConfig with values from the environment, if present. This is the preferred way
   * to configure the oidc identity provider and should take precedence over `airbyte.yml`.
   */
  @Singleton
  @Requires(property = "airbyte.auth.identity-provider.type", value = "oidc")
  fun defaultOidcConfig(airbyteAuthConfig: AirbyteAuthConfig): OidcConfig {
    if (airbyteAuthConfig.identityProvider.oidc.domain
        .isEmpty() ||
      airbyteAuthConfig.identityProvider.oidc.appName
        .isEmpty() ||
      airbyteAuthConfig.identityProvider.oidc.clientId
        .isEmpty() ||
      airbyteAuthConfig.identityProvider.oidc.clientSecret
        .isEmpty()
    ) {
      throw IllegalStateException(
        "Missing required OIDC configuration. Please ensure all of the following properties are set: " +
          "airbyte.auth.identity-provider.oidc.domain, " +
          "airbyte.auth.identity-provider.oidc.app-name, " +
          "airbyte.auth.identity-provider.oidc.client-id, " +
          "airbyte.auth.identity-provider.oidc.client-secret",
      )
    }

    val displayName =
      airbyteAuthConfig.identityProvider.oidc.displayName
        .ifBlank { airbyteAuthConfig.identityProvider.oidc.appName }

    return OidcConfig(
      domain = airbyteAuthConfig.identityProvider.oidc.domain,
      appName = airbyteAuthConfig.identityProvider.oidc.appName,
      displayName = displayName,
      clientId = airbyteAuthConfig.identityProvider.oidc.clientId,
      clientSecret = airbyteAuthConfig.identityProvider.oidc.clientSecret,
    )
  }
}

data class OidcConfig(
  var domain: String,
  var appName: String,
  var displayName: String,
  var clientId: String,
  var clientSecret: String,
)
