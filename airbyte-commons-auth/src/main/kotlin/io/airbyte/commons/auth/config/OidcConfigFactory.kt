/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

@Factory
class OidcConfigFactory {
  /**
   * Returns the OidcConfig with values from the environment, if present. This is the preferred way
   * to configure the oidc identity provider and should take precedence over `airbyte.yml`.
   */
  @Singleton
  @Requires(property = "airbyte.auth.identity-provider.type", value = "oidc")
  fun defaultOidcConfig(
    @Value("\${airbyte.auth.identity-provider.oidc.domain}") domain: String?,
    @Value("\${airbyte.auth.identity-provider.oidc.app-name}") appName: String?,
    @Value("\${airbyte.auth.identity-provider.oidc.display-name}") displayName: String?,
    @Value("\${airbyte.auth.identity-provider.oidc.client-id}") clientId: String?,
    @Value("\${airbyte.auth.identity-provider.oidc.client-secret}") clientSecret: String?,
  ): OidcConfig {
    if (domain.isNullOrEmpty() || appName.isNullOrEmpty() || clientId.isNullOrEmpty() || clientSecret.isNullOrEmpty()) {
      throw IllegalStateException(
        "Missing required OIDC configuration. Please ensure all of the following properties are set: " +
          "airbyte.auth.identity-provider.oidc.domain, " +
          "airbyte.auth.identity-provider.oidc.app-name, " +
          "airbyte.auth.identity-provider.oidc.client-id, " +
          "airbyte.auth.identity-provider.oidc.client-secret",
      )
    }

    val displayName = displayName ?: appName

    return OidcConfig(domain, appName, displayName, clientId, clientSecret)
  }
}

data class OidcConfig(
  var domain: String,
  var appName: String,
  var displayName: String,
  var clientId: String,
  var clientSecret: String,
)
