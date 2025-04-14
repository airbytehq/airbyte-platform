/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.airbyte.config.Configs
import io.airbyte.config.Configs.AirbyteEdition
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton

/**
 * Data class representing the AuthConfigs for an Airbyte instance. This includes the [AuthMode] and
 * optional sub-configurations like [OidcConfig] and [AirbyteKeycloakConfiguration].
 */
data class AuthConfigs(
  val authMode: AuthMode,
  val keycloakConfig: AirbyteKeycloakConfiguration? = null,
  val oidcConfig: OidcConfig? = null,
  val initialUserConfig: InitialUserConfig? = null,
)

/**
 * Enum representing the different authentication modes that Airbyte can be configured to use.
 * Note that `SIMPLE` refers to the single-user username/password authentication mode that Community
 * edition uses, while `OIDC` refers to the OpenID Connect authentication mode that Enterprise and
 * Cloud use. `NONE` is used when authentication is disabled completely.
 */
enum class AuthMode {
  OIDC,
  SIMPLE,
  NONE,
}

const val SIMPLE = "simple"

@Factory
class AuthModeFactory(
  private val airbyteEdition: Configs.AirbyteEdition,
  @Property(name = "micronaut.security.enabled")
  private val securityEnabled: Boolean = false,
  @Property(name = "airbyte.auth.identity-provider.type")
  private val identityProviderType: String? = null,
) {
  /**
   * The default auth mode is determined by the deployment mode and edition.
   */
  @Singleton
  fun defaultAuthMode(): AuthMode =
    when (airbyteEdition) {
      AirbyteEdition.CLOUD -> AuthMode.OIDC
      AirbyteEdition.ENTERPRISE -> {
        when (identityProviderType) {
          SIMPLE -> AuthMode.SIMPLE
          else -> AuthMode.OIDC
        }
      }
      AirbyteEdition.COMMUNITY -> {
        if (securityEnabled) AuthMode.SIMPLE else AuthMode.NONE
      }
      else -> throw IllegalStateException("Unknown or unspecified Airbyte edition: $airbyteEdition")
    }
}

/**
 * This factory provides an Application's AuthConfigs based on the deployment mode and edition.
 * It includes optional sub-configurations, like the [OidcConfig] and [AirbyteKeycloakConfiguration]
 * for convenience, even though those configurations are managed by their own factories and can be
 * injected independently.
 */
@Factory
class AuthConfigFactory(
  private val authMode: AuthMode,
  private val keycloakConfig: AirbyteKeycloakConfiguration? = null,
  private val oidcConfig: OidcConfig? = null,
  private val initialUserConfig: InitialUserConfig? = null,
) {
  @Singleton
  fun authConfig(): AuthConfigs = AuthConfigs(authMode, keycloakConfig, oidcConfig, initialUserConfig)
}
