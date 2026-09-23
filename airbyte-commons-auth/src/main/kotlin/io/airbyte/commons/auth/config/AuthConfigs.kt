/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.micronaut.context.annotation.Factory
import io.micronaut.security.config.SecurityConfigurationProperties
import jakarta.inject.Singleton

/**
 * Data class representing the AuthConfigs for an Airbyte instance. This includes the [AuthMode] and
 * optional sub-configurations like [OidcConfig] and [AirbyteKeycloakConfig].
 */
data class AuthConfigs(
  val authMode: AuthMode,
  val keycloakConfig: AirbyteKeycloakConfig? = null,
  val oidcConfig: OidcConfig? = null,
  val initialUserConfig: InitialUserConfig? = null,
)

/**
 * Enum representing the different authentication modes that Airbyte can be configured to use.
 * Note that `SIMPLE` refers to the single-user username/password authentication mode that Community
 * edition uses, while `OIDC` refers to the OpenID Connect authentication mode that Cloud uses.
 * `NONE` is used when authentication is disabled completely.
 */
enum class AuthMode {
  OIDC,
  SIMPLE,
  NONE,
}

@Factory
class AuthModeFactory(
  private val airbyteConfig: AirbyteConfig,
  private val micronautSecurityConfig: SecurityConfigurationProperties?,
) {
  /**
   * The default auth mode is determined by the deployment mode and edition.
   */
  @Singleton
  fun defaultAuthMode(): AuthMode =
    when (airbyteConfig.edition) {
      AirbyteEdition.CLOUD -> AuthMode.OIDC
      AirbyteEdition.COMMUNITY -> {
        if (micronautSecurityConfig?.isEnabled == true) AuthMode.SIMPLE else AuthMode.NONE
      }
      else -> throw IllegalStateException("Unknown or unspecified Airbyte edition: ${airbyteConfig.edition}")
    }
}

/**
 * This factory provides an Application's AuthConfigs based on the deployment mode and edition.
 * It includes optional sub-configurations, like the [OidcConfig] and [AirbyteKeycloakConfig]
 * for convenience, even though those configurations are managed by their own factories and can be
 * injected independently.
 */
@Factory
class AuthConfigFactory(
  private val authMode: AuthMode,
  private val keycloakConfig: AirbyteKeycloakConfig,
  private val oidcConfig: OidcConfig? = null,
  private val initialUserConfig: InitialUserConfig? = null,
) {
  @Singleton
  fun authConfig(): AuthConfigs = AuthConfigs(authMode, keycloakConfig, oidcConfig, initialUserConfig)
}
