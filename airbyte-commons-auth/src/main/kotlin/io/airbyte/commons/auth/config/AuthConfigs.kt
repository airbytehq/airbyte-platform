package io.airbyte.commons.auth.config

import io.airbyte.config.Configs
import io.airbyte.config.Configs.DeploymentMode
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
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

@Factory
class AuthModeFactory(
  val deploymentMode: DeploymentMode,
  val airbyteEdition: Configs.AirbyteEdition,
) {
  /**
   * When the Micronaut environment is set to `community-auth`, the `SIMPLE` auth mode is used
   * regardless of the deployment mode or other configurations. This bean replaces the
   * [defaultAuthMode] when the `community-auth` environment is active.
   */
  @Singleton
  @Requires(env = ["community-auth"])
  @Primary
  fun communityAuthMode(): AuthMode {
    return AuthMode.SIMPLE
  }

  /**
   * The default auth mode is determined by the deployment mode and edition.
   */
  @Singleton
  fun defaultAuthMode(): AuthMode {
    return when {
      deploymentMode == DeploymentMode.CLOUD -> AuthMode.OIDC
      airbyteEdition == Configs.AirbyteEdition.PRO -> AuthMode.OIDC
      deploymentMode == DeploymentMode.OSS -> AuthMode.NONE
      else -> throw IllegalStateException("Unknown or unspecified deployment mode: $deploymentMode")
    }
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
  val authMode: AuthMode,
  val keycloakConfig: AirbyteKeycloakConfiguration? = null,
  val oidcConfig: OidcConfig? = null,
  val initialUserConfig: InitialUserConfig? = null,
) {
  @Singleton
  fun authConfig(): AuthConfigs {
    return AuthConfigs(authMode, keycloakConfig, oidcConfig, initialUserConfig)
  }
}
