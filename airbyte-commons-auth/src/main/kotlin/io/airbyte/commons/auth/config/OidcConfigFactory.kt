package io.airbyte.commons.auth.config

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
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
  @Primary
  @Requires(property = "airbyte.auth.identity-provider.type", value = "oidc")
  fun defaultOidcConfig(
    @Value("\${airbyte.auth.identity-provider.oidc.domain}") domain: String?,
    @Value("\${airbyte.auth.identity-provider.oidc.app-name}") appName: String?,
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

    return OidcConfig(domain, appName, clientId, clientSecret)
  }

  /**
   * Returns the OidcConfig with values from the single-idp-style `airbyte.yml` config, if present.
   * This is for backwards compatibility.
   */
  @Singleton
  @Requires(property = "airbyte-yml.auth.identity-provider.type", value = "oidc")
  fun airbyteYmlSingleOidcConfig(
    @Value("\${airbyte-yml.auth.identity-provider.oidc.domain}") domain: String?,
    @Value("\${airbyte-yml.auth.identity-provider.oidc.app-name}") appName: String?,
    @Value("\${airbyte-yml.auth.identity-provider.oidc.client-id}") clientId: String?,
    @Value("\${airbyte-yml.auth.identity-provider.oidc.client-secret}") clientSecret: String?,
  ): OidcConfig {
    if (domain.isNullOrEmpty() || appName.isNullOrEmpty() || clientId.isNullOrEmpty() || clientSecret.isNullOrEmpty()) {
      throw IllegalStateException(
        "Missing required OIDC configuration. Please ensure all of the following properties are set: " +
          "airbyte-yml.auth.identity-provider.oidc.domain, " +
          "airbyte-yml.auth.identity-provider.oidc.app-name, " +
          "airbyte-yml.auth.identity-provider.oidc.client-id, " +
          "airbyte-yml.auth.identity-provider.oidc.client-secret",
      )
    }
    return OidcConfig(domain, appName, clientId, clientSecret)
  }

  /**
   * Returns the OidcConfig with values from the list-style `airbyte.yml` config, if present.
   * This is for backwards compatibility.
   */
  @Singleton
  @Requires(missingProperty = "airbyte-yml.auth.identity-provider")
  @Requires(property = "airbyte-yml.auth.identity-providers")
  fun airbyteYmlListOidcConfig(idpConfigList: List<IdentityProviderConfiguration>?): OidcConfig {
    if (idpConfigList.isNullOrEmpty()) {
      throw IllegalStateException(
        "Missing required OIDC configuration. Please ensure all of the following properties are set: airbyte-yml.auth.identity-providers",
      )
    }
    if (idpConfigList.size > 1) {
      throw IllegalStateException("Only one identity provider is supported. Found ${idpConfigList.size} identity providers.")
    }

    return idpConfigList.first().toOidcConfig()
  }
}

data class OidcConfig(
  var domain: String,
  var appName: String,
  var clientId: String,
  var clientSecret: String,
)
