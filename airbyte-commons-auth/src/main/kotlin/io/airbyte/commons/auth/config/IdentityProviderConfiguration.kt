/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.auth.config

import io.micronaut.context.annotation.EachProperty

/**
 * This class pulls together all the auth identity provider configuration as defined in airbyte.yml.
 * It can be injected as a single dependency, rather than injecting each individual value
 * separately.
 */
@EachProperty(value = "airbyte-yml.auth.identity-providers", list = true)
data class IdentityProviderConfiguration(
  var domain: String? = null,
  var appName: String? = null,
  var displayName: String? = null,
  var clientId: String? = null,
  var clientSecret: String? = null,
) {
  // Eventually, AuthOidcConfiguration will simply replace this class.
  // For now, we want to support airbyte.auth.identity-providers for backwards-compatibility.
  fun toOidcConfig(): OidcConfig {
    return OidcConfig(domain!!, appName!!, displayName ?: appName!!, clientId!!, clientSecret!!)
  }
}
