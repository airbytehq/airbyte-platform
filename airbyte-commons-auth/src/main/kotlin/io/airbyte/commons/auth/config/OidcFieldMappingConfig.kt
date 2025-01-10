/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires

@ConfigurationProperties("airbyte.auth.identity-provider.oidc.endpoints.fields")
@Requires(property = "airbyte.auth.identity-provider.type", value = "generic-oidc")
class OidcFieldMappingConfig {
  var sub: String = "sub"
  var email: String = "email"
  var name: String = "name"
}
