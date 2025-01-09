/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Singleton
@Requires(property = "airbyte.auth.identity-provider.type", value = "generic-oidc")
data class OidcEndpointConfig(
  @Property(name = "airbyte.auth.identity-provider.oidc.endpoints.authorization-server-endpoint")
  val authorizationServerEndpoint: String,
  @Property(name = "airbyte.auth.identity-provider.oidc.endpoints.user-info-endpoint")
  var userInfoEndpoint: String,
  // TODO: I don't love that this lives here,
  //  I would have called this class OidcConfig though, but that already exists.
  @Property(name = "airbyte.auth.identity-provider.oidc.endpoints.client-id")
  var clientId: String,
)
