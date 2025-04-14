/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Singleton
@Requires(property = "airbyte.auth.identity-provider.type", value = "generic-oidc")
data class GenericOidcConfig(
  @Property(name = "airbyte.auth.identity-provider.oidc.endpoints.authorization-server-endpoint")
  val authorizationServerEndpoint: String,
  @Property(name = "airbyte.auth.identity-provider.oidc.client-id")
  val clientId: String,
  @Property(name = "airbyte.auth.identity-provider.oidc.audience")
  val audience: String?,
)
