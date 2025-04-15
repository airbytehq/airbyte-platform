/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.config

import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton

@Singleton
class JwtIdentityProvidersConfig(
  @Property(name = "airbyte.auth.identity-provider.verify-issuer")
  val verifyIssuer: Boolean = true,
  @Property(name = "airbyte.auth.identity-provider.verify-audience")
  val verifyAudience: Boolean = true,
  @Property(name = "airbyte.auth.identity-provider.issuers")
  val issuers: List<String> = emptyList(),
  @Property(name = "airbyte.auth.identity-provider.audiences")
  val audiences: List<String> = emptyList(),
)
