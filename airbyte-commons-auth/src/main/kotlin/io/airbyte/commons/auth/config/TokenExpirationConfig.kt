/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton

@Singleton
class TokenExpirationConfig(
  @Property(name = "airbyte.auth.token-expiration.application-token-expiration-in-minutes")
  val applicationTokenExpirationInMinutes: Long = 15,
  @Property(name = "airbyte.auth.token-expiration.dataplane-token-expiration-in-minutes")
  val dataplaneTokenExpirationInMinutes: Long = 5,
  @Property(name = "airbyte.auth.token-expiration.embedded-token-expiration-in-minutes")
  val embeddedTokenExpirationInMinutes: Long = 15,
  @Property(name = "airbyte.auth.token-expiration.service-account-token-expiration-in-minutes")
  val serviceAccountTokenExpirationInMinutes: Long = 15,
)
