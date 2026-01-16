/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.config

import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration

@Factory
class ApplicationServiceConfig {
  @Singleton
  @RequiresAirbyteProEnabled
  @Named("access-token-expiration-time")
  @Primary
  fun getProAccessTokenExpirationTime(): Duration = Duration.ofHours(24)

  @Singleton
  @Secondary
  @Named("access-token-expiration-time")
  fun getAccessTokenExpirationTime(): Duration = Duration.ofMinutes(3)
}
