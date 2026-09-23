/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.config

import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration

@Factory
class ApplicationServiceConfig {
  @Singleton
  @Named("access-token-expiration-time")
  fun getAccessTokenExpirationTime(): Duration = Duration.ofMinutes(3)
}
