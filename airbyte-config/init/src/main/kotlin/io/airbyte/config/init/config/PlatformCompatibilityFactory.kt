/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init.config

import io.airbyte.micronaut.runtime.AirbytePlatformCompatibilityConfig
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import java.time.Duration

@Factory
class PlatformCompatibilityFactory {
  @Singleton
  @Named("platformCompatibilityClient")
  fun platformCompatibilityClient(airbytePlatformCompatibilityConfig: AirbytePlatformCompatibilityConfig): OkHttpClient =
    OkHttpClient
      .Builder()
      .callTimeout(Duration.ofMillis(airbytePlatformCompatibilityConfig.remote.timeoutMs))
      .build()
}
