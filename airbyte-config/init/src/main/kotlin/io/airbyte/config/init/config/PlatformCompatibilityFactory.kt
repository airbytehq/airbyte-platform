/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init.config

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import java.time.Duration

@Factory
class PlatformCompatibilityFactory {
  @Singleton
  @Named("platformCompatibilityClient")
  fun platformCompatibilityClient(
    @Value("\${airbyte.platform-compatibility.remote.timeout-ms:30000}") platformCompatibilityRemoteTimeoutMs: Long,
  ): OkHttpClient {
    return OkHttpClient.Builder()
      .callTimeout(Duration.ofMillis(platformCompatibilityRemoteTimeoutMs))
      .build()
  }
}
