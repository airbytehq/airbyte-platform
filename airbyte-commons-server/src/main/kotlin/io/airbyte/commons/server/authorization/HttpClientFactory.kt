/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient

@Factory
class HttpClientFactory {
  /**
   * Create a new instance of {@link OkHttpClient} for use with Keycloak clients.
   * For now, this is a simple instance with no additional configuration, but this can be
   * tuned if needed for calls made to Keycloak.
   */
  @Singleton
  @Named("keycloakHttpClient")
  fun okHttpClient(): OkHttpClient = OkHttpClient()
}
