package io.airbyte.server.config

import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient

@Factory
class HttpClientFactory {
  /**
   * Create a new instance of {@link OkHttpClient} for use with the Keycloak token validator.
   * For now, this is a simple instance with no additional configuration, but this can be
   * tuned if needed for calls made within the {@link KeycloakTokenValidator}.
   */
  @Singleton
  @Named("keycloakTokenValidatorHttpClient")
  fun okHttpClient(): OkHttpClient {
    return OkHttpClient()
  }
}
