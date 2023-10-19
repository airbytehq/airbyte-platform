package io.airbyte.notification.config

import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient

@Factory
class HttpClientFactory {
  @Singleton
  @Named("webhookHttpClient")
  fun okHttpClient(): OkHttpClient {
    return OkHttpClient()
  }
}
