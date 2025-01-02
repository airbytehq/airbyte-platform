/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config

import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.net.http.HttpClient

@Factory
class HttpClientFactory {
  @Singleton
  @Named("webhookHttpClient")
  fun webhookHttpClient(): HttpClient {
    return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
  }
}
