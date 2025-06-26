/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpHeaders
import io.micronaut.http.exceptions.HttpException
import io.micronaut.security.oauth2.client.clientcredentials.ClientCredentialsClient
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Interceptor that adds an access token to the request headers using the Keycloak client credentials flow.
 * Only enabled when the current application is configured with a Keycloak oauth2 client.
 * Micronaut will automatically inject the @Named client credentials client bean based on the
 * `micronaut.security.oauth2.clients.keycloak.*` properties that this interceptor requires.
 */
@Singleton
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "micronaut.security.oauth2.clients.keycloak.client-id", pattern = ".+")
@Requires(property = "micronaut.security.oauth2.clients.keycloak.client-secret", pattern = ".+")
@Requires(property = "micronaut.security.oauth2.clients.keycloak.openid.issuer", pattern = ".+")
class KeycloakAccessTokenInterceptor(
  @Named("keycloak") private val clientCredentialsClient: ClientCredentialsClient,
) : AirbyteApiInterceptor {
  private fun fetchAccessToken(): Mono<String?> =
    Mono
      .defer { Mono.from(clientCredentialsClient.requestToken()) }
      .map { it.accessToken }
      .retryWhen(
        Retry
          .backoff(3, Duration.ofSeconds(1))
          .filter { it is HttpException },
      )

  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    try {
      logger.debug { "Intercepting request to add Keycloak access token..." }
      val accessToken = fetchAccessToken().block()
      if (accessToken != null) {
        builder.addHeader(HttpHeaders.AUTHORIZATION, "Bearer $accessToken")
        logger.debug { "Added access token to header $accessToken" }
      } else {
        logger.error { "Failed to obtain access token from Keycloak" }
      }
    } catch (e: Exception) {
      logger.error(e) { "Failed to add Keycloak access token to request" }
      // do not throw exception, just proceed with the original request and let the request fail
      // authorization downstream.
      return chain.proceed(originalRequest)
    }

    return chain.proceed(builder.build())
  }
}
