/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DataplaneTokenRequestBody
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
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
 * Interceptor that is used by Dataplane clients to add an access token to the request headers
 * that authenticates the request to the Control Plane. Intended for dataplane applications that
 * have Micronaut Security enabled, so that the lifecycle of the access token can be automatically
 * cached/refreshed/managed by Micronaut Security's [ClientCredentialsClient].
 *
 * Note that enabling Micronaut Security causes significant application startup overhead, and is
 * not recommended for applications that require fast startup times (like the Connector Sidecar and
 * Workload Init applications).
 */
@Singleton
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "micronaut.security.oauth2.clients.dataplane.client-id", pattern = ".+")
@Requires(property = "micronaut.security.oauth2.clients.dataplane.client-secret", pattern = ".+")
@Replaces(KeycloakAccessTokenInterceptor::class) // TODO(parker) remove this @Replaces once dataplane apps are no longer keycloak clients
class DataplaneAccessTokenInterceptorWithSecurity(
  @Named("dataplane") private val clientCredentialsClient: ClientCredentialsClient,
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
      logger.debug { "Intercepting request to add control plane access token..." }
      val accessToken = fetchAccessToken().block()
      if (accessToken != null) {
        builder.addHeader(HttpHeaders.AUTHORIZATION, "Bearer $accessToken")
        logger.debug { "Successfully added access token to header" }
      } else {
        logger.error { "Failed to obtain access token from Control Plane" }
      }
    } catch (e: Exception) {
      logger.error(e) { "Failed to add access token to request" }
      // do not throw exception, just proceed with the original request and let the request fail authorization downstream.
      return chain.proceed(originalRequest)
    }

    return chain.proceed(builder.build())
  }
}

/**
 * Interceptor that is used by Dataplane clients to add an access token to the request headers
 * that authenticates the request to the Control Plane. Intended for dataplane applications that
 * need to avoid the startup overhead of Micronaut Security, and can get away with naively calling
 * the token endpoint for each request.
 */
@Singleton
@Requires(property = "micronaut.security.enabled", value = "false")
@Requires(property = "airbyte.auth.dataplane-client-id", pattern = ".+")
@Requires(property = "airbyte.auth.dataplane-client-secret", pattern = ".+")
@Replaces(KeycloakAccessTokenInterceptor::class) // TODO(parker) remove this @Replaces once dataplane apps are no longer keycloak clients
class DataplaneAccessTokenInterceptorNoSecurity(
  private val airbyteApiClient: AirbyteApiClient,
  @Value("\${airbyte.auth.dataplane-client-id}") private val clientId: String,
  @Value("\${airbyte.auth.dataplane-client-secret}") private val clientSecret: String,
) : AirbyteApiInterceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    try {
      logger.debug { "Intercepting request to add dataplane access token..." }
      val accessToken =
        airbyteApiClient.dataplaneApi
          .getDataplaneToken(
            DataplaneTokenRequestBody(
              clientId = clientId,
              clientSecret = clientSecret,
            ),
          ).accessToken
      builder.addHeader(HttpHeaders.AUTHORIZATION, "Bearer $accessToken")
      logger.debug { "Successfully added access token to header" }
    } catch (e: Exception) {
      logger.error(e) { "Failed to add access token to request" }
      // do not throw exception, just proceed with the original request and let the request fail authorization downstream.
      return chain.proceed(chain.request())
    }
    return chain.proceed(builder.build())
  }
}
