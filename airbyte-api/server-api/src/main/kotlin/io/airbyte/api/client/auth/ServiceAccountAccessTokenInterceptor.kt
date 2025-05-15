/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.client.auth.AccessTokenHelper.Companion.decodeExpiry
import io.airbyte.api.server.generated.models.ServiceAccountTokenRequestBody
import io.airbyte.commons.annotation.InternalForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpHeaders
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.time.Instant
import java.util.UUID

private val logger = KotlinLogging.logger { }

@Singleton
@Requires(property = "airbyte.auth.service-account-id", pattern = ".+")
@Requires(property = "airbyte.auth.service-account-secret", pattern = ".+")
@Requires(property = "airbyte.auth.service-account-token-endpoint", pattern = ".+")
@Replaces(KeycloakAccessTokenInterceptor::class)
open class ServiceAccountAccessTokenInterceptor(
  @Value("\${airbyte.auth.service-account-id}") private val serviceAccountId: String,
  @Value("\${airbyte.auth.service-account-secret}") private val serviceAccountSecret: String,
  @Value("\${airbyte.auth.service-account-token-endpoint}") private val serviceAccountTokenEndpoint: String,
  @Named("airbyteApiOkHttpClientWithoutInterceptors") private val httpClient: OkHttpClient,
) : AirbyteApiInterceptor {
  companion object {
    private const val APPLICATION_JSON = "application/json"
    private const val TOKEN_REFRESH_BUFFER_SECONDS = 30L

    @InternalForTesting
    internal const val ACCESS_TOKEN_RESPONSE_PARAM = "access_token"
  }

  private val objectMapper = ObjectMapper()

  @InternalForTesting
  @Volatile
  internal var cachedToken: CachedToken? = null

  @InternalForTesting
  internal data class CachedToken(
    val token: String,
    val expiresAt: Instant,
  )

  @InternalForTesting
  internal open fun getCachedToken(): String =
    synchronized(this) {
      val now = Instant.now()
      val token = cachedToken
      if (token != null && token.expiresAt.isAfter(now.plusSeconds(TOKEN_REFRESH_BUFFER_SECONDS))) {
        logger.debug { "Using cached token for service account" }
        return@synchronized token.token
      }

      val newToken = fetchNewToken()
      cachedToken = newToken
      return@synchronized newToken.token
    }

  private fun fetchNewToken(): CachedToken {
    logger.debug { "Fetching new service account access token from $serviceAccountTokenEndpoint..." }

    val jsonPayload =
      objectMapper.writeValueAsString(
        ServiceAccountTokenRequestBody(
          serviceAccountId = UUID.fromString(serviceAccountId),
          secret = serviceAccountSecret,
        ),
      )
    val requestBody = jsonPayload.toRequestBody(APPLICATION_JSON.toMediaType())
    val request =
      Request
        .Builder()
        .url(serviceAccountTokenEndpoint)
        .post(requestBody)
        .build()

    val response: Response = httpClient.newCall(request).execute()
    if (!response.isSuccessful) {
      throw Exception("Token request failed with HTTP code ${response.code}")
    }

    val responseBody = response.body?.string() ?: throw Exception("Token response body is null")
    val tokenNode = objectMapper.readTree(responseBody)
    val token = tokenNode.get(ACCESS_TOKEN_RESPONSE_PARAM).asText()

    val expiry = decodeExpiry(token)
    logger.debug { "Fetched service account token with expiry at $expiry" }

    return CachedToken(token, expiry)
  }

  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    try {
      logger.debug { "Fetching service account access token from control plane..." }
      val accessToken = getCachedToken()
      builder.addHeader(HttpHeaders.AUTHORIZATION, "Bearer $accessToken")
      logger.debug { "Service account token added successfully" }
    } catch (e: Exception) {
      logger.error(e) { "Failed to obtain or add service account access token" }
      return chain.proceed(originalRequest)
    }
    return chain.proceed(builder.build())
  }
}
