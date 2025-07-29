/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.client.auth.AccessTokenHelper.Companion.decodeExpiry
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpHeaders
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.time.Clock
import java.time.Instant

private val logger = KotlinLogging.logger { }

/**
 * AccessTokenInterceptor adds an "Authorization: Bearer $token" header to requests,
 * where $token is a short-lived access token retrieved from an API token generation
 * endpoint.
 *
 * The given client ID and secret are used to retrieve the access tokens.
 * Access tokens are cached, and retrieved again when they expire.
 */
class AccessTokenInterceptor(
  private val clientId: String,
  private val clientSecret: String,
  private val tokenEndpoint: String,
  private val httpClient: OkHttpClient,
) : Interceptor {
  var clock = Clock.systemUTC()

  companion object {
    private const val CLIENT_ID_PARAM = "client_id"
    private const val CLIENT_SECRET_PARAM = "client_secret"
    private const val APPLICATION_JSON = "application/json"
    private const val TOKEN_REFRESH_BUFFER_SECONDS = 30L
    private const val ACCESS_TOKEN_RESPONSE_PARAM = "access_token"
  }

  private val objectMapper = ObjectMapper()

  @Volatile
  private var cachedToken: CachedToken? = null

  private data class CachedToken(
    val token: String,
    val expiresAt: Instant,
  )

  /**
   * Returns a valid token, using the cached token if available and not near expiry,
   * otherwise fetching a new token.
   */
  private fun getCachedToken(): String =
    synchronized(this) {
      val now = clock.instant()
      val token = cachedToken
      if (token != null && token.expiresAt.isAfter(now.plusSeconds(TOKEN_REFRESH_BUFFER_SECONDS))) {
        logger.debug { "Using cached token" }
        return@synchronized token.token
      }

      // Fetch new token and update cache.
      val newToken = fetchNewToken()
      cachedToken = newToken
      return@synchronized newToken.token
    }

  /**
   * Makes an HTTP POST to the token endpoint, parses the response, and returns a new [CachedToken].
   */
  private fun fetchNewToken(): CachedToken {
    logger.debug { "Fetching new access token from $tokenEndpoint..." }

    val jsonPayload =
      objectMapper.writeValueAsString(
        mapOf(
          CLIENT_ID_PARAM to clientId,
          CLIENT_SECRET_PARAM to clientSecret,
        ),
      )
    val requestBody = jsonPayload.toRequestBody(APPLICATION_JSON.toMediaType())
    val request =
      Request
        .Builder()
        .url(tokenEndpoint)
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
    logger.debug { "Fetched token with expiry at $expiry" }

    return CachedToken(token, expiry)
  }

  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()
    try {
      logger.debug { "Fetching access token from control plane..." }
      val accessToken = getCachedToken()
      builder.addHeader(HttpHeaders.AUTHORIZATION, "Bearer $accessToken")
      logger.debug { "Token added successfully. $accessToken" }
    } catch (e: Exception) {
      logger.error(e) { "Failed to obtain or add access token" }
      return chain.proceed(originalRequest)
    }
    val resp = chain.proceed(builder.build())

    // If the request failed with http 403 forbidden, then the current token is invalid
    // and should be cleared. The caller is expected to retry.
    if (resp.code == 403) {
      synchronized(this) {
        this.cachedToken = null
      }
    }
    return resp
  }
}
