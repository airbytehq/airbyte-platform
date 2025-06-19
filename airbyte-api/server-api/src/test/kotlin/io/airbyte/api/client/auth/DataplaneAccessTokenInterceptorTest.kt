/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.airbyte.api.client.auth.DataplaneAccessTokenInterceptor.Companion.ACCESS_TOKEN_RESPONSE_PARAM
import io.airbyte.commons.auth.support.UnsignedJwtHelper
import io.micronaut.http.HttpHeaders
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.time.Duration.Companion.minutes

internal class DataplaneAccessTokenInterceptorTest {
  @Test
  fun `fetches new token if cached token is expired`() {
    val newToken = UnsignedJwtHelper.buildUnsignedJwtWithExpClaim(secondsFromNow = 5.minutes.inWholeSeconds)
    val httpClient = mockk<OkHttpClient>()
    every { httpClient.newCall(any()) } returns
      mockk {
        every { execute() } returns
          mockk {
            every { isSuccessful } returns true
            every { body } returns
              mockk {
                every { string() } returns "{\"$ACCESS_TOKEN_RESPONSE_PARAM\":\"$newToken\"}"
              }
          }
      }

    val interceptor =
      DataplaneAccessTokenInterceptor(
        authClientId = "test-client-id",
        authClientSecret = "test-client-secret",
        authTokenEndpoint = "http://example.com/token",
        httpClient = httpClient,
      )

    interceptor.cachedToken =
      DataplaneAccessTokenInterceptor.CachedToken(
        token = "expiredtoken",
        expiresAt = Instant.now().minusSeconds(5.minutes.inWholeSeconds),
      )

    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()
    val response: Response =
      mockk {
        every { code } returns 200
      }

    every { builder.addHeader(any(), any()) } returns (builder)
    every { builder.build() } returns (mockk<Request>())
    every { request.newBuilder() } returns (builder)
    every { chain.request() } returns (request)
    every { chain.proceed(any()) } returns (response)

    interceptor.intercept(chain)

    verify { httpClient.newCall(any()) }
    verify { builder.addHeader(HttpHeaders.AUTHORIZATION, "Bearer $newToken") }
  }

  @Test
  fun `test that the authorization header is added from a cached token`() {
    val httpClient = mockk<OkHttpClient>()

    val interceptor =
      DataplaneAccessTokenInterceptor(
        authClientId = "test-client-id",
        authClientSecret = "test-client-secret",
        authTokenEndpoint = "http://example.com/token",
        httpClient = httpClient,
      )

    interceptor.cachedToken =
      DataplaneAccessTokenInterceptor.CachedToken(
        token = "testtoken",
        expiresAt = Instant.now().plusSeconds(5.minutes.inWholeSeconds),
      )

    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()
    val response: Response =
      mockk {
        every { code } returns 200
      }

    every { builder.addHeader(any(), any()) } returns (builder)
    every { builder.build() } returns (mockk<Request>())
    every { request.newBuilder() } returns (builder)
    every { chain.request() } returns (request)
    every { chain.proceed(any()) } returns (response)

    interceptor.intercept(chain)

    verify(exactly = 0) { httpClient.newCall(any()) }
    verify { builder.addHeader(HttpHeaders.AUTHORIZATION, "Bearer testtoken") }
  }

  @Test
  fun `test that when token fetching fails, the original request is proceeded`() {
    // Force token fetching to fail by clearing the cached token and overriding getCachedToken.
    // Here we simulate failure by making getCachedToken throw an exception via a subclass.
    val failingInterceptor =
      object : DataplaneAccessTokenInterceptor(
        "test-client-id",
        "test-client-secret",
        "https://example.com/token",
        mockk<OkHttpClient>(),
      ) {
        override fun getCachedToken(): String = throw Exception("Token fetch error")
      }

    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()

    every { builder.addHeader(any(), any()) } returns (builder)
    every { builder.build() } returns (mockk<Request>())
    every { request.newBuilder() } returns (builder)
    every { chain.request() } returns (request)
    every { chain.proceed(any()) } returns (mockk<Response>())

    failingInterceptor.intercept(chain)

    verify(exactly = 0) { builder.addHeader(any(), any()) }
    verify(exactly = 1) { chain.proceed(request) }
  }

  @Test
  fun `test that when a request fails with 403 forbidden, the cached token is cleared`() {
    val httpClient = mockk<OkHttpClient>()

    val interceptor =
      DataplaneAccessTokenInterceptor(
        authClientId = "test-client-id",
        authClientSecret = "test-client-secret",
        authTokenEndpoint = "http://example.com/token",
        httpClient = httpClient,
      )

    interceptor.cachedToken =
      DataplaneAccessTokenInterceptor.CachedToken(
        token = "testtoken",
        expiresAt = Instant.now().plusSeconds(5.minutes.inWholeSeconds),
      )

    val chain: Interceptor.Chain = mockk()
    val request = Request.Builder().url("http://testing.airbyte.io").build()
    val response: Response =
      Response
        .Builder()
        .protocol(Protocol.HTTP_1_1)
        .message("forbidden")
        .request(request)
        .code(403)
        .build()

    every { chain.request() } returns request
    every { chain.proceed(any()) } returns response

    interceptor.intercept(chain)

    assertNull(interceptor.cachedToken)
  }
}
