/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.airbyte.commons.auth.support.UnsignedJwtHelper
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlin.time.Duration.Companion.minutes

internal class AccessTokenInterceptorTest {
  lateinit var mockServer: MockWebServer

  @BeforeEach
  fun setup() {
    mockServer = MockWebServer()
    mockServer.start()
  }

  @AfterEach
  fun teardown() {
    mockServer.shutdown()
  }

  @Test
  fun tokenFetching() {
    // This tests the behaviors of token fetching: caching, expiration, errors, etc.

    val exp = 5.minutes.inWholeSeconds

    // Set up a mock server that will respond to calls to the /token and /api endpoints.
    // The interceptor will retrieve a token from the /token endpoint for each call to /api.
    // The first time, there is no cached token, so the interceptor retrieves a token.
    // The second time, the token is expired, so the interceptor retrieves a fresh token.
    val interceptor =
      AccessTokenInterceptor(
        clientId = "test-client-id",
        clientSecret = "test-client-secret",
        tokenEndpoint = mockServer.url("/token").toString(),
        httpClient = OkHttpClient().newBuilder().build(),
      )
    interceptor.clock = Clock.fixed(Instant.now(), ZoneId.systemDefault())

    val client = OkHttpClient().newBuilder().addInterceptor(interceptor).build()
    val apiRequest = Request.Builder().url(mockServer.url("/api")).build()

    fun doApiRequest() = client.newCall(apiRequest).execute()

    // Make the first call to /api. Since this is the first call and there's no cached token,
    // the /token endpoint will be called first to retrieve an access token.
    val firstToken = UnsignedJwtHelper.buildUnsignedJwtWithExpClaim(exp, interceptor.clock)
    mockServer.enqueue(MockResponse().setBody("""{"access_token": "$firstToken"}"""))
    mockServer.enqueue(MockResponse().setBody("ok"))
    doApiRequest()

    // Verify the content of the first /token request.
    mockServer.takeRequest().run {
      assertEquals("/token", path)
      assertEquals("""{"client_id":"test-client-id","client_secret":"test-client-secret"}""", body.readUtf8())
    }

    // Verify that the token was sent on the /api request.
    assertEquals("Bearer $firstToken", mockServer.takeRequest().headers["Authorization"])

    // Move clock forward 1 minute. The token is not yet expired,
    // so the call will not retrieve a token.
    interceptor.clock = Clock.offset(interceptor.clock, Duration.ofMinutes(1))
    mockServer.enqueue(MockResponse().setBody("ok"))
    doApiRequest()
    assertEquals("/api", mockServer.takeRequest().path)

    // Move the clock forward so that the first token expires.
    // The next call to /api will trigger a call to /token to retrieve a fresh access token.
    interceptor.clock = Clock.offset(interceptor.clock, Duration.ofMinutes(5))
    val secondToken = UnsignedJwtHelper.buildUnsignedJwtWithExpClaim(exp, interceptor.clock)
    mockServer.enqueue(MockResponse().setBody("""{"access_token": "$secondToken"}"""))
    mockServer.enqueue(MockResponse().setBody("ok"))
    doApiRequest()

    assertEquals("/token", mockServer.takeRequest().path)
    mockServer.takeRequest().run {
      assertEquals("/api", path)
      assertEquals("Bearer $secondToken", headers["Authorization"])
    }

    // If a call to retrieve a token fails with a 5xx, the interceptor will proceed with the original request.
    // The /api request will not have an Authorization header.
    // Move the clock forward so the token is expired, then set up the /token endpoint to return a 500.
    interceptor.clock = Clock.offset(interceptor.clock, Duration.ofMinutes(6))
    mockServer.enqueue(MockResponse().setResponseCode(500))
    mockServer.enqueue(MockResponse().setBody("ok"))
    doApiRequest()

    assertEquals("/token", mockServer.takeRequest().path)
    mockServer.takeRequest().run {
      assertEquals("/api", path)
      assertEquals(null, headers["Authorization"])
    }

    // Reset the scenario with a successful /token and /api request.
    val thirdToken = UnsignedJwtHelper.buildUnsignedJwtWithExpClaim(exp, interceptor.clock)
    mockServer.enqueue(MockResponse().setBody("""{"access_token": "$thirdToken"}"""))
    mockServer.enqueue(MockResponse().setBody("ok"))
    doApiRequest()
    assertEquals("/token", mockServer.takeRequest().path)
    assertEquals("/api", mockServer.takeRequest().path)

    // If a call to the API returns a 403, the current cached token will be invalidated, and the next call
    // will retrieve a fresh token.
    mockServer.enqueue(MockResponse().setResponseCode(403))
    doApiRequest()
    assertEquals("/api", mockServer.takeRequest().path)

    val fourthToken = UnsignedJwtHelper.buildUnsignedJwtWithExpClaim(exp, interceptor.clock)
    mockServer.enqueue(MockResponse().setBody("""{"access_token": "$fourthToken"}"""))
    mockServer.enqueue(MockResponse().setBody("ok"))
    doApiRequest()

    assertEquals("/token", mockServer.takeRequest().path)
    mockServer.takeRequest().run {
      assertEquals("/api", path)
      assertEquals("Bearer $fourthToken", headers["Authorization"])
    }
  }
}
