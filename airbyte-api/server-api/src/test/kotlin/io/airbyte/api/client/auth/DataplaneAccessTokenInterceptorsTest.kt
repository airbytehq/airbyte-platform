/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.DataplaneApi
import io.airbyte.api.client.model.generated.AccessToken
import io.airbyte.api.client.model.generated.DataplaneTokenRequestBody
import io.micronaut.http.HttpHeaders
import io.micronaut.http.client.exceptions.ResponseClosedException
import io.micronaut.security.oauth2.client.clientcredentials.ClientCredentialsClient
import io.micronaut.security.oauth2.endpoint.token.response.TokenResponse
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono

class DataplaneAccessTokenInterceptorsTest {
  @Nested
  inner class DataplaneAccessTokenInterceptorWithSecurityTest {
    private lateinit var clientCredentialsClient: ClientCredentialsClient
    private lateinit var interceptor: DataplaneAccessTokenInterceptorWithSecurity
    private lateinit var chain: Interceptor.Chain
    private lateinit var request: Request
    private lateinit var response: Response

    @BeforeEach
    fun setup() {
      clientCredentialsClient = mockk()
      interceptor = DataplaneAccessTokenInterceptorWithSecurity(clientCredentialsClient)
      chain = mockk()
      request = mockk()
      response = mockk()
    }

    @Test
    fun `test intercept when clientCredentialsClient fails to return token`() {
      every { clientCredentialsClient.requestToken() } returns Mono.error(RuntimeException("Failed to get token"))
      every { chain.request() } returns request
      every { request.newBuilder() } returns Request.Builder().url("http://localhost")
      every { chain.proceed(request) } returns response

      val result = interceptor.intercept(chain)

      Assertions.assertEquals(response, result)
      verify { chain.proceed(request) }
    }

    @Test
    fun `test intercept when clientCredentialsClient has intermittent HTTP error`() {
      val tokenResponse = mockk<TokenResponse>()
      every { tokenResponse.accessToken } returns "valid-token"

      every { clientCredentialsClient.requestToken() } returnsMany
        listOf(
          Mono.error(ResponseClosedException("HTTP error")),
          Mono.just(tokenResponse),
        )
      every { chain.request() } returns request
      every { request.newBuilder() } returns Request.Builder().url("http://localhost")
      every { chain.proceed(any()) } returns response

      val result = interceptor.intercept(chain)
      Assertions.assertEquals(response, result)
      verify {
        chain.proceed(
          withArg {
            val header = it.header(HttpHeaders.AUTHORIZATION)
            Assertions.assertEquals("Bearer valid-token", header)
          },
        )
      }
    }

    @Test
    fun `test intercept when clientCredentialsClient returns persistent HTTP error`() {
      every { clientCredentialsClient.requestToken() } returns Mono.error(ResponseClosedException("HTTP error"))
      every { chain.request() } returns request
      every { request.newBuilder() } returns Request.Builder().url("http://localhost")
      every { chain.proceed(request) } returns response

      val result = interceptor.intercept(chain)

      Assertions.assertEquals(response, result)
      verify { chain.proceed(request) }
    }

    @Test
    fun `test intercept when clientCredentialsClient returns valid token and attaches Authorization header`() {
      val tokenResponse = mockk<TokenResponse>()
      every { tokenResponse.accessToken } returns "valid-token"
      every { clientCredentialsClient.requestToken() } returns Mono.just(tokenResponse)
      every { chain.request() } returns request
      every { request.newBuilder() } returns Request.Builder().url("http://localhost")
      every { chain.proceed(any()) } returns response

      val result = interceptor.intercept(chain)

      Assertions.assertEquals(response, result)
      verify {
        chain.proceed(
          withArg {
            val header = it.header(HttpHeaders.AUTHORIZATION)
            Assertions.assertEquals("Bearer valid-token", header)
          },
        )
      }
    }
  }

  @Nested
  inner class DataplaneAccessTokenInterceptorNoSecurityTest {
    private lateinit var airbyteApiClient: AirbyteApiClient
    private lateinit var dataplaneApiClient: DataplaneApi
    private lateinit var interceptor: DataplaneAccessTokenInterceptorNoSecurity
    private lateinit var chain: Interceptor.Chain
    private lateinit var request: Request
    private lateinit var response: Response

    private val clientId: String = "test-client-id"
    private val clientSecret: String = "test-client-secret"

    @BeforeEach
    fun setup() {
      dataplaneApiClient = mockk()
      airbyteApiClient =
        mockk {
          every { dataplaneApi } returns dataplaneApiClient
        }
      interceptor = DataplaneAccessTokenInterceptorNoSecurity(airbyteApiClient, clientId, clientSecret)
      chain = mockk()
      request = mockk()
      response = mockk()
    }

    @Test
    fun `test intercept when getDataplaneToken fails to return token`() {
      every { dataplaneApiClient.getDataplaneToken(any()) } throws Exception("HTTP error")
      every { chain.request() } returns request
      every { request.newBuilder() } returns Request.Builder().url("http://localhost")
      every { chain.proceed(request) } returns response

      val result = interceptor.intercept(chain)

      Assertions.assertEquals(response, result)
      verify { chain.proceed(request) }
      verify { dataplaneApiClient.getDataplaneToken(eq(DataplaneTokenRequestBody(clientId, clientSecret))) }
    }

    @Test
    fun `test intercept when getDataplaneToken returns valid token and attaches Authorization header`() {
      val tokenResponse = mockk<AccessToken>()
      every { tokenResponse.accessToken } returns "valid-token"
      every { airbyteApiClient.dataplaneApi.getDataplaneToken(any()) } returns tokenResponse
      every { chain.request() } returns request
      every { request.newBuilder() } returns Request.Builder().url("http://localhost")
      every { chain.proceed(any()) } returns response

      val result = interceptor.intercept(chain)

      Assertions.assertEquals(response, result)
      verify {
        chain.proceed(
          withArg {
            val header = it.header(HttpHeaders.AUTHORIZATION)
            Assertions.assertEquals("Bearer valid-token", header)
          },
        )
      }
    }
  }
}
