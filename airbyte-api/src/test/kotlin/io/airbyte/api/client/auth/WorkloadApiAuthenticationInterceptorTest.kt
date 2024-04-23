/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.airbyte.api.client.auth.WorkloadApiAuthenticationInterceptor.Companion.BEARER_TOKEN_PREFIX
import io.micronaut.http.HttpHeaders
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.Test
import java.util.Base64

internal class WorkloadApiAuthenticationInterceptorTest {
  @Test
  internal fun `test that when the bearer token is not blank, the authentication header is added`() {
    val applicationName = "the-application-name"
    val bearerToken = "a bearer token"
    val expectedBearerToken = Base64.getEncoder().encodeToString(bearerToken.toByteArray())
    val interceptor = WorkloadApiAuthenticationInterceptor(bearerToken, applicationName)
    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()

    every { builder.addHeader(any(), any()) } returns (builder)
    every { builder.build() } returns (mockk<Request>())
    every { request.header(HttpHeaders.USER_AGENT) } returns null
    every { request.newBuilder() } returns (builder)
    every { chain.request() } returns (request)
    every { chain.proceed(any()) } returns (mockk<Response>())

    interceptor.intercept(chain)

    verify { builder.addHeader(HttpHeaders.USER_AGENT, formatUserAgent(applicationName)) }
    verify { builder.addHeader(HttpHeaders.AUTHORIZATION, "$BEARER_TOKEN_PREFIX $expectedBearerToken") }
  }

  @Test
  internal fun `test that when the bearer token is blank, the authentication header is not added`() {
    val applicationName = "the-application-name"
    val bearerToken = ""
    val interceptor = WorkloadApiAuthenticationInterceptor(bearerToken, applicationName)
    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()

    every { builder.addHeader(any(), any()) } returns (builder)
    every { builder.build() } returns (mockk<Request>())
    every { request.header(HttpHeaders.USER_AGENT) } returns null
    every { request.newBuilder() } returns (builder)
    every { chain.request() } returns (request)
    every { chain.proceed(any()) } returns (mockk<Response>())

    interceptor.intercept(chain)

    verify { builder.addHeader(HttpHeaders.USER_AGENT, formatUserAgent(applicationName)) }
    verify(exactly = 0) { builder.addHeader(HttpHeaders.AUTHORIZATION, "$BEARER_TOKEN_PREFIX $bearerToken") }
  }
}
