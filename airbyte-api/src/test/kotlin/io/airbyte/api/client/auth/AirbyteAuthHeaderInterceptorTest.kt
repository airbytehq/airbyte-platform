/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.micronaut.http.HttpHeaders
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.Test
import java.util.Optional

internal class AirbyteAuthHeaderInterceptorTest {
  @Test
  internal fun `test that when the Airbyte auth header is provided, the authentication header is added`() {
    val applicationName = "the-application-name"
    val headerName = "header-name"
    val headerValue = "header-value"
    val authHeader =
      object : AirbyteAuthHeader {
        override fun getHeaderName(): String {
          return headerName
        }

        override fun getHeaderValue(): String {
          return headerValue
        }
      }
    val interceptor = AirbyteAuthHeaderInterceptor(Optional.of(authHeader), applicationName)
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
    verify { builder.addHeader(headerName, headerValue) }
  }

  @Test
  internal fun `test that when the Airbyte auth header is not provided, the authentication header is not added`() {
    val applicationName = "the-application-name"
    val interceptor = AirbyteAuthHeaderInterceptor(Optional.empty(), applicationName)
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
    verify(exactly = 0) { builder.addHeader(any(), not(formatUserAgent(applicationName))) }
  }
}
