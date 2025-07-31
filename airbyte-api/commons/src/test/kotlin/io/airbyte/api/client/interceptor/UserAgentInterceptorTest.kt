/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.interceptor

import io.micronaut.http.HttpHeaders
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.Test

internal class UserAgentInterceptorTest {
  @Test
  internal fun `test that the user agent header is not overwritten if already present on the request`() {
    val applicationName = "the-application-name"
    val existingUserAgent = "existing-user-agent"
    val interceptor = UserAgentInterceptor(userAgent = applicationName)
    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()

    every { builder.addHeader(any(), any()) } returns (builder)
    every { builder.build() } returns (mockk<Request>())
    every { request.header(HttpHeaders.USER_AGENT) } returns existingUserAgent
    every { request.newBuilder() } returns (builder)
    every { chain.request() } returns (request)
    every { chain.proceed(any()) } returns (mockk<Response>())

    interceptor.intercept(chain)

    verify(exactly = 0) { builder.addHeader(HttpHeaders.USER_AGENT, any()) }
  }

  @Test
  internal fun `test that the user agent header is added if not present on the request`() {
    val applicationName = "the-application-name"
    val interceptor = UserAgentInterceptor(userAgent = applicationName)
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

    verify(exactly = 1) { builder.addHeader(HttpHeaders.USER_AGENT, "TheApplicationName") }
  }
}
