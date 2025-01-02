/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import io.micronaut.context.BeanProvider
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.Test

internal class InternalApiAuthenticationInterceptorTest {
  @Test
  internal fun `test that when the internal API header name is not blank, the authentication header is added`() {
    val internalApiHeaderName = "header-name"
    val authHeaderValue = "the token"
    val internalApiAuthToken: BeanProvider<String> = mockk()
    val interceptor = InternalApiAuthenticationInterceptor(authHeaderName = internalApiHeaderName, authHeaderValue = internalApiAuthToken)
    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()

    every { internalApiAuthToken.isPresent } returns true
    every { internalApiAuthToken.get() } returns authHeaderValue
    every { builder.addHeader(any(), any()) } returns (builder)
    every { builder.build() } returns (mockk<Request>())
    every { request.newBuilder() } returns (builder)
    every { chain.request() } returns (request)
    every { chain.proceed(any()) } returns (mockk<Response>())

    interceptor.intercept(chain)

    verify { builder.addHeader(internalApiHeaderName, authHeaderValue) }
  }

  @Test
  internal fun `test that when the internal API header name is not blank but the token value is missing, the authentication header is not added`() {
    val internalApiHeaderName = "header-name"
    val authHeaderValue = "the token"
    val internalApiAuthToken: BeanProvider<String> = mockk()
    val interceptor = InternalApiAuthenticationInterceptor(authHeaderName = internalApiHeaderName, authHeaderValue = internalApiAuthToken)
    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()

    every { internalApiAuthToken.isPresent } returns false
    every { builder.addHeader(any(), any()) } returns (builder)
    every { builder.build() } returns (mockk<Request>())
    every { request.newBuilder() } returns (builder)
    every { chain.request() } returns (request)
    every { chain.proceed(any()) } returns (mockk<Response>())

    interceptor.intercept(chain)

    verify(exactly = 0) { builder.addHeader(any(), authHeaderValue) }
  }

  @Test
  internal fun `test that when the internal API header name is blank, the authentication header is not added`() {
    val internalApiHeaderName = ""
    val authHeaderValue = "the token"
    val internalApiAuthToken: BeanProvider<String> = mockk()
    val interceptor = InternalApiAuthenticationInterceptor(authHeaderName = internalApiHeaderName, authHeaderValue = internalApiAuthToken)
    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()

    every { internalApiAuthToken.isPresent } returns true
    every { internalApiAuthToken.get() } returns authHeaderValue
    every { builder.addHeader(any(), any()) } returns (builder)
    every { builder.build() } returns (mockk<Request>())
    every { request.newBuilder() } returns (builder)
    every { chain.request() } returns (request)
    every { chain.proceed(any()) } returns (mockk<Response>())

    interceptor.intercept(chain)

    verify(exactly = 0) { builder.addHeader(any(), authHeaderValue) }
  }
}
