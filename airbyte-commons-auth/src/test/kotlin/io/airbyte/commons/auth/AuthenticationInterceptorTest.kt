package io.airbyte.commons.auth

import io.airbyte.commons.auth.AuthenticationInterceptor.Companion.BEARER_TOKEN_PREFIX
import io.airbyte.commons.auth.AuthenticationInterceptor.Companion.USER_AGENT_VALUE
import io.micronaut.http.HttpHeaders
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.Test
import java.util.Base64

class AuthenticationInterceptorTest {
  @Test
  fun `test that when the bearer token is not blank, the authentication header is added`() {
    val bearerToken = "a bearer token"
    val expectedBearerToken = Base64.getEncoder().encodeToString(bearerToken.toByteArray())
    val interceptor = AuthenticationInterceptor(bearerToken)
    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()

    every { builder.header(any(), any()) }.returns(builder)
    every { builder.build() }.returns(mockk<Request>())
    every { request.newBuilder() }.returns(builder)
    every { chain.request() }.returns(request)
    every { chain.proceed(any()) }.returns(mockk<Response>())

    interceptor.intercept(chain)

    verify { builder.header(HttpHeaders.USER_AGENT, USER_AGENT_VALUE) }
    verify { builder.header(HttpHeaders.AUTHORIZATION, "$BEARER_TOKEN_PREFIX $expectedBearerToken") }
  }

  @Test
  fun `test that when the bearer token is blank, the authentication header is not added`() {
    val bearerToken = ""
    val interceptor = AuthenticationInterceptor(bearerToken)
    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()

    every { builder.header(any(), any()) }.returns(builder)
    every { builder.build() }.returns(mockk<Request>())
    every { request.newBuilder() }.returns(builder)
    every { chain.request() }.returns(request)
    every { chain.proceed(any()) }.returns(mockk<Response>())

    interceptor.intercept(chain)

    verify { builder.header(HttpHeaders.USER_AGENT, USER_AGENT_VALUE) }
    verify(exactly = 0) { builder.header(HttpHeaders.AUTHORIZATION, "$BEARER_TOKEN_PREFIX $bearerToken") }
  }
}
