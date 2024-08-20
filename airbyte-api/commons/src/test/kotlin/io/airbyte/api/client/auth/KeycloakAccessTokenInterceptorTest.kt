package io.airbyte.api.client.auth

import io.micronaut.http.HttpHeaders
import io.micronaut.security.oauth2.client.clientcredentials.ClientCredentialsClient
import io.micronaut.security.oauth2.endpoint.token.response.TokenResponse
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono

class KeycloakAccessTokenInterceptorTest {
  private lateinit var clientCredentialsClient: ClientCredentialsClient
  private lateinit var interceptor: KeycloakAccessTokenInterceptor
  private lateinit var chain: Interceptor.Chain
  private lateinit var request: Request
  private lateinit var response: Response

  @BeforeEach
  fun setup() {
    clientCredentialsClient = mockk()
    interceptor = KeycloakAccessTokenInterceptor(clientCredentialsClient)
    chain = mockk()
    request = mockk()
    response = mockk()
  }

  @Test
  fun `test intercept when clientCredentialsClient fails to return token`() {
    every { clientCredentialsClient.requestToken() } returns Mono.error(RuntimeException("Failed to get token"))
    every { chain.request() } returns request
    every { chain.proceed(request) } returns response

    val result = interceptor.intercept(chain)

    assertEquals(response, result)
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

    assertEquals(response, result)
    verify {
      chain.proceed(
        withArg {
          val header = it.header(HttpHeaders.AUTHORIZATION)
          assertEquals("Bearer valid-token", header)
        },
      )
    }
  }
}
