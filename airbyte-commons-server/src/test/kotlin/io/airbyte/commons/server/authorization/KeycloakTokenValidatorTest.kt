/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import io.airbyte.data.services.impls.keycloak.AirbyteKeycloakClient
import io.airbyte.data.services.impls.keycloak.InvalidTokenException
import io.airbyte.metrics.MetricClient
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.netty.NettyHttpHeaders
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.jwt.validator.JwtAuthenticationFactory
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import reactor.test.StepVerifier
import java.net.URI
import java.util.Optional
import java.util.function.Predicate

internal class KeycloakTokenValidatorTest {
  private lateinit var keycloakTokenValidator: KeycloakTokenValidator
  private lateinit var airbyteKeycloakClient: AirbyteKeycloakClient
  private lateinit var authenticationFactory: JwtAuthenticationFactory

  @BeforeEach
  fun setUp() {
    // Make sure we're covering the case where the token contains an underscore, since this used to
    // break in production.
    assert(VALID_ACCESS_TOKEN.contains("_"))

    airbyteKeycloakClient = mockk()
    authenticationFactory = mockk()

    keycloakTokenValidator =
      KeycloakTokenValidator(airbyteKeycloakClient, authenticationFactory, Optional.empty<MetricClient>())
  }

  @Test
  fun testValidTokenCreatesAuthentication() {
    val expectedUserId = "0f0cbf9a-24c2-46cc-b582-d1ff2c0d5ef5"
    val httpRequest = mockHttpRequest(VALID_ACCESS_TOKEN)

    // Mock the AirbyteKeycloakClient to not throw (valid token)
    every { airbyteKeycloakClient.validateToken(VALID_ACCESS_TOKEN) } just Runs

    val mockedRoles =
      mutableSetOf<String?>(
        "ORGANIZATION_ADMIN",
        "ORGANIZATION_EDITOR",
        "ORGANIZATION_READER",
        "ORGANIZATION_MEMBER",
        "ADMIN",
        "EDITOR",
        "READER",
      )

    every { authenticationFactory.createAuthentication(any()) } returns Optional.of(Authentication.build(expectedUserId, mockedRoles))

    val responsePublisher: Publisher<Authentication> = keycloakTokenValidator.validateToken(VALID_ACCESS_TOKEN, httpRequest)

    StepVerifier
      .create(responsePublisher)
      .expectNextMatches(Predicate { r: Authentication? -> matchSuccessfulResponse(r!!, expectedUserId, mockedRoles) })
      .verifyComplete()
  }

  @Test
  fun testInvalidTokenPassesToNextValidator() {
    val httpRequest = mockHttpRequest(VALID_ACCESS_TOKEN)

    // Mock the AirbyteKeycloakClient to throw an exception (invalid token)
    every { airbyteKeycloakClient.validateToken(VALID_ACCESS_TOKEN) } answers { InvalidTokenException("Invalid token") }

    val responsePublisher: Publisher<Authentication> = keycloakTokenValidator.validateToken(VALID_ACCESS_TOKEN, httpRequest)

    // Verify the stream remains empty (passed to next validator).
    StepVerifier
      .create(responsePublisher)
      .expectComplete()
      .verify()
  }

  @Test
  fun testExceptionDuringValidationReturnsEmpty() {
    val httpRequest = mockHttpRequest(VALID_ACCESS_TOKEN)

    // Mock the AirbyteKeycloakClient to throw an exception
    every { airbyteKeycloakClient.validateToken(VALID_ACCESS_TOKEN) } throws RuntimeException("Keycloak unavailable")

    val responsePublisher: Publisher<Authentication> = keycloakTokenValidator.validateToken(VALID_ACCESS_TOKEN, httpRequest)

    // Verify the stream remains empty (error handled gracefully).
    StepVerifier
      .create(responsePublisher)
      .expectComplete()
      .verify()
  }

  private fun mockHttpRequest(jwtToken: String): HttpRequest<*> {
    val uri = URI.create(LOCALHOST + URI_PATH)

    // set up mocked incoming request
    val httpRequest = mockk<HttpRequest<*>>()
    val headers = NettyHttpHeaders()
    headers.add(HttpHeaders.AUTHORIZATION, "Bearer $jwtToken")
    every { httpRequest.getUri() } returns uri
    every { httpRequest.getHeaders() } returns headers

    return httpRequest
  }

  private fun matchSuccessfulResponse(
    authentication: Authentication,
    expectedUserId: String?,
    expectedRoles: MutableCollection<String?>,
  ): Boolean =
    authentication.getName() == expectedUserId &&
      authentication.getRoles().containsAll(expectedRoles)

  companion object {
    private const val LOCALHOST = "http://localhost"
    private const val URI_PATH = "/some/path"
    private const val INTERNAL_REALM_NAME = "_internal"

    // Note that this token was specifically constructed to include an underscore, which was a bug in
    // production
    // due to incorrect decoding.
    private val VALID_ACCESS_TOKEN =
      """
      eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIwM095c3pkWmNrZFd6Mk84d0ZFRkZVblJPLVJrN1lGLWZzRm1kWG1Q
      bHdBIn0.eyJleHAiOjE2ODY4MTEwNTAsImlhdCI6MTY4NjgxMDg3MCwiYXV0aF90aW1lIjoxNjg2ODA3MTAzLCJqdGkiOiI1YzZhYTQ0
      Yi02ZDRlLTRkMTktOWQ0NC02YmY0ZjRlMzM5OTYiLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwMDAvYXV0aC9yZWFsbXMvbWFzdGVy
      IiwiYXVkIjpbIm1hc3Rlci1yZWFsbSIsImFjY291bnQiXSwic3ViIjoiMGYwY2JmOWEtMjRjMi00NmNjLWI1ODItZDFmZjJjMGQ1ZWY1
      IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYWlyYnl0ZS13ZWJhcHAiLCJzZXNzaW9uX3N0YXRlIjoiN2FhOTdmYTEtYTI1Mi00NmQ0LWE0
      NTMtOTE2Y2E3M2E4NmQ4IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwOi8vbG9jYWxob3N0OjgwMDAiXSwicmVhbG1f
      YWNjZXNzIjp7InJvbGVzIjpbImNyZWF0ZS1yZWFsbSIsImRlZmF1bHQtcm9sZXMtbWFzdGVyIiwib2ZmbGluZV9hY2Nlc3MiLCJhZG1p
      biIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsibWFzdGVyLXJlYWxtIjp7InJvbGVzIjpbInZpZXctaWRl
      bnRpdHktcHJvdmlkZXJzIiwidmlldy1yZWFsbSIsIm1hbmFnZS1pZGVudGl0eS1wcm92aWRlcnMiLCJpbXBlcnNvbmF0aW9uIiwiY3Jl
      YXRlLWNsaWVudCIsIm1hbmFnZS11c2VycyIsInF1ZXJ5LXJlYWxtcyIsInZpZXctYXV0aG9yaXphdGlvbiIsInF1ZXJ5LWNsaWVudHMi
      LCJxdWVyeS11c2VycyIsIm1hbmFnZS1ldmVudHMiLCJtYW5hZ2UtcmVhbG0iLCJ2aWV3LWV2ZW50cyIsInZpZXctdXNlcnMiLCJ2aWV3
      LWNsaWVudHMiLCJtYW5hZ2UtYXV0aG9yaXphdGlvbiIsIm1hbmFnZS1jbGllbnRzIiwicXVlcnktZ3JvdXBzIl19LCJhY2NvdW50Ijp7
      InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9w
      ZW5pZCBwcm9maWxlIGVtYWlsIiwic2lkIjoiN2FhOTdmYTEtYTI1Mi00NmQ0LWE0NTMtOTE2Y2E3M2E4NmQ4IiwiZW1haWxfdmVyaWZp
      ZWQiOmZhbHNlLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhZW_DqyJ9
      
      """.trimIndent().replace("\n", "").replace("\r", "")
  }
}
