/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import com.auth0.jwt.algorithms.Algorithm
import com.nimbusds.jwt.JWT
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.airbyte.commons.auth.roles.AuthRole.Companion.getInstanceAdminRoles
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.data.auth.TokenType
import io.airbyte.data.auth.TokenType.Companion.fromClaims
import io.airbyte.metrics.MetricClient
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.netty.NettyHttpHeaders
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.jwt.validator.JwtAuthenticationFactory
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Response
import okhttp3.ResponseBody
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.mockito.kotlin.anyOrNull
import org.reactivestreams.Publisher
import reactor.test.StepVerifier
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.util.Optional
import java.util.UUID
import java.util.function.Predicate

internal class KeycloakTokenValidatorTest {
  private lateinit var keycloakTokenValidator: KeycloakTokenValidator
  private lateinit var httpClient: OkHttpClient
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfiguration
  private lateinit var authenticationFactory: JwtAuthenticationFactory

  @BeforeEach
  fun setUp() {
    // Make sure we're covering the case where the token contains an underscore, since this used to
    // break in production.
    assert(VALID_ACCESS_TOKEN.contains("_"))

    httpClient = mock()

    keycloakConfiguration = mock()
    Mockito
      .`when`(keycloakConfiguration.getKeycloakUserInfoEndpointForRealm(anyOrNull()))
      .thenReturn(LOCALHOST + URI_PATH)
    Mockito.`when`(keycloakConfiguration.internalRealm).thenReturn(INTERNAL_REALM_NAME)
    authenticationFactory = mock()

    keycloakTokenValidator =
      KeycloakTokenValidator(httpClient, keycloakConfiguration, authenticationFactory, Optional.empty<MetricClient>())
  }

  @Test
  @Throws(Exception::class)
  fun testValidateToken() {
    val expectedUserId = "0f0cbf9a-24c2-46cc-b582-d1ff2c0d5ef5"
    val responseBody = "{\"sub\":\"0f0cbf9a-24c2-46cc-b582-d1ff2c0d5ef5\",\"preferred_username\":\"airbyte\"}"
    val httpRequest = mockRequests(VALID_ACCESS_TOKEN, responseBody)

    val responsePublisher: Publisher<Authentication> = keycloakTokenValidator.validateToken(VALID_ACCESS_TOKEN, httpRequest)

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

    Mockito
      .`when`(authenticationFactory.createAuthentication(anyOrNull()))
      .thenReturn(Optional.of(Authentication.build(expectedUserId, mockedRoles)))

    StepVerifier
      .create(responsePublisher)
      .expectNextMatches(Predicate { r: Authentication? -> matchSuccessfulResponse(r!!, expectedUserId, mockedRoles) })
      .verifyComplete()
  }

  @Test
  @Throws(Exception::class)
  fun testInternalServiceAccountIsInstanceAdmin() {
    val sub = UUID.randomUUID().toString()
    val issuer = "/auth/realms/" + INTERNAL_REALM_NAME
    val clientName = "airbyte-workload-client"
    val accessToken =
      com.auth0.jwt.JWT
        .create()
        .withSubject(sub)
        .withIssuer(issuer)
        .withClaim("azp", clientName)
        .sign(Algorithm.none())

    val responseBody = serialize(mapOf("sub" to sub, "iss" to issuer, "azp" to clientName))
    val httpRequest = mockRequests(accessToken, responseBody)

    val responsePublisher: Publisher<Authentication> = keycloakTokenValidator.validateToken(accessToken, httpRequest)

    Mockito.verifyNoInteractions(authenticationFactory)

    StepVerifier
      .create(responsePublisher)
      .expectNextMatches(Predicate { r: Authentication? -> matchSuccessfulResponseServiceAccount(r!!, clientName, getInstanceAdminRoles()) })
      .verifyComplete()
  }

  @Test
  @Throws(Exception::class)
  fun testKeycloakValidationFailureNoSubClaim() {
    val httpRequest = mockRequests(VALID_ACCESS_TOKEN, "{\"preferred_username\":\"airbyte\"}")

    val responsePublisher: Publisher<Authentication> = keycloakTokenValidator.validateToken(VALID_ACCESS_TOKEN, httpRequest)

    // Verify the stream remains empty.
    StepVerifier
      .create(responsePublisher)
      .expectComplete()
      .verify()
  }

  @Test
  @Throws(URISyntaxException::class)
  fun testTokenWithNoRealmIsPassedToNextValidator() {
    val uri = URI(LOCALHOST + URI_PATH)

    val blankJWT =
      com.auth0.jwt.JWT
        .create()
        .sign(Algorithm.none())

    // set up mocked incoming request
    val httpRequest = mock<HttpRequest<*>>()
    val headers = NettyHttpHeaders()
    headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + blankJWT)
    Mockito.`when`(httpRequest.getUri()).thenReturn(uri)
    Mockito.`when`(httpRequest.getHeaders()).thenReturn(headers)

    val responsePublisher: Publisher<Authentication> = keycloakTokenValidator.validateToken(blankJWT, httpRequest)
    StepVerifier
      .create(responsePublisher)
      .verifyComplete()
  }

  @Throws(IOException::class)
  private fun mockRequests(
    jwtToken: String?,
    userInfoPayload: String?,
  ): HttpRequest<*> {
    val uri = URI.create(LOCALHOST + URI_PATH)

    // set up mocked incoming request
    val httpRequest = mock<HttpRequest<*>>()
    val headers = NettyHttpHeaders()
    headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + jwtToken)
    Mockito.`when`(httpRequest.getUri()).thenReturn(uri)
    Mockito.`when`(httpRequest.getHeaders()).thenReturn(headers)

    // set up mock http response from Keycloak
    val userInfoResponse = mock<Response>()
    val userInfoResponseBody = mock<ResponseBody>()
    Mockito.`when`(userInfoResponseBody.string()).thenReturn(userInfoPayload)
    Mockito.`when`(userInfoResponse.body).thenReturn(userInfoResponseBody)
    Mockito.`when`(userInfoResponse.code).thenReturn(200)
    Mockito.`when`(userInfoResponse.isSuccessful).thenReturn(true)

    val call = mock<Call>()
    Mockito.`when`(call.execute()).thenReturn(userInfoResponse)
    Mockito.`when`(httpClient.newCall(anyOrNull())).thenReturn(call)

    return httpRequest
  }

  private fun matchSuccessfulResponse(
    authentication: Authentication,
    expectedUserId: String?,
    expectedRoles: MutableCollection<String?>,
  ): Boolean =
    authentication.getName() == expectedUserId &&
      authentication.getRoles().containsAll(expectedRoles)

  private fun matchSuccessfulResponseServiceAccount(
    authentication: Authentication,
    expectedUserId: String?,
    expectedRoles: Set<String?>,
  ): Boolean =
    authentication.getName() == expectedUserId &&
      authentication.getRoles().containsAll(expectedRoles) &&
      fromClaims(authentication.getAttributes()) == TokenType.LEGACY_KEYCLOAK_SERVICE_ACCOUNT

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
