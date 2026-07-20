/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.airbyte.commons.server.support.AuthenticationHttpHeaders
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimAccessDeniedException
import io.airbyte.domain.models.scim.ScimAuthenticationException
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimAuthenticationService
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

private const val SCIM_AUTH_SPEC = "ScimAuthenticationFilterTest"
private const val SCIM_ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"
private const val SCIM_CONTENT_TYPE = "application/scim+json"

@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SCIM_AUTH_SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
class ScimAuthenticationFilterTest {
  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var authenticationService: ScimAuthenticationService

  private val rawToken = "airbyte_scim_${"a".repeat(64)}"
  private val context =
    ScimAuthenticationContext(
      configurationId = UUID.randomUUID(),
      organizationId = OrganizationId(UUID.randomUUID()),
      tokenHash = "b".repeat(64),
    )

  @BeforeEach
  fun setUp() {
    clearMocks(authenticationService)
  }

  @Test
  fun `authenticated request carries explicit context and hides authorization downstream`() {
    every { authenticationService.authenticate(rawToken) } returns context

    val response =
      client.toBlocking().exchange(
        HttpRequest.GET<Any>("/scim/v2/Test?organizationId=${UUID.randomUUID()}").bearerAuth(rawToken),
        Map::class.java,
      )

    assertEquals(HttpStatus.OK, response.status)
    assertEquals(context.configurationId.toString(), response.body()["configurationId"])
    assertEquals(context.organizationId.value.toString(), response.body()["organizationId"])
    assertEquals(false, response.body()["authorizationVisible"])
  }

  @Test
  fun `authentication executes off the Netty event loop`() {
    val authenticationThreadName = AtomicReference<String>()
    every { authenticationService.authenticate(rawToken) } answers {
      authenticationThreadName.set(Thread.currentThread().name)
      context
    }

    val response =
      client.toBlocking().exchange(
        HttpRequest.GET<Any>("/scim/v2/Test").bearerAuth(rawToken),
        Map::class.java,
      )

    assertEquals(HttpStatus.OK, response.status)
    assertFalse(
      authenticationThreadName.get().contains("eventLoopGroup"),
      "Expected authentication off the Netty event loop, but ran on ${authenticationThreadName.get()}",
    )
  }

  @Test
  fun `base SCIM path is authenticated`() {
    every { authenticationService.authenticate(rawToken) } returns context

    val response =
      client.toBlocking().exchange(
        HttpRequest.GET<Any>("/scim/v2").bearerAuth(rawToken),
        Map::class.java,
      )

    assertEquals(context.organizationId.value.toString(), response.body()["organizationId"])
  }

  @Test
  fun `bearer scheme is case insensitive and accepts one or more spaces`() {
    every { authenticationService.authenticate(rawToken) } returns context

    val response =
      client.toBlocking().exchange(
        HttpRequest
          .GET<Any>("/scim/v2/Test")
          .header(HttpHeaders.AUTHORIZATION, "bEaReR    $rawToken"),
        Map::class.java,
      )

    assertEquals(HttpStatus.OK, response.status)
    verify(exactly = 1) { authenticationService.authenticate(rawToken) }
  }

  @Test
  fun `missing or malformed authorization returns a generic SCIM 401`() {
    val requests =
      listOf(
        HttpRequest.GET<Any>("/scim/v2/Test"),
        HttpRequest.GET<Any>("/scim/v2/Test").header(HttpHeaders.AUTHORIZATION, "Basic credentials"),
        HttpRequest.GET<Any>("/scim/v2/Test").header(HttpHeaders.AUTHORIZATION, "Bearer"),
        HttpRequest.GET<Any>("/scim/v2/Test").header(HttpHeaders.AUTHORIZATION, "Bearer\t$rawToken"),
      )

    requests.forEach { request ->
      assertScimError(exchangeError(request), HttpStatus.UNAUTHORIZED, "Invalid bearer token", bearerChallenge = true)
    }
    verify(exactly = 0) { authenticationService.authenticate(any()) }
  }

  @Test
  fun `multiple authorization headers return a generic SCIM 401`() {
    val request = HttpRequest.GET<Any>("/scim/v2/Test")
    request.headers.add(HttpHeaders.AUTHORIZATION, "Bearer $rawToken")
    request.headers.add(HttpHeaders.AUTHORIZATION, "Bearer $rawToken")

    assertScimError(exchangeError(request), HttpStatus.UNAUTHORIZED, "Invalid bearer token", bearerChallenge = true)
    verify(exactly = 0) { authenticationService.authenticate(any()) }
  }

  @Test
  fun `unknown rotated disabled or revoked token returns a generic SCIM 401`() {
    every { authenticationService.authenticate(rawToken) } throws ScimAuthenticationException()

    assertScimError(
      exchangeError(HttpRequest.GET<Any>("/scim/v2/Test").bearerAuth(rawToken)),
      HttpStatus.UNAUTHORIZED,
      "Invalid bearer token",
      bearerChallenge = true,
    )
  }

  @Test
  fun `gate denial returns SCIM 403 without a bearer challenge`() {
    every { authenticationService.authenticate(rawToken) } throws ScimAccessDeniedException("SCIM access is denied")

    assertScimError(
      exchangeError(HttpRequest.GET<Any>("/scim/v2/Test").bearerAuth(rawToken)),
      HttpStatus.FORBIDDEN,
      "SCIM access is denied",
      bearerChallenge = false,
    )
  }

  @Test
  fun `unexpected authentication infrastructure failures remain 500`() {
    every { authenticationService.authenticate(rawToken) } throws IllegalStateException("database unavailable")

    val error = exchangeError(HttpRequest.GET<Any>("/scim/v2/Test").bearerAuth(rawToken))

    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, error.status)
    assertNull(error.response.headers[HttpHeaders.WWW_AUTHENTICATE])
  }

  @Test
  fun `downstream stale authentication failure uses the same SCIM 401 contract`() {
    every { authenticationService.authenticate(rawToken) } returns context

    assertScimError(
      exchangeError(HttpRequest.GET<Any>("/scim/v2/Test/reject").bearerAuth(rawToken)),
      HttpStatus.UNAUTHORIZED,
      "Invalid bearer token",
      bearerChallenge = true,
    )
  }

  @Test
  fun `request supplied tenant identifiers cannot replace authenticated context`() {
    every { authenticationService.authenticate(rawToken) } returns context

    val response =
      client.toBlocking().exchange(
        HttpRequest
          .POST(
            "/scim/v2/Test/${UUID.randomUUID()}?organizationId=${UUID.randomUUID()}",
            mapOf("organizationId" to UUID.randomUUID().toString()),
          ).bearerAuth(rawToken)
          .header(AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER, UUID.randomUUID().toString())
          .contentType(MediaType.APPLICATION_JSON_TYPE),
        Map::class.java,
      )

    assertEquals(context.configurationId.toString(), response.body()["configurationId"])
    assertEquals(context.organizationId.value.toString(), response.body()["organizationId"])
  }

  @Test
  fun `SCIM token cannot authenticate a normal API route`() {
    val error =
      exchangeError(
        HttpRequest.GET<Any>("/api/v1/scim-auth-test").bearerAuth(rawToken),
      )

    assertEquals(HttpStatus.UNAUTHORIZED, error.status)
    verify(exactly = 0) { authenticationService.authenticate(any()) }
  }

  private fun <I> exchangeError(request: HttpRequest<I>): HttpClientResponseException =
    assertThrows {
      client.toBlocking().exchange<I, Any>(request)
    }

  private fun assertScimError(
    error: HttpClientResponseException,
    status: HttpStatus,
    detail: String,
    bearerChallenge: Boolean,
  ) {
    assertEquals(status, error.status)
    assertEquals(
      SCIM_CONTENT_TYPE,
      error.response.contentType
        .orElseThrow()
        .name,
    )
    val body = error.response.getBody(Map::class.java).orElseThrow()
    assertEquals(listOf(SCIM_ERROR_SCHEMA), body["schemas"])
    assertEquals(status.code.toString(), body["status"])
    assertEquals(detail, body["detail"])
    if (bearerChallenge) {
      assertEquals("Bearer", error.response.headers[HttpHeaders.WWW_AUTHENTICATE])
    } else {
      assertFalse(error.response.headers.contains(HttpHeaders.WWW_AUTHENTICATE))
    }
  }
}

@Requires(property = "spec.name", value = SCIM_AUTH_SPEC)
@Controller
@Secured(SecurityRule.IS_ANONYMOUS)
class ScimAuthenticationTestController {
  @Get("/scim/v2")
  fun base(request: HttpRequest<*>): Map<String, Any> = contextResponse(request)

  @Get("/scim/v2/Test")
  fun get(request: HttpRequest<*>): Map<String, Any> = contextResponse(request)

  @Post("/scim/v2/Test/{ignoredOrganizationId}")
  fun post(
    request: HttpRequest<*>,
    ignoredOrganizationId: UUID,
  ): Map<String, Any> = contextResponse(request)

  @Get("/scim/v2/Test/reject")
  fun reject(): Nothing = throw ScimAuthenticationException()

  @Get("/api/v1/scim-auth-test")
  @Secured(SecurityRule.IS_AUTHENTICATED)
  fun normalApi(): String = "authenticated"

  private fun contextResponse(request: HttpRequest<*>): Map<String, Any> {
    val context = request.scimAuthenticationContext()
    return mapOf(
      "configurationId" to context.configurationId.toString(),
      "organizationId" to context.organizationId.value.toString(),
      "authorizationVisible" to request.headers.contains(HttpHeaders.AUTHORIZATION),
    )
  }
}

@Requires(property = "spec.name", value = SCIM_AUTH_SPEC)
@Factory
class ScimAuthenticationTestBeans {
  @Singleton
  @Primary
  fun scimAuthenticationService(): ScimAuthenticationService = mockk()
}
