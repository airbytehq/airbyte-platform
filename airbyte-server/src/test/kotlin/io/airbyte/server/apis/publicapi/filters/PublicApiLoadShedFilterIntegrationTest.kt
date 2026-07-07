/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.filters

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LoadShedPublicApi
import io.airbyte.featureflag.TokenSubject
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.security.annotation.Secured
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.rules.SecurityRule
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

private const val SPEC = "PublicApiLoadShedFilterIntegrationTest"
private const val TEST_SUBJECT = "460c8eef-3e73-44ac-ae06-8972afc70e9a"

/**
 * End-to-end proof that [PublicApiLoadShedFilter] is discovered, ordered after Micronaut's real
 * [io.micronaut.security.filters.SecurityFilter], and sheds a matched caller with a real HTTP 429.
 *
 * Security is enabled and a test [AuthenticationFetcher] authenticates every request as
 * [TEST_SUBJECT], so `request.userPrincipal` is populated by the real security machinery — exactly
 * what the filter reads. The feature-flag client is a mock we drive per-test.
 */
@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
class PublicApiLoadShedFilterIntegrationTest {
  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @AfterEach
  fun resetFlagClient() {
    clearMocks(LoadShedTestBeans.featureFlagClient)
  }

  @Test
  fun `sheds public API request with 429 when the flag targets the caller`() {
    every { LoadShedTestBeans.featureFlagClient.boolVariation(LoadShedPublicApi, TokenSubject(TEST_SUBJECT)) } returns true

    val ex =
      assertThrows<HttpClientResponseException> {
        client.toBlocking().exchange<Any, Any>(HttpRequest.GET("/api/public/v1/ping"))
      }

    assertEquals(HttpStatus.TOO_MANY_REQUESTS, ex.status)
    // hard rate limit: no Retry-After, we don't invite a retry
    assertEquals(null, ex.response.headers.get("Retry-After"))
    // the caller actually receives the rate-limit message body
    assertEquals(LOAD_SHED_MESSAGE, ex.response.getBody(Map::class.java).orElse(emptyMap<Any, Any>())["message"])
  }

  @Test
  fun `allows the public API request when the flag is off`() {
    every { LoadShedTestBeans.featureFlagClient.boolVariation(LoadShedPublicApi, TokenSubject(TEST_SUBJECT)) } returns false

    val response = client.toBlocking().exchange(HttpRequest.GET<Any>("/api/public/v1/ping"), String::class.java)

    assertEquals(HttpStatus.OK, response.status)
    assertEquals("pong", response.body())
  }

  @Test
  fun `does not shed non-public API paths even when the flag is on for the caller`() {
    every { LoadShedTestBeans.featureFlagClient.boolVariation(LoadShedPublicApi, TokenSubject(TEST_SUBJECT)) } returns true

    // The filter is scoped to /api/public/** — an internal /api/v1 path must be untouched.
    val response = client.toBlocking().exchange(HttpRequest.GET<Any>("/api/v1/ping"), String::class.java)

    assertEquals(HttpStatus.OK, response.status)
  }
}

/** Authenticates every request as [TEST_SUBJECT] so the real SecurityFilter populates the principal. */
@Requires(property = "spec.name", value = SPEC)
@Singleton
class FixedSubjectAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> = Flux.just(Authentication.build(TEST_SUBJECT))
}

@Requires(property = "spec.name", value = SPEC)
@Controller
@Secured(SecurityRule.IS_ANONYMOUS)
class LoadShedPingController {
  @Get("/api/public/v1/ping")
  fun publicPing(): String = "pong"

  @Get("/api/v1/ping")
  fun internalPing(): String = "pong"
}

@Requires(property = "spec.name", value = SPEC)
@Factory
class LoadShedTestBeans {
  @Singleton
  @Primary
  fun featureFlagClient(): FeatureFlagClient = Companion.featureFlagClient

  companion object {
    val featureFlagClient: FeatureFlagClient = mockk(relaxed = true)
  }
}
