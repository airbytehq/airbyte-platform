/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.filters

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LoadShedPublicApi
import io.airbyte.featureflag.TokenSubject
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.filter.ServerFilterChain
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.security.Principal
import java.util.Optional

class PublicApiLoadShedFilterTest {
  private val tokenSubject = "460c8eef-3e73-44ac-ae06-8972afc70e9a"
  private val metricClient = mockk<MetricClient>(relaxed = true)

  private fun filter(ff: FeatureFlagClient) = PublicApiLoadShedFilter(ff, metricClient)

  private fun request(principalName: String?): HttpRequest<*> =
    mockk<HttpRequest<*>>(relaxed = true) {
      every { method } returns HttpMethod.GET
      every { path } returns "/api/public/v1/connections"
      every { userPrincipal } returns Optional.ofNullable(principalName?.let { Principal { it } })
    }

  private fun chain(response: MutableHttpResponse<*>): ServerFilterChain = mockk { every { proceed(any()) } returns Mono.just(response) }

  @Test
  fun `sheds request with a 429 and no Retry-After when flag targets the token subject`() {
    val ff = mockk<FeatureFlagClient> { every { boolVariation(LoadShedPublicApi, TokenSubject(tokenSubject)) } returns true }
    val chain = chain(mockk())

    val result =
      Mono
        .from(filter(ff).doFilter(request(tokenSubject), chain))
        .block()!!

    assertEquals(HttpStatus.TOO_MANY_REQUESTS, result.status)
    // hard rate limit: we deliberately do NOT send Retry-After, so clients aren't told to retry
    assertNull(result.headers.get(HttpHeaders.RETRY_AFTER))
    // shed requests must not reach the downstream chain (they never consume a public-api thread)
    verify(exactly = 0) { chain.proceed(any()) }
    // and the shed is counted, tagged with the subject so Datadog shows who is throttled
    verify(exactly = 1) {
      metricClient.count(OssMetricsRegistry.PUBLIC_API_LOAD_SHED, 1L, MetricAttribute(MetricTags.TOKEN_SUBJECT, tokenSubject))
    }
  }

  @Test
  fun `proceeds and does not count when flag is off for the token subject`() {
    val response = mockk<MutableHttpResponse<*>>()
    val ff = mockk<FeatureFlagClient> { every { boolVariation(LoadShedPublicApi, TokenSubject(tokenSubject)) } returns false }

    StepVerifier
      .create(filter(ff).doFilter(request(tokenSubject), chain(response)))
      .expectNext(response)
      .expectComplete()
      .verify()

    verify { metricClient wasNot Called }
  }

  @Test
  fun `proceeds without evaluating the flag when there is no authenticated principal`() {
    val response = mockk<MutableHttpResponse<*>>()
    val ff = mockk<FeatureFlagClient>()

    StepVerifier
      .create(filter(ff).doFilter(request(null), chain(response)))
      .expectNext(response)
      .expectComplete()
      .verify()

    verify(exactly = 0) { ff.boolVariation(any(), any()) }
  }

  @Test
  fun `fails open and proceeds when flag evaluation throws`() {
    val response = mockk<MutableHttpResponse<*>>()
    val ff =
      mockk<FeatureFlagClient> {
        every { boolVariation(LoadShedPublicApi, TokenSubject(tokenSubject)) } throws RuntimeException("ff down")
      }

    StepVerifier
      .create(filter(ff).doFilter(request(tokenSubject), chain(response)))
      .expectNext(response)
      .expectComplete()
      .verify()
  }
}
