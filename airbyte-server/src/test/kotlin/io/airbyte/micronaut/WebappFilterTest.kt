/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut

import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.filter.ServerFilterChain
import io.micronaut.http.netty.NettyHttpHeaders
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.util.Optional

class WebappFilterTest {
  @Test
  fun `pathPrefixes are unchanged`() {
    // Given the importance of these prefixes, this silly looking test is here to double-check that any changes to the pathPrefixes was made
    // intentionally.
    assertEquals(4, pathPrefixes.size)
    assertTrue(pathPrefixes.all { it.startsWith("/") }, "path prefixes must start with a /s")

    assertTrue(pathPrefixes.contains("/api"), "api path prefix missing")
    assertTrue(pathPrefixes.contains("/assets"), "assets path prefix missing")
    assertTrue(pathPrefixes.contains("/fonts"), "fonts path prefix missing")
    assertTrue(pathPrefixes.contains("/routes"), "routes path prefix missing")
  }

  @Test
  fun `webappContent is defined`() {
    // as webapp content is dynamic, ensure it's not empty
    assertTrue(webappContents.isNotEmpty(), "webappContents must not be empty")
    assertTrue(webappContents.all { it.startsWith("/") }, "webapp contents must start with a /")
  }

  @Test
  fun `pathPrefixes do not redirect to index-html`() {
    val response = mockk<MutableHttpResponse<*>> {}
    val requestSlot = slot<HttpRequest<*>>()
    val chain = mockk<ServerFilterChain> { every { proceed(capture(requestSlot)) } returns Mono.just(response) }

    val filter = WebappFilter()
    val headers = NettyHttpHeaders().apply { add("dummy-header", "dummy-value") }

    // request each known path, verify the chain is called with the request
    pathPrefixes.forEach { prefix ->
      val request =
        mockk<HttpRequest<*>> {
          every { path } returns "$prefix/more-path"
          every { method } returns HttpMethod.GET
          every { this@mockk.headers } returns headers
          every { body } returns Optional.of("dummy-body")
        }

      StepVerifier
        .create(filter.doFilter(request, chain))
        .expectNext(response)
        .expectComplete()
        .verify()

      assertEquals("$prefix/more-path", requestSlot.captured.path, prefix)
      assertEquals(request.method, requestSlot.captured.method, prefix)
      // provided header is there
      assertEquals(headers.get("dummy-header"), requestSlot.captured.headers.get("dummy-header"), prefix)
      // no CSP header was added
      assertNull(requestSlot.captured.headers.get(CSP_HEADER), prefix)
      assertEquals(request.body.get(), requestSlot.captured.body.get(), prefix)
    }
  }

  @Test
  fun `non matching paths return index-html`() {
    val request = mockk<HttpRequest<*>>(relaxed = true) { every { path } returns "/workspaces/imagine-random-uuid-here" }
    val requestSlot = slot<HttpRequest<*>>()
    val response =
      mockk<MutableHttpResponse<*>> {
        every { header(CSP_HEADER, CSP_VALUE) } returns this@mockk
      }
    val chain = mockk<ServerFilterChain> { every { proceed(capture(requestSlot)) } returns Mono.just(response) }

    val filter = WebappFilter()

    StepVerifier
      .create(filter.doFilter(request, chain))
      .expectNext(response)
      .expectComplete()
      .verify()

    assertEquals(PATH_INDEX_REDIRECT, requestSlot.captured.path)
    verify { response.header(CSP_HEADER, CSP_VALUE) }
  }

  @Test
  fun `web content request does not redirect to index-html`() {
    val existingFile = "/index.css"
    val request = mockk<HttpRequest<*>>(relaxed = true) { every { path } returns existingFile }
    val requestSlot = slot<HttpRequest<*>>()
    val response =
      mockk<MutableHttpResponse<*>> {
        every { header(CSP_HEADER, CSP_VALUE) } returns this@mockk
      }
    val chain = mockk<ServerFilterChain> { every { proceed(capture(requestSlot)) } returns Mono.just(response) }

    val filter = WebappFilter()

    StepVerifier
      .create(filter.doFilter(request, chain))
      .expectNext(response)
      .expectComplete()
      .verify()

    assertEquals(existingFile, requestSlot.captured.path)
    verify { response.header(CSP_HEADER, CSP_VALUE) }
  }

  @Test
  fun `auth_flow redirects correctly`() {
    val request = mockk<HttpRequest<*>>(relaxed = true) { every { path } returns PATH_AUTH_FLOW }
    val requestSlot = slot<HttpRequest<*>>()
    val response =
      mockk<MutableHttpResponse<*>> {
        every { header(CSP_HEADER, CSP_VALUE) } returns this@mockk
      }
    val chain = mockk<ServerFilterChain> { every { proceed(capture(requestSlot)) } returns Mono.just(response) }

    val filter = WebappFilter()

    StepVerifier
      .create(filter.doFilter(request, chain))
      .expectNext(response)
      .expectComplete()
      .verify()

    assertEquals(PATH_AUTH_FLOW_REDIRECT, requestSlot.captured.path)
    verify { response.header(CSP_HEADER, CSP_VALUE) }
  }

  @Test
  fun `HttpRequest copy`() {
    val orig =
      HttpRequest.create<String>(HttpMethod.PATCH, "/path-original").apply {
        header("header-key", "header-value")
        body("body-content")
      }

    val copy = orig.copy("/path-new")
    assertEquals("/path-new", copy.path)
    assertEquals(orig.method, copy.method)
    assertEquals(orig.headers.get("header-key"), copy.headers.get("header-key"))
    assertEquals(orig.body, copy.body)
  }
}
