/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.simple.SimpleHttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.authentication.ServerAuthentication
import io.micronaut.security.rules.SecurityRuleResult
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.ws.rs.POST
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import reactor.test.StepVerifier
import java.util.UUID

class ScopedTokensSecurityRuleTest {
  val workspaceId = UUID.randomUUID()
  lateinit var resolver: AuthenticationHeaderResolver
  lateinit var auth: Authentication

  @BeforeEach
  fun setup() {
    resolver = mockk()
    auth = ServerAuthentication("test", emptyList(), emptyMap())
  }

  @Test
  fun `basic request with missing details is ignored`() {
    val request = SimpleHttpRequest<Any>(HttpMethod.POST, "http://test/foo", null)
    verifyRule(request, SecurityRuleResult.UNKNOWN)
  }

  @Test
  fun `the health endpoint is ignored`() {
    val request = SimpleHttpRequest<Any>(HttpMethod.POST, "http://test/api/v1/health", null)
    verifyRule(request, SecurityRuleResult.UNKNOWN)
  }

  @Test
  fun `null auth is ignored`() {
    val request = SimpleHttpRequest<Any>(HttpMethod.POST, "http://test/foo", null)
    val rule = ScopedTokenSecurityRule(resolver).check(request, null)
    StepVerifier
      .create(rule)
      .expectNext(SecurityRuleResult.UNKNOWN)
      .verifyComplete()
  }

  @Test
  fun `token scope claim with invalid value is ignored`() {
    val request = SimpleHttpRequest<Any>(HttpMethod.POST, "http://test/foo", null)
    val attrs = mapOf(TokenScopeClaim.CLAIM_ID to "123")
    auth = ServerAuthentication("test", emptyList(), attrs)
    verifyRule(request, SecurityRuleResult.UNKNOWN)
  }

  @Test
  fun `unresolved workspace ID is rejected`() {
    val request = SimpleHttpRequest<Any>(HttpMethod.POST, "http://test/foo", null)
    val attrs = mapOf(TokenScopeClaim.CLAIM_ID to mapOf("workspaceId" to workspaceId.toString()))
    auth = ServerAuthentication("test", emptyList(), attrs)
    every { resolver.resolveWorkspace(any()) } returns emptyList()
    verifyRule(request, SecurityRuleResult.REJECTED)
  }

  @Test
  fun `resolving multiple workspace IDs is rejected`() {
    val request = SimpleHttpRequest<Any>(HttpMethod.POST, "http://test/foo", null)
    val attrs = mapOf(TokenScopeClaim.CLAIM_ID to mapOf("workspaceId" to workspaceId.toString()))
    auth = ServerAuthentication("test", emptyList(), attrs)
    every { resolver.resolveWorkspace(any()) } returns listOf(workspaceId, UUID.randomUUID())
    verifyRule(request, SecurityRuleResult.REJECTED)
  }

  @Test
  fun `mismatched workspace ID is rejected`() {
    val request = SimpleHttpRequest<Any>(HttpMethod.POST, "http://test/foo", null)
    val attrs = mapOf(TokenScopeClaim.CLAIM_ID to mapOf("workspaceId" to workspaceId.toString()))
    auth = ServerAuthentication("test", emptyList(), attrs)
    every { resolver.resolveWorkspace(any()) } returns listOf(UUID.randomUUID())
    verifyRule(request, SecurityRuleResult.REJECTED)
  }

  @Test
  fun `workspace resolver gets called with request headers`() {
    val request = SimpleHttpRequest<Any>(HttpMethod.POST, "http://test/foo", null)
    request.headers.add("foo", "bar")
    val attrs = mapOf(TokenScopeClaim.CLAIM_ID to mapOf("workspaceId" to workspaceId.toString()))
    auth = ServerAuthentication("test", emptyList(), attrs)
    val expectedHeaders = mapOf("foo" to "bar")
    every { resolver.resolveWorkspace(any()) } returns emptyList()
    ScopedTokenSecurityRule(resolver).check(request, auth)
    verify { resolver.resolveWorkspace(expectedHeaders) }
  }

  @Test
  fun `matching scope and resolved workspace ID is unknown`() {
    val request = SimpleHttpRequest<Any>(HttpMethod.POST, "http://test/foo", null)
    val attrs = mapOf(TokenScopeClaim.CLAIM_ID to mapOf("workspaceId" to workspaceId.toString()))
    auth = ServerAuthentication("test", emptyList(), attrs)
    every { resolver.resolveWorkspace(any()) } returns listOf(workspaceId)
    verifyRule(request, SecurityRuleResult.UNKNOWN)
  }

  private fun verifyRule(
    request: HttpRequest<*>,
    expect: SecurityRuleResult,
  ) {
    val rule = ScopedTokenSecurityRule(resolver).check(request, auth)
    StepVerifier
      .create(rule)
      .expectNext(expect)
      .verifyComplete()
  }
}
