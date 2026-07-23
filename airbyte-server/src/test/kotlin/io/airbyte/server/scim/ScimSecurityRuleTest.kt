/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.micronaut.http.HttpRequest
import io.micronaut.security.rules.SecurityRuleResult
import org.junit.jupiter.api.Test
import reactor.test.StepVerifier

class ScimSecurityRuleTest {
  private val rule = ScimSecurityRule()

  @Test
  fun `non-SCIM requests remain unknown`() {
    verifyRule(HttpRequest.GET<Any>("/api/v1/workspaces"), SecurityRuleResult.UNKNOWN)
  }

  @Test
  fun `SCIM requests without authentication context are rejected`() {
    listOf("/scim/v2", "/scim/v2/ServiceProviderConfig").forEach { path ->
      verifyRule(HttpRequest.GET<Any>(path), SecurityRuleResult.REJECTED)
    }
  }

  @Test
  fun `SCIM requests with authentication context are allowed`() {
    val request = HttpRequest.GET<Any>("/scim/v2/ServiceProviderConfig")
    request.setAttribute(SCIM_AUTHENTICATION_ATTRIBUTE, Any())

    verifyRule(request, SecurityRuleResult.ALLOWED)
  }

  private fun verifyRule(
    request: HttpRequest<*>,
    expected: SecurityRuleResult,
  ) {
    StepVerifier
      .create(rule.check(request, null))
      .expectNext(expected)
      .verifyComplete()
  }
}
