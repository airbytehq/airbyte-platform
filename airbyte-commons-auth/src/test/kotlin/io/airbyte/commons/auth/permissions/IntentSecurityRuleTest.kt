/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.permissions

import io.airbyte.commons.auth.generated.Intent
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.http.HttpAttributes
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.rules.SecuredAnnotationRule
import io.micronaut.security.rules.SecurityRuleResult
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.web.router.RouteMatch
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import reactor.test.StepVerifier
import java.util.Optional

val mockRoles = setOf("role-1", "role-2")

@MicronautTest
internal class IntentSecurityRuleTest {
  private lateinit var intentSecurityRule: IntentSecurityRule
  private lateinit var routeMatch: RouteMatch<*>
  private lateinit var request: HttpRequest<*>
  private lateinit var authentication: Authentication

  @BeforeEach
  fun setup() {
    intentSecurityRule = IntentSecurityRule()

    routeMatch = mockk()
    every { routeMatch.isAnnotationPresent(RequiresIntent::class.java) } returns true
    every {
      routeMatch.getAnnotation(RequiresIntent::class.java)
    } returns AnnotationValue.builder(RequiresIntent::class.java).value(Intent.UploadCustomConnector.name).build()

    request = mockk()
    every { request.getAttribute(HttpAttributes.ROUTE_MATCH, RouteMatch::class.java) } returns Optional.of(routeMatch)

    authentication = mockk()
    every { authentication.roles } returns mockRoles

    mockkObject(Intent.UploadCustomConnector)
    every { Intent.UploadCustomConnector.roles } returns mockRoles
  }

  @Test
  fun `order is below micronaut security @Secured annotation`() {
    assert(intentSecurityRule.order < SecuredAnnotationRule.ORDER)
  }

  @Test
  fun `check returns UKNOWN if annotation not present on route`() {
    every { routeMatch.isAnnotationPresent(RequiresIntent::class.java) } returns false

    val result = intentSecurityRule.check(request, authentication)
    StepVerifier
      .create(result)
      .expectNext(SecurityRuleResult.UNKNOWN)
      .verifyComplete()
  }

  @Test
  fun `check returns UKNOWN if user has no roles`() {
    every { authentication.roles } returns null

    val result = intentSecurityRule.check(request, authentication)
    StepVerifier
      .create(result)
      .expectNext(SecurityRuleResult.UNKNOWN)
      .verifyComplete()
  }

  @Test
  fun `check returns REJECTED if specified intent has no roles`() {
    every { Intent.UploadCustomConnector.roles } returns emptySet()

    val result = intentSecurityRule.check(request, authentication)
    StepVerifier
      .create(result)
      .expectNext(SecurityRuleResult.REJECTED)
      .verifyComplete()
  }

  @Test
  fun `check returns ALLOWED if user has a matching role`() {
    val validRole = mockRoles.first()
    every { authentication.roles } returns listOf(validRole)

    val result = intentSecurityRule.check(request, authentication)
    StepVerifier
      .create(result)
      .expectNext(SecurityRuleResult.ALLOWED)
      .verifyComplete()
  }

  @Test
  fun `check returns REJECTED if user has no matching roles`() {
    every { authentication.roles } returns listOf("useless-role-1", "useless-role-2")

    val result = intentSecurityRule.check(request, authentication)
    StepVerifier
      .create(result)
      .expectNext(SecurityRuleResult.REJECTED)
      .verifyComplete()
  }

  @Test
  internal fun `check returns UNKNOWN if no route match attribute`() {
    every { request.getAttribute(HttpAttributes.ROUTE_MATCH, RouteMatch::class.java) } returns Optional.empty()

    val result = intentSecurityRule.check(request, authentication)
    StepVerifier
      .create(result)
      .expectNext(SecurityRuleResult.UNKNOWN)
      .verifyComplete()
  }
}
