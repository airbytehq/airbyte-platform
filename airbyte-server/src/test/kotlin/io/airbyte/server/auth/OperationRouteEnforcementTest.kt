/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.security.annotation.Secured
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.rules.SecurityRule
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class OperationRouteEnforcementTest {
  @Test
  fun `a real class-bound checker allows SHARED coverage`() {
    operationContext(checkerEnabled = true).start().use { context ->
      val inventory = context.getBean(RouteAccessInventory::class.java)
      val route = inventory.routes().first { it.httpMethodName == "GET" && it.uriMatchTemplate.toString().endsWith("/read") }
      assertTrue(inventory.violations(route).isEmpty(), inventory.violations(route).joinToString())
    }
  }

  @Test
  fun `actual startup accepts verified SHARED routes`() {
    operationContext(checkerEnabled = true, validateStartup = true).use { context -> assertDoesNotThrow { context.start() } }
  }

  @Test
  fun `a missing checker fails actual startup`() {
    operationContext(validateStartup = true).use { context ->
      val failure = assertThrows(RuntimeException::class.java) { context.start() }
      val messages = generateSequence<Throwable>(failure) { it.cause }.joinToString("\n") { it.message.orEmpty() }
      assertTrue(messages.contains("checker"), messages)
    }
  }

  @Test
  fun `a missing DI rule fails actual startup`() {
    operationContext(checkerEnabled = true, validateStartup = true, invalidRoutes = true).use { context ->
      val failure = assertThrows(RuntimeException::class.java) { context.start() }
      val messages = generateSequence<Throwable>(failure) { it.cause }.joinToString("\n") { it.message.orEmpty() }
      assertTrue(messages.contains("concrete DI rule"), messages)
    }
  }

  @Test
  fun `rules that are not DI beans never count as SHARED enforcement`() {
    operationContext(checkerEnabled = true, validateStartup = false, invalidRoutes = true).start().use { context ->
      val inventory = context.getBean(RouteAccessInventory::class.java)
      val route = inventory.routes().first { it.httpMethodName == "GET" && it.uriMatchTemplate.toString().endsWith("/non-bean") }
      assertTrue(inventory.violations(route).isNotEmpty())
    }
  }
}

class OperationNonBeanRule : OperationRule {
  override fun readInput(args: Array<out Any?>) = OperationFixtureInput(args.single() as UUID)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = Unit
}

@Controller("/api/v1/invalid-operation-fixture")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
@Requires(property = "test.invalid-routes", value = "true")
class OperationInvalidController {
  @Get("/non-bean")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationNonBeanRule::class)
  fun nonBean() = "unbound"
}
