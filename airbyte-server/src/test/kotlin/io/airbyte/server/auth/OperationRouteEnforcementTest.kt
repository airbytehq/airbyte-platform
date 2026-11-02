/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.PropertySource
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.annotation.Secured
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.rules.SecurityRule
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

class OperationRouteEnforcementTest {
  @ParameterizedTest
  @ValueSource(strings = ["missing", "empty", "role", "mixed"])
  fun `SHARED routes without authentication-only security fail actual startup`(metadata: String) {
    for (securityEnabled in listOf(true, false)) {
      operationContext(checkerEnabled = true, securityEnabled = securityEnabled).use { context ->
        context.environment.addPropertySource(PropertySource.of("shared-metadata", mapOf("test.shared-metadata" to metadata)))
        val failure = assertThrows(RuntimeException::class.java) { context.start() }
        val messages = generateSequence<Throwable>(failure) { it.cause }.joinToString("\n") { it.message.orEmpty() }
        assertTrue(messages.contains("protected route requires authentication-only @Secured"), messages)
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `actual startup accepts effective class and method authentication for SHARED routes`(securityEnabled: Boolean) {
    operationContext(checkerEnabled = true, securityEnabled = securityEnabled).use { context ->
      context.environment.addPropertySource(PropertySource.of("shared-metadata", mapOf("test.shared-metadata" to "method-authentication")))
      assertDoesNotThrow { context.start() }
      val inventory = context.getBean(RouteAccessInventory::class.java)
      assertTrue(inventory.routes().any { it.uriMatchTemplate.toString() == "/api/v1/shared-metadata-fixture" })
    }
  }

  @Test
  fun `automatic HEAD routes retain shared authorization after startup validation`() {
    operationContext(checkerEnabled = true, earlierAllow = true).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          for ((id, expected) in listOf(UUID(0, 1) to 200, UUID(0, 0) to 403)) {
            val request =
              java.net.http.HttpRequest
                .newBuilder(URI("${server.url}/api/v1/operation-fixture/read?id=$id"))
                .header("Authorization", "Bearer valid")
                .method(
                  "HEAD",
                  java.net.http.HttpRequest.BodyPublishers
                    .noBody(),
                ).build()
            assertEquals(expected, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode())
          }
          assertEquals(1, context.getBean(OperationFixtureWork::class.java).calls.get())
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `unverified declarations deny at runtime after earlier ALLOWED even without authentication`(securityEnabled: Boolean) {
    operationContext(
      checkerEnabled = true,
      validateStartup = false,
      invalidRoutes = true,
      earlierAllow = true,
      securityEnabled = securityEnabled,
    ).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          for (path in listOf("missing", "non-bean")) {
            val request =
              java.net.http.HttpRequest
                .newBuilder(URI("${server.url}/api/v1/invalid-operation-fixture/$path"))
                .header("Authorization", "Bearer valid")
                .GET()
                .build()
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            assertEquals(403, response.statusCode(), "$path: ${response.body()}")
            assertEquals(0, context.getBean(OperationInvalidController::class.java).calls.get())
          }
        }
      }
    }
  }

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
  val calls = AtomicInteger()

  @Get("/missing")
  fun missing(): String {
    calls.incrementAndGet()
    return "unchecked"
  }

  @Get("/non-bean")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, OperationNonBeanRule::class)
  fun nonBean() = "unbound"
}

@Controller("/api/v1/shared-metadata-fixture")
@Requires(property = "test.shared-metadata", value = "missing")
class OperationMissingSecurityController {
  @Get
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, OperationFixtureRule::class)
  fun read() = "unreachable"
}

@Controller("/api/v1/shared-metadata-fixture")
@Requires(property = "test.shared-metadata", value = "empty")
@Secured
class OperationEmptySecurityController {
  @Get
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, OperationFixtureRule::class)
  fun read() = "unreachable"
}

@Controller("/api/v1/shared-metadata-fixture")
@Requires(property = "test.shared-metadata", value = "role")
@Secured("legacy-role")
class OperationRoleSecurityController {
  @Get
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, OperationFixtureRule::class)
  fun read() = "unreachable"
}

@Controller("/api/v1/shared-metadata-fixture")
@Requires(property = "test.shared-metadata", value = "mixed")
@Secured(SecurityRule.IS_AUTHENTICATED, "legacy-role")
class OperationMixedSecurityController {
  @Get
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, OperationFixtureRule::class)
  fun read() = "unreachable"
}

@Controller("/api/v1/shared-metadata-fixture")
@Requires(property = "test.shared-metadata", value = "method-authentication")
@Secured("legacy-role")
class OperationMethodAuthenticationController {
  @Get
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, OperationFixtureRule::class)
  fun read() = "accepted"
}
