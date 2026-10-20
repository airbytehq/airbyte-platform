/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.annotation.Secured
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse

class RouteAccessHttpTest {
  @Test
  fun `an authentication-only managed route without a declaration fails startup`() {
    context(validateStartup = true).use { context ->
      val failure = assertThrows(RuntimeException::class.java) { context.start() }
      val messages = generateSequence<Throwable>(failure) { it.cause }.map { it.message.orEmpty() }.toList()
      assertTrue(messages.any { it.contains("missing method-level @AuthorizeOperation") }, messages.joinToString("\n"))
    }
  }

  @Test
  fun `an earlier ALLOWED result cannot bypass missing declaration denial`() {
    context(validateStartup = false, earlierAllow = true).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val request =
            java.net.http.HttpRequest
              .newBuilder(
                URI("${server.url}/api/v1/inventory-fixture/missing"),
              ).header("Authorization", "Bearer reader")
              .GET()
              .build()
          assertEquals(403, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode())
          assertEquals(0, context.getBean(RouteAccessMissingController::class.java).calls)
        }
      }
    }
  }

  @Test
  fun `an undeclared public API route outside the usual api prefix also denies requests`() {
    context(validateStartup = false, earlierAllow = true).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val request =
            java.net.http.HttpRequest
              .newBuilder(
                URI("${server.url}/public/v1/inventory-fixture/missing"),
              ).header("Authorization", "Bearer reader")
              .GET()
              .build()
          assertEquals(403, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode())
        }
      }
    }
  }

  @Test
  fun `LEGACY metadata preserves existing role denial public login and handler failures`() {
    context(validateStartup = false).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          fun status(
            path: String,
            token: String = "",
          ): Int {
            val request =
              java.net.http.HttpRequest
                .newBuilder(URI("${server.url}$path"))
            if (token.isNotEmpty()) request.header("Authorization", "Bearer $token")
            val response = client.send(request.GET().build(), HttpResponse.BodyHandlers.ofString())
            return response.statusCode()
          }
          assertEquals(401, status("/api/v1/inventory-fixture/legacy"))
          assertEquals(403, status("/api/v1/inventory-fixture/legacy", "wrong-role"))
          assertEquals(200, status("/api/v1/inventory-fixture/legacy", "reader"))
          assertEquals(200, status("/api/v1/inventory-fixture/public"))
          assertEquals(200, status("/api/v1/inventory-fixture/login"))
          assertEquals(404, status("/api/v1/inventory-fixture/not-found", "reader"))
        }
      }
    }
  }

  @Test
  fun `handler errors retain the same HTTP status and body with the declaration filter`() {
    fun response(skipFilter: Boolean): Pair<Int, String> =
      context(validateStartup = false, skipFilter = skipFilter).start().use { context ->
        context.getBean(EmbeddedServer::class.java).start().use { server ->
          HttpClient.newHttpClient().use { client ->
            val request =
              java.net.http.HttpRequest
                .newBuilder(URI("${server.url}/api/v1/inventory-fixture/failure"))
                .header("Authorization", "Bearer reader")
                .GET()
                .build()
            val result = client.send(request, HttpResponse.BodyHandlers.ofString())
            result.statusCode() to result.body()
          }
        }
      }
    val baseline = response(skipFilter = true)
    assertEquals(400, baseline.first)
    assertTrue(baseline.second.contains("handler failed"), baseline.second)
    assertEquals(baseline, response(skipFilter = false))
  }

  @Test
  fun `configured no-auth behavior is preserved`() {
    context(validateStartup = false, securityEnabled = false).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val request =
            java.net.http.HttpRequest
              .newBuilder(URI("${server.url}/api/v1/inventory-fixture/legacy"))
              .GET()
              .build()
          assertEquals(200, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode())
        }
      }
    }
  }

  @Test
  fun `unverified SHARED cannot execute after an earlier ALLOWED result`() {
    context(validateStartup = false, earlierAllow = true).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val request =
            java.net.http.HttpRequest
              .newBuilder(
                URI("${server.url}/api/v1/inventory-fixture/shared"),
              ).header("Authorization", "Bearer reader")
              .GET()
              .build()
          assertEquals(403, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode())
          assertEquals(0, context.getBean(RouteAccessMissingController::class.java).calls)
        }
      }
    }
  }

  private fun context(
    validateStartup: Boolean,
    earlierAllow: Boolean = false,
    securityEnabled: Boolean = true,
    skipFilter: Boolean = false,
  ): ApplicationContext {
    val allowed = mutableListOf("io.airbyte.server.auth.RouteAccess")
    val isolated =
      object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
        override fun isWithin(beanDefinitionReference: BeanDefinitionReference<*>): Boolean =
          isWithin(beanDefinitionReference.beanDefinitionName.replace("$", ""))

        override fun isWithin(cls: Class<*>): Boolean = isWithin(cls.name)

        override fun isWithin(className: String): Boolean =
          className.startsWith("io.airbyte.") &&
            (
              allowed.none(className::startsWith) || (!validateStartup && className.contains("RouteAccessStartup")) ||
                (skipFilter && className.contains("RouteAccessFilter"))
            )
      }
    return ApplicationContext
      .builder()
      .enableDefaultPropertySources(false)
      .deduceEnvironment(false)
      .environmentPropertySource(false)
      .beanConfigurations(isolated)
      .properties(
        mapOf(
          "spec.name" to "RouteAccessHttpTest",
          "micronaut.server.port" to -1,
          "micronaut.metrics.enabled" to false,
          "micronaut.security.enabled" to securityEnabled,
          "micronaut.security.filter.enabled" to securityEnabled,
          "test.earlier-allow" to earlierAllow,
          "micronaut.security.reject-not-found" to false,
        ),
      ).build()
  }
}

@Controller("/api/v1/inventory-fixture")
@Requires(property = "spec.name", value = "RouteAccessHttpTest")
@Secured(SecurityRule.IS_AUTHENTICATED)
class RouteAccessMissingController {
  @Get("/legacy")
  @Secured("test-reader")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.LEGACY)
  fun legacy() = "legacy permission retained"

  @Get("/public")
  @Secured(SecurityRule.IS_ANONYMOUS)
  @AuthorizeOperation(RouteAccessPolicy.PUBLIC, RouteMigration.NOT_APPLICABLE)
  fun publicRoute() = "public"

  @Get("/login")
  @Secured(SecurityRule.IS_ANONYMOUS)
  @AuthorizeOperation(RouteAccessPolicy.LOGIN, RouteMigration.NOT_APPLICABLE)
  fun loginRoute() = "login"

  @Get("/failure")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.LEGACY)
  fun failure(): String =
    throw io.micronaut.http.exceptions
      .HttpStatusException(io.micronaut.http.HttpStatus.SERVICE_UNAVAILABLE, "handler failed")

  @Get("/shared")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED)
  fun shared(): String {
    calls++
    return "unguarded"
  }

  var calls = 0

  @Get("/missing")
  fun missing(): String {
    calls++
    return "unchecked"
  }
}

@Singleton
@Requires(property = "spec.name", value = "RouteAccessHttpTest")
class RouteAccessTestAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> =
    when (request.headers.authorization.orElse("")) {
      "Bearer reader" -> Publishers.just(Authentication.build("test-user", listOf("test-reader")))
      "" -> Publishers.empty()
      else -> Publishers.just(Authentication.build("test-user"))
    }
}

@Singleton
@Requires(property = "spec.name", value = "RouteAccessHttpTest")
@Requires(property = "test.earlier-allow", value = "true")
class RouteAccessEarlierAllow : SecurityRule<HttpRequest<*>> {
  override fun getOrder(): Int = Int.MIN_VALUE

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> = Publishers.just(SecurityRuleResult.ALLOWED)
}

@Controller("/public/v1/inventory-fixture")
@Requires(property = "spec.name", value = "RouteAccessHttpTest")
@Secured(SecurityRule.IS_AUTHENTICATED)
class RouteAccessOutsidePrefixController {
  @Get("/missing")
  fun missing() = "unchecked"
}
