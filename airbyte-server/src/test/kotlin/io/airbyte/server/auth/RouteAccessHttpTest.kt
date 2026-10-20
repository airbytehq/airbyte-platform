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
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.reactivestreams.Publisher
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse

class RouteAccessHttpTest {
  @ParameterizedTest
  @CsvSource(
    "missing,missing method-level @AuthorizeOperation",
    "outside,missing method-level @AuthorizeOperation",
    "shared,SHARED requires a concrete DI rule",
  )
  fun `invalid managed declarations fail startup with and without authentication`(
    invalidRoute: String,
    expectedError: String,
  ) {
    for (securityEnabled in listOf(true, false)) {
      context(invalidRoute = invalidRoute, securityEnabled = securityEnabled).use { context ->
        val failure = assertThrows(RuntimeException::class.java) { context.start() }
        val messages = generateSequence<Throwable>(failure) { it.cause }.map { it.message.orEmpty() }.toList()
        assertTrue(messages.any { it.contains(expectedError) }, messages.joinToString("\n"))
      }
    }
  }

  @Test
  fun `LEGACY metadata preserves existing role denial public login and handler failures`() {
    context().start().use { context ->
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
  fun `handler errors preserve their HTTP status and body`() {
    context().start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val request =
            java.net.http.HttpRequest
              .newBuilder(URI("${server.url}/api/v1/inventory-fixture/failure"))
              .header("Authorization", "Bearer reader")
              .GET()
              .build()
          val result = client.send(request, HttpResponse.BodyHandlers.ofString())
          assertEquals(400, result.statusCode())
          assertTrue(result.body().contains("handler failed"), result.body())
        }
      }
    }
  }

  @Test
  fun `configured no-auth behavior is preserved`() {
    context(securityEnabled = false).start().use { context ->
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

  private fun context(
    invalidRoute: String = "",
    securityEnabled: Boolean = true,
  ): ApplicationContext {
    val allowed = listOf("io.airbyte.server.auth.RouteAccess")
    val isolated =
      object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
        override fun isWithin(beanDefinitionReference: BeanDefinitionReference<*>): Boolean =
          isWithin(beanDefinitionReference.beanDefinitionName.replace("$", ""))

        override fun isWithin(cls: Class<*>): Boolean = isWithin(cls.name)

        override fun isWithin(className: String): Boolean = className.startsWith("io.airbyte.") && allowed.none(className::startsWith)
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
          "test.invalid-route" to invalidRoute,
          "micronaut.security.reject-not-found" to false,
        ),
      ).build()
  }
}

@Controller("/api/v1/inventory-fixture")
@Requires(property = "spec.name", value = "RouteAccessHttpTest")
@Secured(SecurityRule.IS_AUTHENTICATED)
class RouteAccessHttpController {
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

@Controller("/api/v1/inventory-fixture")
@Requires(property = "spec.name", value = "RouteAccessHttpTest")
@Requires(property = "test.invalid-route", value = "missing")
@Secured(SecurityRule.IS_AUTHENTICATED)
class RouteAccessMissingController {
  @Get("/missing")
  fun missing() = "unchecked"
}

@Controller("/api/v1/inventory-fixture")
@Requires(property = "spec.name", value = "RouteAccessHttpTest")
@Requires(property = "test.invalid-route", value = "shared")
@Secured(SecurityRule.IS_AUTHENTICATED)
class RouteAccessUnverifiedController {
  @Get("/shared")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED)
  fun shared() = "unguarded"
}

@Controller("/public/v1/inventory-fixture")
@Requires(property = "spec.name", value = "RouteAccessHttpTest")
@Requires(property = "test.invalid-route", value = "outside")
@Secured(SecurityRule.IS_AUTHENTICATED)
class RouteAccessOutsidePrefixController {
  @Get("/missing")
  fun missing() = "unchecked"
}
