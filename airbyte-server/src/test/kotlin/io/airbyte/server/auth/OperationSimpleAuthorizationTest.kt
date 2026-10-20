/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.QueryValue
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.annotation.Secured
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.rules.SecurityRule
import io.mockk.clearMocks
import io.mockk.verify
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import reactor.core.publisher.Mono
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

class OperationSimpleAuthorizationTest {
  @Test
  fun `HTTP requests use startup declarations without repeating inventory validation`() {
    operationContext(checkerEnabled = true, validateStartup = true, simple = true, inventorySpy = true).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        val inventory = context.getBean(RouteAccessInventory::class.java)
        verify(exactly = 1) { inventory.validate() }
        verify(atLeast = 1) { inventory.violations(any()) }
        clearMocks(inventory, answers = false)

        HttpClient.newHttpClient().use { client ->
          fun response(id: UUID): HttpResponse<String> =
            client.send(
              java.net.http.HttpRequest
                .newBuilder(URI("${server.url}/api/v1/simple-operation/plain?id=$id"))
                .header("Authorization", "Bearer valid")
                .GET()
                .build(),
              HttpResponse.BodyHandlers.ofString(),
            )

          val id = UUID.randomUUID()
          val allowed = response(id)
          assertEquals(200, allowed.statusCode(), allowed.body())
          assertEquals(id.toString(), allowed.body())
          assertEquals(403, response(UUID(0, 0)).statusCode())
          assertEquals(1, context.getBean(OperationSimpleController::class.java).calls.get())
          assertEquals(1, context.getBean(OperationSimpleBusiness::class.java).calls.get())
          verify(exactly = 0) { inventory.violations(any()) }
        }
      }
    }
  }

  @Test
  fun `startup accepts a SHARED endpoint calling a normal business method`() {
    operationContext(checkerEnabled = true, validateStartup = true, simple = true).use { context ->
      assertDoesNotThrow { context.start() }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = ["plain", "no-result", "reactive-no-result"])
  fun `an authorized endpoint succeeds with plain and reactive responses`(path: String) {
    operationContext(checkerEnabled = true, simple = true).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val id = UUID.randomUUID()
          val request =
            java.net.http.HttpRequest
              .newBuilder(URI("${server.url}/api/v1/simple-operation/$path?id=$id"))
              .header("Authorization", "Bearer valid")
              .GET()
              .build()
          val response = client.send(request, HttpResponse.BodyHandlers.ofString())
          assertEquals(200, response.statusCode(), response.body())
          assertEquals(if (path == "plain") id.toString() else "accepted", response.body())
          assertEquals(if (path == "plain") 1 else 0, context.getBean(OperationSimpleBusiness::class.java).calls.get())
        }
      }
    }
  }

  @Test
  fun `rule denial prevents the controller and normal business work after earlier ALLOWED`() {
    operationContext(checkerEnabled = true, earlierAllow = true, simple = true).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val request =
            java.net.http.HttpRequest
              .newBuilder(URI("${server.url}/api/v1/simple-operation/plain?id=${UUID(0, 0)}"))
              .header("Authorization", "Bearer valid")
              .GET()
              .build()
          assertEquals(403, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode())
          assertEquals(0, context.getBean(OperationSimpleBusiness::class.java).calls.get())
          assertEquals(0, context.getBean(OperationSimpleController::class.java).calls.get())
        }
      }
    }
  }
}

@Singleton
@Requires(property = "test.simple", value = "true")
class OperationSimpleRule : OperationRule {
  override fun readInput(args: Array<out Any?>) = OperationFixtureInput(args.single() as UUID)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val id = (input as OperationFixtureInput).id
    if (id == UUID(0, 0)) throw ForbiddenProblem()
  }
}

@Singleton
@Requires(property = "test.simple", value = "true")
class OperationSimpleBusiness {
  val calls = AtomicInteger()

  fun read(id: UUID): String {
    calls.incrementAndGet()
    return id.toString()
  }
}

@Controller("/api/v1/simple-operation")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Requires(property = "test.simple", value = "true")
class OperationSimpleController(
  private val business: OperationSimpleBusiness,
) {
  val calls = AtomicInteger()

  @Get("/plain")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationSimpleRule::class)
  fun plain(
    @QueryValue id: UUID,
  ): String {
    calls.incrementAndGet()
    return business.read(id)
  }

  @Get("/no-result")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationSimpleRule::class)
  fun noResult(
    @QueryValue id: UUID,
  ) = "accepted"

  @Get("/reactive-no-result")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationSimpleRule::class)
  fun reactiveNoResult(
    @QueryValue id: UUID,
  ): Mono<String> = Mono.just("accepted")
}
