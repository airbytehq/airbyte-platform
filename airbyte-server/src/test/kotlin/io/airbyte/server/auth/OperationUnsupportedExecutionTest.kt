/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.QueryValue
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.annotation.Secured
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.rules.SecurityRule
import io.micronaut.web.router.RouteAttributes
import io.micronaut.web.router.Router
import jakarta.inject.Singleton
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import reactor.core.publisher.ConnectableFlux
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

class OperationUnsupportedExecutionTest {
  @ParameterizedTest
  @ValueSource(strings = ["shared", "state"])
  fun `Flow subtypes reject before controller or business work`(shape: String) {
    assertUnsupported(shape, controllerCalls = 0)
  }

  @ParameterizedTest
  @ValueSource(strings = ["connectable", "custom"])
  fun `unsupported Publisher subtypes reject before controller or business work`(shape: String) {
    assertUnsupported(shape, controllerCalls = 0)
  }

  @ParameterizedTest
  @ValueSource(strings = ["publisher", "mono", "flux"])
  fun `supported Publisher Mono and Flux declarations support repeat subscriptions with original arguments`(shape: String) {
    operationContext(checkerEnabled = true).start().use { context ->
      val controller = context.getBean(OperationShapeController::class.java)
      val id = UUID.randomUUID()
      val publisher =
        withRequest(context, shape, id) {
          when (shape) {
            "publisher" -> controller.publisher(id)
            "mono" -> controller.mono(id)
            else -> controller.flux(id)
          }
        }
      assertEquals(0, context.getBean(OperationFixtureWork::class.java).calls.get())
      StepVerifier.create(publisher).expectNext(id.toString()).verifyComplete()
      StepVerifier.create(publisher).expectNext(id.toString()).verifyComplete()
      assertEquals(2, context.getBean(OperationFixtureWork::class.java).calls.get())
    }
  }

  @Test
  fun `unsupported completion stages fail closed before any business work`() {
    operationContext(checkerEnabled = true).start().use { context ->
      val id = UUID.randomUUID()
      val request = HttpRequest.GET<Any>("/api/v1/operation-fixture/stage?id=$id")
      request.setUserPrincipal(Authentication.build("caller"))
      RouteAttributes.setRouteMatch(request, context.getBean(Router::class.java).GET<Any, Any>(request.uri.toString()).orElseThrow())
      assertThrows(ForbiddenProblem::class.java) {
        ServerRequestContext.with(request, Supplier { context.getBean(OperationFixtureController::class.java).stage(id) })
      }
      assertEquals(0, context.getBean(OperationFixtureWork::class.java).calls.get())
    }
  }

  private fun assertUnsupported(
    shape: String,
    controllerCalls: Int,
  ) {
    operationContext(checkerEnabled = true).start().use { context ->
      val controller = context.getBean(OperationShapeController::class.java)
      val id = UUID.randomUUID()
      val result =
        runCatching {
          withRequest(context, shape, id) {
            when (shape) {
              "shared" -> controller.shared(id)
              "state" -> controller.state(id)
              "connectable" -> controller.connectable(id)
              else -> controller.custom(id)
            }
          }
        }
      assertAll(
        { assertEquals(controllerCalls, context.getBean(OperationShapeWork::class.java).controllerCalls.get(), "controller work") },
        { assertEquals(0, context.getBean(OperationFixtureWork::class.java).calls.get(), "business work") },
        { assertInstanceOf(ForbiddenProblem::class.java, result.exceptionOrNull()) },
      )
    }
  }

  private fun <T> withRequest(
    context: ApplicationContext,
    shape: String,
    id: UUID,
    call: () -> T,
  ): T {
    val request = HttpRequest.GET<Any>("/api/v1/operation-shapes/$shape?id=$id")
    request.setUserPrincipal(Authentication.build("caller"))
    RouteAttributes.setRouteMatch(request, context.getBean(Router::class.java).GET<Any, Any>(request.uri.toString()).orElseThrow())
    return ServerRequestContext.with(request, Supplier(call))
  }
}

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
class OperationShapeWork {
  val controllerCalls = AtomicInteger()
}

class OperationCustomPublisher(
  private val value: String,
) : Publisher<String> {
  override fun subscribe(subscriber: Subscriber<in String>) = Mono.just(value).subscribe(subscriber)
}

@Controller("/api/v1/operation-shapes")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
class OperationShapeController(
  private val business: OperationFixtureBusiness,
  private val work: OperationShapeWork,
) {
  @Get("/shared")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationFixtureRule::class)
  fun shared(
    @QueryValue id: UUID,
  ): SharedFlow<String> {
    work.controllerCalls.incrementAndGet()
    return MutableSharedFlow<String>(replay = 1).apply { tryEmit(business.read(id)) }
  }

  @Get("/state")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationFixtureRule::class)
  fun state(
    @QueryValue id: UUID,
  ): StateFlow<String> {
    work.controllerCalls.incrementAndGet()
    return MutableStateFlow(business.read(id))
  }

  @Get("/connectable")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationFixtureRule::class)
  fun connectable(
    @QueryValue id: UUID,
  ): ConnectableFlux<String> {
    work.controllerCalls.incrementAndGet()
    return Flux.just(business.read(id)).publish()
  }

  @Get("/custom")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationFixtureRule::class)
  fun custom(
    @QueryValue id: UUID,
  ): OperationCustomPublisher {
    work.controllerCalls.incrementAndGet()
    return OperationCustomPublisher(business.read(id))
  }

  @Get("/publisher")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationFixtureRule::class)
  fun publisher(
    @QueryValue id: UUID,
  ): Publisher<String> = Mono.fromCallable { business.read(id) }

  @Get("/mono")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationFixtureRule::class)
  fun mono(
    @QueryValue id: UUID,
  ): Mono<String> = Mono.fromCallable { business.read(id) }

  @Get("/flux")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationFixtureRule::class)
  fun flux(
    @QueryValue id: UUID,
  ): Flux<String> = Flux.defer { Flux.just(business.read(id)) }
}
