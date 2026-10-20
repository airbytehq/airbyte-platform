/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.authentication.Authentication
import io.micronaut.web.router.RouteAttributes
import io.micronaut.web.router.Router
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Supplier

class OperationReactiveExecutionTest {
  @Test
  fun `an authorized deferred route can return without business work`() {
    operationContext(checkerEnabled = true).start().use { context ->
      context.getBean(OperationFixtureBehavior::class.java).publisher = { _, _ -> Mono.just("accepted") }
      val id = UUID.randomUUID()
      val request = request(context, id)
      StepVerifier.create(deferred(context, request, id)).expectNext("accepted").verifyComplete()
      assertEquals(0, context.getBean(OperationFixtureWork::class.java).calls.get())
    }
  }

  @Test
  fun `an authorized publisher supports scheduler changes and repeat subscriptions`() {
    operationContext(checkerEnabled = true).start().use { context ->
      context.getBean(OperationFixtureBehavior::class.java).publisher =
        { id, read ->
          Mono.fromCallable { read(id) }.subscribeOn(Schedulers.boundedElastic())
        }
      val id = UUID.randomUUID()
      val request = request(context, id)
      val publisher = deferred(context, request, id)
      assertEquals(0, context.getBean(OperationFixtureWork::class.java).calls.get())
      StepVerifier.create(publisher).expectNext(id.toString()).verifyComplete()
      StepVerifier.create(publisher).expectNext(id.toString()).verifyComplete()
      assertEquals(2, context.getBean(OperationFixtureWork::class.java).calls.get())
    }
  }

  @Test
  fun `cancellation reaches the deferred publisher after work starts`() {
    operationContext(checkerEnabled = true).start().use { context ->
      val cancelled = AtomicBoolean()
      context.getBean(OperationFixtureBehavior::class.java).publisher = { id, read ->
        Mono.defer {
          read(id)
          Mono.never<String>().doOnCancel { cancelled.set(true) }
        }
      }
      val id = UUID.randomUUID()
      val request = request(context, id)
      StepVerifier
        .create(deferred(context, request, id))
        .then { assertEquals(1, context.getBean(OperationFixtureWork::class.java).calls.get()) }
        .thenCancel()
        .verify()
      assertTrue(cancelled.get())
    }
  }

  @Test
  fun `deferred errors propagate after business work`() {
    operationContext(checkerEnabled = true).start().use { context ->
      context.getBean(OperationFixtureBehavior::class.java).publisher = { id, read ->
        Mono.defer {
          read(id)
          Mono.error<String>(IllegalStateException("deferred failure"))
        }
      }
      val id = UUID.randomUUID()
      val request = request(context, id)
      StepVerifier.create(deferred(context, request, id)).expectError(IllegalStateException::class.java).verify()
      assertEquals(1, context.getBean(OperationFixtureWork::class.java).calls.get())
    }
  }

  @Test
  fun `concurrent deferred requests use their own original arguments`() {
    operationContext(checkerEnabled = true).start().use { context ->
      val entered = CountDownLatch(2)
      context.getBean(OperationFixtureBehavior::class.java).publisher = { id, read ->
        Mono
          .fromCallable {
            entered.countDown()
            check(entered.await(10, TimeUnit.SECONDS))
            read(id)
          }.subscribeOn(Schedulers.boundedElastic())
      }
      val ids = listOf(UUID.randomUUID(), UUID.randomUUID())
      val publishers = ids.map { deferred(context, request(context, it), it) }
      StepVerifier
        .create(Flux.merge(publishers).collectList())
        .assertNext { assertEquals(ids.map(UUID::toString).toSet(), it.toSet()) }
        .verifyComplete()
      assertEquals(2, context.getBean(OperationFixtureWork::class.java).calls.get())
    }
  }

  @Test
  fun `rule denial prevents deferred publisher creation and business work`() {
    operationContext(checkerEnabled = true).start().use { context ->
      context.getBean(OperationFixtureBehavior::class.java).publisher = { _, _ -> throw AssertionError("Controller work ran") }
      val id = UUID(0, 0)
      assertThrows(ForbiddenProblem::class.java) { deferred(context, request(context, id), id) }
      assertEquals(0, context.getBean(OperationFixtureWork::class.java).calls.get())
    }
  }

  private fun deferred(
    context: ApplicationContext,
    request: HttpRequest<*>,
    id: UUID,
  ): Publisher<String> = ServerRequestContext.with(request, Supplier { context.getBean(OperationFixtureController::class.java).deferred(id) })

  private fun request(
    context: ApplicationContext,
    id: UUID,
  ): HttpRequest<*> {
    val request = HttpRequest.GET<Any>("/api/v1/operation-fixture/deferred?id=$id")
    request.setUserPrincipal(Authentication.build("caller"))
    RouteAttributes.setRouteMatch(request, context.getBean(Router::class.java).GET<Any, Any>(request.uri.toString()).orElseThrow())
    return request
  }
}
