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
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.function.Supplier

class OperationExecutionTest {
  @Test
  fun `controller exceptions propagate without business work`() =
    withRequest { context, request ->
      context.getBean(OperationFixtureBehavior::class.java).call = { _, _ -> throw IllegalStateException("business failure") }
      val failure = assertThrows(IllegalStateException::class.java) { invoke(context, request) }
      assertEquals("business failure", failure.message)
      assertEquals(0, context.getBean(OperationFixtureWork::class.java).calls.get())
    }

  @Test
  fun `a SHARED controller call without a current request rejects before business work`() {
    operationContext(checkerEnabled = true).start().use { context ->
      assertThrows(ForbiddenProblem::class.java) { context.getBean(OperationFixtureController::class.java).read(ID) }
      assertEquals(0, context.getBean(OperationFixtureWork::class.java).calls.get())
    }
  }

  private fun invoke(
    context: ApplicationContext,
    request: HttpRequest<*>,
  ): String = ServerRequestContext.with(request, Supplier { context.getBean(OperationFixtureController::class.java).read(ID) })

  private fun withRequest(test: (ApplicationContext, HttpRequest<*>) -> Unit) {
    operationContext(checkerEnabled = true).start().use { context ->
      val request = HttpRequest.GET<Any>("/api/v1/operation-fixture/read?id=$ID")
      request.setUserPrincipal(Authentication.build("caller"))
      RouteAttributes.setRouteMatch(request, context.getBean(Router::class.java).GET<Any, Any>(request.uri.toString()).orElseThrow())
      test(context, request)
    }
  }

  companion object {
    private val ID = UUID.fromString("00000000-0000-0000-0000-000000000001")
  }
}
