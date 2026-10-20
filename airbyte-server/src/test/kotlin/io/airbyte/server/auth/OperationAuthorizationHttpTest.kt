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
import io.micronaut.aop.InterceptorBean
import io.micronaut.aop.MethodInterceptor
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.QueryValue
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.annotation.Secured
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import io.mockk.spyk
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

class OperationAuthorizationHttpTest {
  @Test
  fun `SHARED preserves the actual server no-auth configuration`() {
    operationContext(checkerEnabled = true, securityEnabled = false).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val request =
            java.net.http.HttpRequest
              .newBuilder(URI("${server.url}/api/v1/operation-fixture/read?id=${UUID.randomUUID()}"))
              .GET()
              .build()
          val response = client.send(request, HttpResponse.BodyHandlers.ofString())
          assertEquals(200, response.statusCode(), response.body())
          assertEquals(1, context.getBean(OperationFixtureWork::class.java).calls.get())
        }
      }
    }
  }

  @Test
  fun `the checker authorizes parsed input before the controller uses its original arguments`() {
    operationContext(checkerEnabled = true).start().use { context ->
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val id = UUID.randomUUID()
          val request =
            java.net.http.HttpRequest
              .newBuilder(URI("${server.url}/api/v1/operation-fixture/read?id=$id"))
              .header("Authorization", "Bearer valid")
              .GET()
              .build()
          val response = client.send(request, HttpResponse.BodyHandlers.ofString())
          assertEquals(200, response.statusCode(), response.body())
          assertEquals(id.toString(), response.body())
          assertEquals(1, context.getBean(OperationFixtureWork::class.java).calls.get())
        }
      }
    }
  }

  @Test
  fun `normal injected business methods can be called directly`() {
    operationContext().start().use { context ->
      val business = context.getBean(OperationFixtureBusiness::class.java)
      val id = UUID.randomUUID()
      assertEquals(id.toString(), business.read(id))
      assertEquals(1, context.getBean(OperationFixtureWork::class.java).calls.get())
    }
  }
}

internal fun operationContext(
  earlierAllow: Boolean = false,
  checkerEnabled: Boolean = false,
  securityEnabled: Boolean = true,
  validateStartup: Boolean = checkerEnabled,
  invalidRoutes: Boolean = false,
  typed: Boolean = false,
  scopeRule: Boolean = false,
  simple: Boolean = false,
  inventorySpy: Boolean = false,
): ApplicationContext {
  val allowed =
    listOf(
      "io.airbyte.server.auth.Operation",
      "io.airbyte.server.auth.AuthorizedRequest",
      "io.airbyte.server.auth.AuthorizationInterceptor",
      "io.airbyte.server.auth.RouteAccess",
      "io.airbyte.server.auth.SharedOperationRoutes",
      "io.airbyte.commons.server.errors.problems.AbstractThrowableProblemHandler",
    ) + if (scopeRule) listOf("io.airbyte.server.auth.ScopedTokenSecurityRule") else emptyList()
  val isolated =
    object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
      override fun isWithin(beanDefinitionReference: BeanDefinitionReference<*>): Boolean =
        isWithin(beanDefinitionReference.beanDefinitionName.replace("$", ""))

      override fun isWithin(cls: Class<*>): Boolean = isWithin(cls.name)

      override fun isWithin(className: String): Boolean =
        className.startsWith("io.airbyte.") &&
          (
            allowed.none(className::startsWith) || (!checkerEnabled && className.contains("AuthorizationInterceptor")) ||
              (!validateStartup && className.contains("RouteAccessStartup"))
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
        "spec.name" to "OperationAuthorizationHttpTest",
        "test.earlier-allow" to earlierAllow,
        "test.checker-enabled" to checkerEnabled,
        "test.invalid-routes" to invalidRoutes,
        "test.typed" to typed,
        "test.simple" to simple,
        "test.inventory-spy" to inventorySpy,
        "micronaut.security.enabled" to securityEnabled,
        "micronaut.security.filter.enabled" to securityEnabled,
        "micronaut.server.port" to -1,
        "micronaut.metrics.enabled" to false,
      ),
    ).build()
}

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
@Requires(property = "test.inventory-spy", value = "true")
class OperationInventorySpy : BeanCreatedEventListener<RouteAccessInventory> {
  override fun onCreated(event: BeanCreatedEvent<RouteAccessInventory>): RouteAccessInventory = spyk(event.bean)
}

data class OperationFixtureInput(
  val id: UUID,
) : OperationInput

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
class OperationFixtureRule : OperationRule {
  override fun readInput(args: Array<out Any?>) = OperationFixtureInput(args.single() as UUID)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    if ((input as OperationFixtureInput).id == UUID(0, 0)) throw ForbiddenProblem()
  }
}

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
class OperationFixtureWork {
  val calls = AtomicInteger()

  fun read(id: UUID): String {
    calls.incrementAndGet()
    return id.toString()
  }
}

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
class OperationFixtureBusiness(
  private val work: OperationFixtureWork,
) {
  fun read(id: UUID) = work.read(id)
}

@Controller("/api/v1/operation-fixture")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
open class OperationFixtureController(
  private val business: OperationFixtureBusiness,
  private val behavior: OperationFixtureBehavior,
) {
  @Get("/read")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationFixtureRule::class)
  open fun read(
    @QueryValue id: UUID,
  ): String = behavior.call(id) { business.read(it) }

  @Get("/deferred")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationFixtureRule::class)
  open fun deferred(
    @QueryValue id: UUID,
  ): Publisher<String> = behavior.publisher(id) { business.read(it) }

  @Get("/stage")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationFixtureRule::class)
  open fun stage(
    @QueryValue id: UUID,
  ): CompletableFuture<String> = CompletableFuture.completedFuture(business.read(id))
}

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
class OperationFixtureBehavior {
  var call: (UUID, (UUID) -> String) -> String = { id, read -> read(id) }
  var publisher: (
    UUID,
    (UUID) -> String,
  ) -> Publisher<String> = { id, read -> Mono.fromCallable { read(id) } }
}

// Keep a proxied route without its real checker to exercise startup validation.
@Singleton
@InterceptorBean(AuthorizeOperation::class)
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
@Requires(property = "test.checker-enabled", value = "false")
class OperationFixtureBypassChecker : MethodInterceptor<Any, Any> {
  override fun intercept(context: MethodInvocationContext<Any, Any>): Any? = context.proceed()
}

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
class OperationFixtureAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> =
    when (request.headers.authorization.orElse("")) {
      "Bearer valid" -> Publishers.just(Authentication.build("caller"))
      "Bearer scoped" ->
        Publishers.just(
          Authentication.build(
            "caller",
            mapOf(TokenScopeClaim.CLAIM_ID to mapOf("workspaceId" to "00000000-0000-0000-0000-000000000001")),
          ),
        )
      else -> Publishers.empty()
    }
}

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
@Requires(property = "test.earlier-allow", value = "true")
class OperationFixtureEarlierAllow : SecurityRule<HttpRequest<*>> {
  override fun getOrder() = Int.MIN_VALUE

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> = Publishers.just(SecurityRuleResult.ALLOWED)
}
