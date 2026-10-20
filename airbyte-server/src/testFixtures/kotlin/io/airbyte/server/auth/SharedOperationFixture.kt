/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.QueryValue
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.security.annotation.Secured
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.authentication.AuthorizationException
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.rules.SecurityRule
import jakarta.inject.Singleton
import org.reactivestreams.Publisher
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

fun sharedOperationContext(
  cloud: Boolean = false,
  invalidOverride: Boolean = false,
): ApplicationContext {
  val allowed =
    listOf(
      "io.airbyte.server.auth.SharedOperationFixture",
      "io.airbyte.server.wrapped.auth.SharedOperationCloud",
      "io.airbyte.server.auth.AuthorizationInterceptor",
      "io.airbyte.server.auth.AuthorizedRequest",
      "io.airbyte.server.auth.RouteAccess",
      "io.airbyte.commons.server.errors.problems.AbstractThrowableProblemHandler",
    )
  val isolated =
    object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
      override fun isWithin(reference: BeanDefinitionReference<*>): Boolean = isWithin(reference.beanDefinitionName.replace("$", ""))

      override fun isWithin(type: Class<*>): Boolean = isWithin(type.name)

      override fun isWithin(name: String): Boolean = name.startsWith("io.airbyte.") && allowed.none(name::startsWith)
    }
  return ApplicationContext
    .builder()
    .enableDefaultPropertySources(false)
    .deduceEnvironment(false)
    .environmentPropertySource(false)
    .environments(if (cloud) "cloud" else "test")
    .beanConfigurations(isolated)
    .properties(
      mapOf(
        "spec.name" to "SharedOperationReplacementTest",
        "test.invalid-override" to invalidOverride,
        "micronaut.security.enabled" to true,
        "micronaut.server.port" to -1,
        "micronaut.metrics.enabled" to false,
      ),
    ).build()
}

data class SharedOperationFixtureInput(
  val id: UUID,
) : OperationInput

@Singleton
@Requires(property = "spec.name", value = "SharedOperationReplacementTest")
class SharedOperationFixtureRule : OperationRule {
  override fun readInput(args: Array<out Any?>) = SharedOperationFixtureInput(args.single() as UUID)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    if ((input as SharedOperationFixtureInput).id == UUID(0, 0)) throw AuthorizationException(caller)
  }
}

@Singleton
@Requires(property = "spec.name", value = "SharedOperationReplacementTest")
class SharedOperationFixtureBusiness {
  val calls = AtomicInteger()

  fun read(id: UUID): String {
    calls.incrementAndGet()
    return id.toString()
  }
}

@Controller("/api/v1/shared-operation-fixture")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Requires(property = "spec.name", value = "SharedOperationReplacementTest")
open class SharedOperationFixtureController(
  protected val business: SharedOperationFixtureBusiness,
) {
  @Get("/inherited")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, SharedOperationFixtureRule::class)
  open fun inherited(
    @QueryValue id: UUID,
  ): String = business.read(id)

  @Get("/overridden")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, SharedOperationFixtureRule::class)
  open fun overridden(
    @QueryValue id: UUID,
  ): String = business.read(id)
}

@Singleton
@Requires(property = "spec.name", value = "SharedOperationReplacementTest")
class SharedOperationFixtureAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> =
    if (request.headers.authorization.orElse("") == "Bearer valid") Publishers.just(Authentication.build("caller")) else Publishers.empty()
}
