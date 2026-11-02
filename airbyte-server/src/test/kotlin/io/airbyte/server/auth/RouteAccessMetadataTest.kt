/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.ConfigSearchAccessException
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.micronaut.web.router.Router
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RouteAccessMetadataTest {
  @Test
  fun `wrong access policies and protected routes without rules are rejected from real route metadata`() {
    context().start().use { context ->
      val inventory = RouteAccessInventory(context.getBean(Router::class.java), context)

      fun errors(suffix: String) =
        inventory.violations(
          inventory.routes().first {
            it.httpMethodName == "GET" &&
              it.uriMatchTemplate.toString().endsWith(suffix)
          },
        )
      assertTrue(errors("/public-protected").any { it.contains("conflicts") })
      assertTrue(errors("/shared").any { it.contains("concrete DI rule") })
      assertTrue(errors("/missing-rule").any { it.contains("concrete DI rule") })
    }
  }

  @Test
  fun `inherited operations are inventoried but an override cannot borrow its parents declaration`() {
    context(replacement = true).start().use { context ->
      val inventory = RouteAccessInventory(context.getBean(Router::class.java), context)
      val inherited = inventory.routes().filter { it.httpMethodName == "GET" && it.uriMatchTemplate.toString().endsWith("/inherited") }
      assertEquals(1, inherited.size)
      assertTrue(inventory.violations(inherited.single()).isEmpty())
      val override = inventory.routes().single { it.httpMethodName == "GET" && it.uriMatchTemplate.toString().endsWith("/overridden") }
      assertTrue(inventory.violations(override).any { it.contains("missing method-level") })
      assertFalse(inventory.routes().any { it.declaringType == RouteAccessParentController::class.java })
    }
  }

  @Test
  fun `an undeclared Config route using a workload URL is still managed`() {
    context().start().use { context ->
      val inventory = RouteAccessInventory(context.getBean(Router::class.java), context)
      val route =
        inventory.routes().single {
          it.httpMethodName == "GET" && it.uriMatchTemplate.toString() == "/api/v1/workload/config-fixture"
        }
      assertTrue(inventory.violations(route).any { it.contains("missing method-level") })
    }
  }

  @Test
  fun `an empty router has an empty inventory`() {
    context(empty = true).start().use { context ->
      val inventory = RouteAccessInventory(context.getBean(Router::class.java), context)
      inventory.validate()
      assertEquals(emptyList<RouteAccessEntry>(), routeAccessEntries(inventory))
    }
  }

  @Test
  fun `search exception is restricted to the three authenticated POST searches without a rule`() {
    context().start().use { context ->
      val inventory = RouteAccessInventory(context.getBean(Router::class.java), context)
      for (route in inventory.routes().filter { it.targetMethod.methodName in setOf("wrongPath", "withRule", "publicException") }) {
        val errors = inventory.violations(route)
        assertTrue(errors.any { it.contains("search exception") }, "${route.httpMethodName} ${route.uriMatchTemplate}: $errors")
      }
      val valid = inventory.routes().filter { it.httpMethodName == "POST" && it.uriMatchTemplate.toString() == "/api/v1/sources/search" }
      assertEquals(1, valid.size)
      assertTrue(inventory.violations(valid.single()).isEmpty())
      val wrongMethod = inventory.routes().first { it.httpMethodName == "GET" && it.uriMatchTemplate.toString() == "/api/v1/sources/search" }
      assertTrue(inventory.violations(wrongMethod).any { it.contains("search exception") })
    }
  }

  private fun context(
    replacement: Boolean = false,
    empty: Boolean = false,
  ): ApplicationContext {
    val allowed =
      if (empty) {
        emptyList()
      } else {
        listOf(
          "io.airbyte.server.auth.RouteAccessMetadataController",
          "io.airbyte.server.auth.RouteAccessWorkloadPathController",
          "io.airbyte.server.auth.RouteAccessParentController",
          "io.airbyte.server.auth.RouteAccessReplacementController",
          "io.airbyte.server.auth.RouteAccessSearchExceptionController",
        )
      }
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
          "spec.name" to "RouteAccessMetadataTest",
          "test.replacement" to replacement,
          "micronaut.security.enabled" to false,
          "micronaut.server.port" to -1,
        ),
      ).build()
  }
}

@Controller("/api/v1/metadata-fixture")
@Requires(property = "spec.name", value = "RouteAccessMetadataTest")
@Secured(SecurityRule.IS_AUTHENTICATED)
class RouteAccessMetadataController {
  @Get("/public-protected")
  @AuthorizeOperation(RouteAccessPolicy.PUBLIC)
  fun wrongPublic() = "unreachable"

  @Get("/shared")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED)
  fun shared() = "unreachable"

  @Get("/missing-rule")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED)
  fun missingRule() = "unreachable"
}

@Context
@Controller("/api/v1/inherited-fixture")
@Requires(property = "spec.name", value = "RouteAccessMetadataTest")
@Secured(SecurityRule.IS_ANONYMOUS)
open class RouteAccessParentController {
  @Get("/inherited")
  @AuthorizeOperation(RouteAccessPolicy.PUBLIC)
  open fun inherited() = "inherited"

  @Get("/overridden")
  @AuthorizeOperation(RouteAccessPolicy.PUBLIC)
  open fun overridden() = "parent"
}

@Context
@Controller("/api/v1/inherited-fixture")
@Replaces(RouteAccessParentController::class)
@Requires(property = "spec.name", value = "RouteAccessMetadataTest")
@Requires(property = "test.replacement", value = "true")
class RouteAccessReplacementController : RouteAccessParentController() {
  override fun overridden() = "override without a declaration"
}

@Controller("/api/v1/workload/config-fixture")
@Requires(property = "spec.name", value = "RouteAccessMetadataTest")
@Secured(SecurityRule.IS_AUTHENTICATED)
class RouteAccessWorkloadPathController {
  @Get
  fun missing() = "unchecked"
}

@Controller("/api/v1")
@Requires(property = "spec.name", value = "RouteAccessMetadataTest")
@Secured(SecurityRule.IS_AUTHENTICATED)
class RouteAccessSearchExceptionController {
  @Post("/sources/search")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED)
  @ConfigSearchAccessException
  fun valid() = "preserved search"

  @Get("/sources/search")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED)
  @ConfigSearchAccessException
  fun wrongMethod() = "unreachable"

  @Post("/search-exception/wrong-path")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED)
  @ConfigSearchAccessException
  fun wrongPath() = "unreachable"

  @Post("/connections/search")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, OperationNonBeanRule::class)
  @ConfigSearchAccessException
  fun withRule() = "unreachable"

  @Post("/destinations/search")
  @Secured(SecurityRule.IS_ANONYMOUS)
  @AuthorizeOperation(RouteAccessPolicy.PUBLIC)
  @ConfigSearchAccessException
  fun publicException() = "unreachable"
}
