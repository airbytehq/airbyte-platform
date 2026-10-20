/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
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
  fun `wrong access policies and unverified SHARED routes are rejected from real route metadata`() {
    context().start().use { context ->
      val inventory = RouteAccessInventory(context.getBean(Router::class.java))

      fun errors(suffix: String) =
        inventory.violations(
          inventory.routes().first {
            it.httpMethodName == "GET" &&
              it.uriMatchTemplate.toString().endsWith(suffix)
          },
        )
      assertTrue(errors("/public-protected").any { it.contains("conflicts") })
      assertTrue(errors("/shared").any { it.contains("business guard") })
    }
  }

  @Test
  fun `inherited operations are inventoried but an override cannot borrow its parents declaration`() {
    context(replacement = true).start().use { context ->
      val inventory = RouteAccessInventory(context.getBean(Router::class.java))
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
      val inventory = RouteAccessInventory(context.getBean(Router::class.java))
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
      val inventory = RouteAccessInventory(context.getBean(Router::class.java))
      inventory.validate()
      assertEquals(emptyList<RouteAccessEntry>(), routeAccessEntries(inventory))
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
  @AuthorizeOperation(RouteAccessPolicy.PUBLIC, RouteMigration.NOT_APPLICABLE)
  fun wrongPublic() = "unreachable"

  @Get("/shared")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED)
  fun shared() = "unreachable"
}

@Context
@Controller("/api/v1/inherited-fixture")
@Requires(property = "spec.name", value = "RouteAccessMetadataTest")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class RouteAccessParentController {
  @Get("/inherited")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.LEGACY)
  open fun inherited() = "inherited"

  @Get("/overridden")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.LEGACY)
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
