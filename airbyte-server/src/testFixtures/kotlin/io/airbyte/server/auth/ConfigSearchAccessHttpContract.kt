/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ConnectionReadList
import io.airbyte.api.server.generated.models.DestinationReadList
import io.airbyte.api.server.generated.models.SourceReadList
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.MatchSearchHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.server.apis.controllers.ConnectionApiController
import io.airbyte.server.apis.controllers.DestinationApiController
import io.airbyte.server.apis.controllers.SourceApiController
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException

/** The three Config root searches deliberately retain their existing edition-specific behavior. */
abstract class ConfigSearchAccessHttpContract {
  protected open val cloud = false

  @Test
  fun `searches preserve authentication binding responses and handler errors`() = searches(security = true)

  @Test
  fun `searches preserve no auth behavior`() = searches(security = false)

  private fun searches(security: Boolean) {
    val controllers =
      if (cloud) {
        listOf("Source", "Destination", "Connection").map {
          Class.forName("io.airbyte.server.wrapped.apis.${it}ApiWrappedController")
        }
      } else {
        listOf(SourceApiController::class.java, DestinationApiController::class.java, ConnectionApiController::class.java)
      }
    RemainingOperationHttpSupport(cloud, controllers, setOf(ConfigSearchRule::class.java.name), security, earlierAllow = false).run(
      configure = { context, business ->
        context.registerSingleton(AuthenticationHeaderResolver::class.java, mockk<AuthenticationHeaderResolver>())
        business.filterIsInstance<SourceHandler>().forEach { every { it.searchSources(any()) } returns SourceReadList(sources = emptyList()) }
        business.filterIsInstance<DestinationHandler>().forEach {
          every { it.searchDestinations(any()) } returns DestinationReadList(destinations = emptyList())
        }
        business.filterIsInstance<MatchSearchHandler>().forEach {
          every { it.searchConnections(any()) } returns
            ConnectionReadList(connections = emptyList())
        }
      },
      check = { context, server, business ->
        val searches =
          routeAccessEntries(context.getBean(RouteAccessInventory::class.java)).filter {
            it.path in setOf("/api/v1/sources/search", "/api/v1/destinations/search", "/api/v1/connections/search")
          }
        assertEquals(setOf("/api/v1/sources/search", "/api/v1/destinations/search", "/api/v1/connections/search"), searches.map { it.path }.toSet())
        assertTrue(searches.all { it.httpMethod == "POST" && it.hasSharedRule && it.rule == ConfigSearchRule::class.java.name })
        for (family in listOf("sources", "destinations", "connections")) {
          val path = "/api/v1/$family/search"
          if (security) {
            val anonymous = RemainingOperationHttpSupport.request(server, "POST", path, "{}", "")
            assertEquals(401, anonymous.statusCode(), anonymous.body())
            RemainingOperationHttpSupport.requireNoBusiness(business)
          }
        }
        val token = if (security) "user-without-roles" else ""
        for (family in listOf("sources", "destinations", "connections")) {
          val path = "/api/v1/$family/search"
          // The existing non-null search signatures receive null for an absent or JSON-null body.
          for ((body, status) in listOf("" to 500, "null" to 500, "{" to 400)) {
            val invalid = RemainingOperationHttpSupport.request(server, "POST", path, body, token)
            assertEquals(status, invalid.statusCode(), "$path $body: ${invalid.body()}")
          }
        }
        RemainingOperationHttpSupport.requireNoBusiness(business)
        for (family in listOf("sources", "destinations", "connections")) {
          val path = "/api/v1/$family/search"
          val response = RemainingOperationHttpSupport.request(server, "POST", path, "{}", token)
          assertEquals(if (cloud) 501 else 200, response.statusCode(), response.body())
          if (!cloud) assertEquals(0, Jsons.deserialize(response.body())[family].size())
        }
        if (cloud) {
          RemainingOperationHttpSupport.requireNoBusiness(business)
        } else {
          business.filterIsInstance<SourceHandler>().forEach {
            verify(exactly = 1) { it.searchSources(any()) }
            every { it.searchSources(any()) } throws IOException("search unavailable")
          }
          business.filterIsInstance<DestinationHandler>().forEach { verify(exactly = 1) { it.searchDestinations(any()) } }
          business.filterIsInstance<MatchSearchHandler>().forEach { verify(exactly = 1) { it.searchConnections(any()) } }
          val failure =
            RemainingOperationHttpSupport.request(
              server,
              "POST",
              "/api/v1/sources/search",
              "{}",
              if (security) "user-without-roles" else "",
            )
          assertEquals(500, failure.statusCode(), failure.body())
        }
        controllers
          .flatMap { it.methods.toList() }
          .filterNot { it.getAnnotation(AuthorizeOperation::class.java)?.rule == ConfigSearchRule::class }
          .mapNotNull { it.getAnnotation(AuthorizeOperation::class.java) }
          .map { it.rule.java }
          .filter { it != OperationRule::class.java }
          .distinct()
          .forEach { type ->
            val rule = context.getBean(type)
            verify(exactly = 0) { rule.readInput(any()) }
            verify(exactly = 0) { rule.authorize(any(), any()) }
          }
      },
    )
  }
}
