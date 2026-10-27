/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteMigration
import io.micronaut.context.ApplicationContext
import io.micronaut.core.io.service.SoftServiceLoader
import io.micronaut.http.HttpMethod
import io.micronaut.http.annotation.Controller
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.web.router.Router
import io.mockk.mockkClass
import jakarta.inject.Named
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.yaml.snakeyaml.Yaml
import java.nio.file.Files
import java.nio.file.Path

class RouteAccessCoverageTest {
  @ParameterizedTest
  @ValueSource(booleans = [false, true])
  fun `every active OSS API route has a valid explicit declaration including authentication-only methods`(securityEnabled: Boolean) {
    val controllers =
      SoftServiceLoader
        .load(BeanDefinitionReference::class.java)
        .collectAll()
        .map { it.beanType }
        .filter { it.name.startsWith("io.airbyte.server.apis.") || it.name == "io.airbyte.workload.api.WorkloadApi" }
        .filter { it.isAnnotationPresent(Controller::class.java) }
        .filterNot { it.name.endsWith("\$Intercepted") }
        .toSet()
    val isolated =
      object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
        override fun isWithin(beanDefinitionReference: BeanDefinitionReference<*>): Boolean =
          isWithin(beanDefinitionReference.beanDefinitionName.replace("$", ""))

        override fun isWithin(cls: Class<*>): Boolean = isWithin(cls.name)

        override fun isWithin(className: String): Boolean = className.startsWith("io.airbyte.") && controllers.none { className.startsWith(it.name) }
      }
    ApplicationContext
      .builder()
      .enableDefaultPropertySources(false)
      .deduceEnvironment(false)
      .environmentPropertySource(false)
      .beanConfigurations(isolated)
      .properties(
        mapOf(
          "micronaut.security.enabled" to securityEnabled,
          "micronaut.security.token.jwt.enabled" to securityEnabled,
          "micronaut.security.filter.enabled" to false,
          "micronaut.metrics.enabled" to false,
          "micronaut.executors.io.type" to "cached",
          "micronaut.executors.dsr-deletion.type" to "cached",
        ),
      ).build()
      .use { context ->
        controllers
          .flatMap { type -> type.declaredConstructors.filterNot { it.isSynthetic }.flatMap { it.parameters.toList() } }
          .distinctBy { it.type to it.getAnnotation(Named::class.java)?.value }
          .filterNot { it.type.isPrimitive || it.type == String::class.java }
          .forEach { parameter ->
            @Suppress("UNCHECKED_CAST")
            val type = parameter.type as Class<Any>
            val name = parameter.getAnnotation(Named::class.java)
            if (name == null) {
              context.registerSingleton(type, mockkClass(type.kotlin, relaxed = true))
            } else {
              context.registerSingleton(type, mockkClass(type.kotlin, relaxed = true), Qualifiers.byName(name.value))
            }
          }
        controllers
          .flatMap { it.methods.toList() }
          .mapNotNull { it.getAnnotation(AuthorizeOperation::class.java) }
          .filter { it.migration == RouteMigration.SHARED }
          .map { it.rule.java }
          .distinct()
          .filter { it != OperationRule::class.java }
          .forEach { type ->
            @Suppress("UNCHECKED_CAST")
            val ruleType = type as Class<OperationRule>
            context.registerSingleton(ruleType, mockkClass(ruleType.kotlin, relaxed = true))
          }
        context.registerSingleton(AuthorizationInterceptor::class.java, mockkClass(AuthorizationInterceptor::class, relaxed = true))
        context.start()
        val inventory = RouteAccessInventory(context.getBean(Router::class.java), context)
        assertTrue(inventory.routes().size > 400, "Coverage must use all real OSS controllers")
        val allManagedControllerRoutes =
          context
            .getBean(Router::class.java)
            .uriRoutes()
            .filter { route ->
              (
                route.declaringType.name.startsWith("io.airbyte.server.apis.")
              ) &&
                !route.uriMatchTemplate.toString().startsWith("/scim/")
            }.toList()
        assertEquals(
          allManagedControllerRoutes.map { "${it.httpMethodName} ${it.uriMatchTemplate}" }.toSet() -
            inventory.routes().map { "${it.httpMethodName} ${it.uriMatchTemplate}" }.toSet(),
          emptySet<String>(),
          "Include routes even when a controller path differs from its contract",
        )
        assertTrue(context.getBean(Router::class.java).uriRoutes().anyMatch { it.declaringType.name == "io.airbyte.workload.api.WorkloadApi" })
        assertFalse(
          inventory.routes().any {
            it.declaringType.name == "io.airbyte.workload.api.WorkloadApi"
          },
          "The separate workload API is not Config/public API",
        )
        val entries = routeAccessEntries(inventory)
        assertRemainingProtectedConfigRoutes(inventory, "oss")
        val batch = entries.filter { it.path.startsWith("/api/v1/") && it.family in setOf("sources", "destinations", "connections") }
        val excludedSearches = setOf("/api/v1/sources/search", "/api/v1/destinations/search", "/api/v1/connections/search")
        assertEquals(
          emptyList<String>(),
          batch.filter { it.path !in excludedSearches && it.migration != RouteMigration.SHARED }.map { "${it.httpMethod} ${it.path}" },
          "Every protected operation in the first PLAT-1512 batch must execute a shared rule",
        )
        assertEquals(excludedSearches, batch.filter { it.migration == RouteMigration.LEGACY }.map { it.path }.toSet())
        assertTrue(
          entries.any {
            it.path == "/api/v1/connections/events/get" && it.migration == RouteMigration.SHARED &&
              it.roles == listOf("isAuthenticated()") && it.implementation.endsWith("ConnectionApiController#getConnectionEvent")
          },
        )
        assertTrue(
          entries.any {
            it.path == "/api/v1/sources/write_discover_catalog_result" && it.migration == RouteMigration.SHARED &&
              it.roles == listOf("isAuthenticated()") && it.implementation.endsWith("SourceApiController#writeDiscoverCatalogResult")
          },
        )
        assertTrue(entries.any { it.path == "/api/v1/commands/run/check" && it.migration == RouteMigration.SHARED })
        for (path in listOf("save_stats", "save_stream_metadata")) {
          assertTrue(
            entries.any {
              it.path == "/api/v1/attempt/$path" && it.migration == RouteMigration.SHARED &&
                it.roles == listOf("isAuthenticated()")
            },
          )
        }
        assertFalse(entries.any { it.implementation.contains("wrapped") })
        writeReportAndCheckContract(entries, "oss-${if (securityEnabled) "authenticated" else "no-auth"}", securityEnabled)
      }
  }

  private fun writeReportAndCheckContract(
    entries: List<RouteAccessEntry>,
    edition: String,
    securityEnabled: Boolean,
  ) {
    val root =
      generateSequence(Path.of("").toAbsolutePath()) {
        it.parent
      }.first { Files.exists(it.resolve("oss/airbyte-api/server-api/src/main/openapi/config.yaml")) }
    val spec = Files.newInputStream(root.resolve("oss/airbyte-api/server-api/src/main/openapi/config.yaml")).use { Yaml().load<Map<String, Any>>(it) }

    @Suppress("UNCHECKED_CAST")
    val paths = spec.getValue("paths") as Map<String, Map<String, Any>>
    val methods = HttpMethod.entries.map { it.name.lowercase() }.toSet()
    val contract = paths.flatMap { (path, operations) -> operations.keys.filter { it in methods }.map { "${it.uppercase()} /api$path" } }.toSet()
    val served = entries.map { "${if (it.httpMethod == "HEAD") "GET" else it.httpMethod} ${it.path}" }.toSet()
    val extra = served - contract
    val absent = contract - served
    val output = Path.of("build/reports/route-access")
    Files.createDirectories(output)
    val summary =
      entries.filter { it.httpMethod != "HEAD" }.groupBy { it.family }.toSortedMap().map { (family, routes) ->
        "| $family | ${routes.size} | ${routes.count { it.migration == RouteMigration.LEGACY }} |"
      }
    val rows =
      entries.map { route ->
        listOf(
          route.httpMethod,
          route.path,
          route.access,
          route.migration,
          route.roles,
          route.intent,
          route.implementation,
        ).joinToString(" | ", "| ", " |")
      }
    val report =
      buildString {
        appendLine("# $edition served route inventory")
        appendLine()
        appendLine("Derived from the active Micronaut Router. HEAD routes are automatic GET companions.")
        appendLine("SHARED declarations execute typed rules before controller work; LEGACY routes retain their existing checks.")
        appendLine(
          "The protected Config API is migrated except for the three root searches.",
        )
        appendLine()
        appendLine("| Family | Operations | LEGACY remaining |")
        appendLine("| --- | --- | --- |")
        appendLine(summary.joinToString("\n"))
        appendLine()
        append("| Method | Path | Access | Migration | Secured roles | Intent |")
        appendLine(" Implementation |")
        appendLine("| --- | --- | --- | --- | --- | --- | --- |")
        appendLine(rows.joinToString("\n"))
      }
    Files.writeString(output.resolve("$edition.md"), report)
    val differences =
      "Served without contract:\n" + extra.sorted().joinToString("\n") +
        "\n\nContract without served route:\n" + absent.sorted().joinToString("\n") + "\n"
    Files.writeString(output.resolve("$edition-openapi-differences.txt"), differences)
    println("$edition route inventory: ${entries.size} routes, ${served.size} operations, ${extra.size} extra, ${absent.size} absent")
    // Existing handwritten internal operations are served without generated interfaces.
    val internalOperationsWithoutContract =
      setOf(
        "GET /api/v1/internal/dsr",
        "GET /api/v1/internal/dsr/{requestId}",
        "POST /api/v1/internal/dsr/preview",
        "POST /api/v1/internal/dsr/{requestId}/cancel",
        "POST /api/v1/internal/dsr/{requestId}/execute",
        "POST /api/v1/internal/force_cleanup_connections",
      )
    // Existing unimplemented contracts remain visible here; changing endpoints is outside this ticket.
    val unservedContractOperations = mutableSetOf<String>()
    if (edition.startsWith("oss")) {
      unservedContractOperations +=
        setOf(
          "GET /api/public/v1/oauth/callback",
          "POST /api/v1/destinations/{destinationId}/execute",
          "POST /api/v1/sources/{sourceId}/execute",
        )
    }
    if (!securityEnabled) {
      unservedContractOperations +=
        setOf(
          "GET /api/public/v1/embedded/organizations",
          "POST /api/public/v1/embedded/scoped_token",
          "POST /api/public/v1/embedded/widget_token",
        )
    }
    assertEquals(internalOperationsWithoutContract, extra, "New served routes missing from the generation contract")
    assertEquals(unservedContractOperations, absent, "Unexpected contract operations missing from this edition/configuration")
  }
}
