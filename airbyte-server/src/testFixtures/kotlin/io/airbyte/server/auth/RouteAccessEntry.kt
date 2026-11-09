/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.micronaut.security.annotation.Secured
import io.micronaut.web.router.UriRouteInfo
import java.lang.reflect.Method

fun routeAccessEntries(inventory: RouteAccessInventory): List<RouteAccessEntry> {
  inventory.validate()
  return inventory
    .routes()
    .map { route ->
      val method = implementationMethod(route)
      val metadata = route.annotationMetadata
      RouteAccessEntry(
        route.httpMethodName,
        normalizePath(route.uriMatchTemplate.toString()),
        "${method.declaringClass.name}#${method.name}",
        metadata.enumValue(AuthorizeOperation::class.java, "access", RouteAccessPolicy::class.java).orElseThrow(),
        metadata.classValue(AuthorizeOperation::class.java, "rule").orElse(OperationRule::class.java).name,
        metadata
          .getValue(Secured::class.java, Array<String>::class.java)
          .orElse(emptyArray())
          .toList(),
      )
    }.sortedWith(compareBy(RouteAccessEntry::path, RouteAccessEntry::httpMethod))
}

fun assertSharedAuthorizationAuthority(entries: List<RouteAccessEntry>) {
  for (entry in entries.filter { it.hasSharedRule }) {
    org.junit.jupiter.api.Assertions.assertEquals(
      listOf(io.micronaut.security.rules.SecurityRule.IS_AUTHENTICATED),
      entry.roles,
      "${entry.httpMethod} ${entry.path} must keep authentication and leave authorization to its shared rule",
    )
  }
}

private fun implementationMethod(route: UriRouteInfo<*, *>): Method {
  var type = route.declaringType
  while (type.name.endsWith("\$Intercepted")) type = type.superclass
  return type.getMethod(
    route.targetMethod.methodName,
    *route.targetMethod.arguments
      .map { it.type }
      .toTypedArray(),
  )
}

private fun normalizePath(path: String): String = path.replace(Regex("\\{[?&][^}]*}"), "").replace(Regex("\\{([^}:]+):[^}]+}"), "{$1}").trimEnd('/')

/** The protected Config operation set is frozen independently of active annotations. */
fun assertRemainingProtectedConfigRoutes(
  inventory: RouteAccessInventory,
  edition: String,
) {
  val expected =
    RouteAccessEntry::class.java.getResourceAsStream("/auth/remaining-config-route-rules.tsv")!!.bufferedReader().useLines { lines ->
      lines
        .filterNot { it.startsWith("#") }
        .map { it.split('\t') }
        .filter { it[0] == edition }
        .associate { "${it[1]} ${it[2]}" to it[3] }
    }
  org.junit.jupiter.api.Assertions
    .assertEquals(if (edition == "oss") 311 else 312, expected.size)
  val actual =
    inventory
      .routes()
      .filter {
        it.httpMethodName != "HEAD"
      }.associateBy { "${it.httpMethodName} ${normalizePath(it.uriMatchTemplate.toString())}" }
  for ((operation, rule) in expected) {
    val route = actual[operation] ?: throw AssertionError("Missing frozen operation $operation")
    val declaration = route.annotationMetadata
    org.junit.jupiter.api.Assertions.assertEquals(
      setOf(io.micronaut.security.rules.SecurityRule.IS_AUTHENTICATED),
      declaration.getValue(Secured::class.java, Array<String>::class.java).orElse(emptyArray()).toSet(),
      "$operation must reach the typed rule through Micronaut authentication",
    )
    org.junit.jupiter.api.Assertions.assertEquals(
      RouteAccessPolicy.PROTECTED,
      declaration.enumValue(AuthorizeOperation::class.java, "access", RouteAccessPolicy::class.java).orElseThrow(),
      operation,
    )
    org.junit.jupiter.api.Assertions
      .assertEquals(rule, declaration.classValue(AuthorizeOperation::class.java, "rule").orElseThrow().name, operation)
  }
}

data class RouteAccessEntry(
  val httpMethod: String,
  val path: String,
  val implementation: String,
  val access: RouteAccessPolicy,
  val rule: String,
  val roles: List<String>,
) {
  val hasSharedRule: Boolean get() = access == RouteAccessPolicy.PROTECTED && rule != OperationRule::class.java.name

  val family: String get() = path.removePrefix("/api/public/v1/").removePrefix("/api/v1/").substringBefore('/')
}

/** A focused endpoint fixture keeps its own rule real while satisfying sibling route declarations. */
fun registerSiblingOperationRules(
  context: io.micronaut.context.ApplicationContext,
  controller: Class<*>,
  realRules: Set<Class<out io.airbyte.commons.auth.route.OperationRule>>,
) {
  controller.methods
    .mapNotNull { it.getAnnotation(AuthorizeOperation::class.java) }
    .filter { it.access == RouteAccessPolicy.PROTECTED && it.rule != OperationRule::class }
    .map { it.rule.java }
    .distinct()
    .filter { it !in realRules }
    .forEach { type ->
      @Suppress("UNCHECKED_CAST")
      val ruleType = type as Class<io.airbyte.commons.auth.route.OperationRule>
      context.registerSingleton(ruleType, io.mockk.mockkClass(ruleType.kotlin, relaxed = true))
    }
}
