/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.micronaut.security.annotation.Secured
import io.micronaut.web.router.UriRouteInfo
import java.lang.reflect.Method

fun routeAccessEntries(inventory: RouteAccessInventory): List<RouteAccessEntry> {
  inventory.validate()
  return inventory
    .routes()
    .map { route ->
      val method = implementationMethod(route)
      val declaration = method.getAnnotation(AuthorizeOperation::class.java)
      RouteAccessEntry(
        route.httpMethodName,
        normalizePath(route.uriMatchTemplate.toString()),
        "${method.declaringClass.name}#${method.name}",
        declaration.access,
        declaration.migration,
        route.annotationMetadata
          .getValue(Secured::class.java, Array<String>::class.java)
          .orElse(emptyArray())
          .toList(),
        route.annotationMetadata
          .stringValue(RequiresIntent::class.java)
          .stream()
          .toList(),
      )
    }.sortedWith(compareBy(RouteAccessEntry::path, RouteAccessEntry::httpMethod))
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

data class RouteAccessEntry(
  val httpMethod: String,
  val path: String,
  val implementation: String,
  val access: RouteAccessPolicy,
  val migration: RouteMigration,
  val roles: List<String>,
  val intent: List<String>,
) {
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
    .filter { it.migration == RouteMigration.SHARED }
    .map { it.rule.java }
    .distinct()
    .filter { it !in realRules }
    .forEach { type ->
      @Suppress("UNCHECKED_CAST")
      val ruleType = type as Class<io.airbyte.commons.auth.route.OperationRule>
      context.registerSingleton(ruleType, io.mockk.mockkClass(ruleType.kotlin, relaxed = true))
    }
}
