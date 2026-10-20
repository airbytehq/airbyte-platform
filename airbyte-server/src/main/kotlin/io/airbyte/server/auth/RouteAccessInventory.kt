/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.workload.api.WorkloadApi
import io.micronaut.context.BeanContext
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.micronaut.web.router.Router
import io.micronaut.web.router.UriRouteInfo
import jakarta.inject.Singleton
import java.lang.reflect.Method

/** Uses active routes, so disabled controllers and replaced OSS implementations are not phantom entries. */
@Singleton
class RouteAccessInventory(
  private val router: Router,
  private val beans: BeanContext,
) {
  fun routes(): List<UriRouteInfo<*, *>> =
    router.uriRoutes().filter(::isManaged).toList().distinctBy {
      listOf(
        it.httpMethodName,
        it.uriMatchTemplate.toString(),
        it.declaringType.name,
        it.targetMethod.methodName,
        it.targetMethod.arguments.joinToString { argument ->
          argument.type.name
        },
      )
    }

  fun validate() {
    val errors = routes().flatMap { route -> violations(route).map { "${route.httpMethodName} ${route.uriMatchTemplate}: $it" } }
    check(errors.isEmpty()) { "Invalid API access declarations:\n${errors.joinToString("\n")}" }
  }

  fun violations(route: UriRouteInfo<*, *>): List<String> {
    if (!isManaged(route)) return emptyList()
    val method = implementationMethod(route)
    val declaration = method.getAnnotation(AuthorizeOperation::class.java) ?: return listOf("missing method-level @AuthorizeOperation on $method")
    val errors = mutableListOf<String>()
    val roles = securedRoles(route).toSet()
    val anonymous = SecurityRule.IS_ANONYMOUS in roles
    if (declaration.migration == RouteMigration.SHARED) errors += sharedViolations(route, declaration)
    if (declaration.access == RouteAccessPolicy.PROTECTED) {
      if (declaration.migration == RouteMigration.NOT_APPLICABLE) errors += "protected routes require a migration status"
      if (anonymous) errors += "protected policy conflicts with anonymous security metadata"
    } else {
      if (declaration.migration != RouteMigration.NOT_APPLICABLE) errors += "public/login routes cannot have a protected migration status"
      if (!anonymous ||
        route.annotationMetadata.hasAnnotation(RequiresIntent::class.java)
      ) {
        errors += "public/login policy conflicts with existing security metadata"
      }
    }
    return errors
  }

  private fun sharedViolations(
    route: UriRouteInfo<*, *>,
    declaration: AuthorizeOperation,
  ): List<String> {
    val rule = declaration.rule.java
    if (rule == OperationRule::class.java || rule.isInterface ||
      java.lang.reflect.Modifier
        .isAbstract(rule.modifiers) ||
      !beans.containsBean(rule)
    ) {
      return listOf("SHARED requires a concrete DI rule")
    }
    if (!beans.containsBean(AuthorizationInterceptor::class.java) ||
      !beans.getBeanDefinition(route.declaringType).isProxy
    ) {
      return listOf("SHARED requires the checker and controller advice")
    }
    return emptyList()
  }

  // Match SecuredAnnotationRule: a method annotation overrides its controller annotation.
  private fun securedRoles(route: UriRouteInfo<*, *>): List<String> =
    route.annotationMetadata
      .getValue(Secured::class.java, Array<String>::class.java)
      .orElse(emptyArray())
      .toList()

  private fun implementationMethod(route: UriRouteInfo<*, *>): Method {
    var type = route.declaringType
    // Micronaut-generated AOP subclasses delegate to the real implementation.
    while (type.name.endsWith("\$Intercepted")) type = type.superclass
    return type.getMethod(
      route.targetMethod.methodName,
      *route.targetMethod.arguments
        .map { it.type }
        .toTypedArray(),
    )
  }

  companion object {
    private val managedBasePaths = listOf("/api/v1", "/api/public", "/public/v1")
    private val managedControllerPrefixes =
      listOf(
        "io.airbyte.server.apis.",
        "io.airbyte.server.wrapped.apis.",
        "io.airbyte.server.wrapped.sonar.",
        "io.airbyte.server.wrapped.InstanceConfigurationApiWrappedController",
      )

    fun isManaged(route: UriRouteInfo<*, *>): Boolean {
      // The workload API has its own contract and authorization, even when present on this classpath.
      if (route.declaringType == WorkloadApi::class.java) return false
      val path = route.uriMatchTemplate.toString()
      if (path == "/scim/v2" || path.startsWith("/scim/v2/")) return false
      if (managedBasePaths.any { path == it || path.startsWith("$it/") }) return true
      return managedControllerPrefixes.any(route.declaringType.name::startsWith)
    }
  }
}
