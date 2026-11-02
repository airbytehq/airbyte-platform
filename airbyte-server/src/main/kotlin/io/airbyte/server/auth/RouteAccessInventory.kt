/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.ConfigSearchAccessException
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteAccessPolicy
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
  @Volatile
  private var validatedRoutes: Set<UriRouteInfo<*, *>> = emptySet()

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
    val activeRoutes = routes()
    val errors = activeRoutes.flatMap { route -> violations(route).map { "${route.httpMethodName} ${route.uriMatchTemplate}: $it" } }
    check(errors.isEmpty()) { "Invalid API access declarations:\n${errors.joinToString("\n")}" }
    validatedRoutes = activeRoutes.toSet()
  }

  fun isValidated(route: UriRouteInfo<*, *>): Boolean = route in validatedRoutes

  fun violations(route: UriRouteInfo<*, *>): List<String> {
    if (!isManaged(route)) return emptyList()
    val method = implementationMethod(route)
    val declaration = method.getAnnotation(AuthorizeOperation::class.java) ?: return listOf("missing method-level @AuthorizeOperation on $method")
    val errors = mutableListOf<String>()
    val roles = securedRoles(route).toSet()
    val anonymous = SecurityRule.IS_ANONYMOUS in roles
    val searchException = method.isAnnotationPresent(ConfigSearchAccessException::class.java)
    if (route.annotationMetadata.hasAnnotation(ConfigSearchAccessException::class.java) != searchException) {
      errors += "search exception must be declared on the implementation method"
    }
    if (searchException) {
      if (route.httpMethodName != "POST" || route.uriMatchTemplate.toString() !in configSearchPaths ||
        declaration.access != RouteAccessPolicy.PROTECTED || declaration.rule.java != OperationRule::class.java
      ) {
        errors += "search exception requires one of the three protected Config POST searches without a rule"
      }
    }
    if (declaration.access == RouteAccessPolicy.PROTECTED) {
      if (securedRoles(route) != listOf(SecurityRule.IS_AUTHENTICATED)) errors += "protected route requires authentication-only @Secured"
      if (!searchException) errors += sharedViolations(route, declaration)
    } else {
      if (declaration.rule.java != OperationRule::class.java) errors += "public/login routes cannot have an operation rule"
      if (!anonymous) {
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
      return listOf("protected route requires a concrete DI rule")
    }
    if (!beans.containsBean(AuthorizationInterceptor::class.java) ||
      !beans.getBeanDefinition(route.declaringType).isProxy
    ) {
      return listOf("protected route requires the checker and controller advice")
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
    private val configSearchPaths = setOf("/api/v1/sources/search", "/api/v1/destinations/search", "/api/v1/connections/search")
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
