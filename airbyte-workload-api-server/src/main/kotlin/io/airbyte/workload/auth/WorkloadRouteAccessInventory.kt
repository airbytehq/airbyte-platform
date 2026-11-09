/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.server.auth.AuthorizationInterceptor
import io.micronaut.context.BeanContext
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.micronaut.web.router.Router
import io.micronaut.web.router.UriRouteInfo
import jakarta.inject.Singleton

@Singleton
class WorkloadRouteAccessInventory(
  private val router: Router,
  private val beans: BeanContext,
) {
  @Volatile
  private var validatedRoutes: Set<UriRouteInfo<*, *>> = emptySet()

  fun routes(): List<UriRouteInfo<*, *>> = router.uriRoutes().filter(::isManaged).toList()

  fun validate() {
    val activeRoutes = routes()
    val errors = activeRoutes.flatMap { route -> violations(route).map { "${route.httpMethodName} ${route.uriMatchTemplate}: $it" } }
    check(errors.isEmpty()) { "Invalid workload API access declarations:\n${errors.joinToString("\n")}" }
    validatedRoutes = activeRoutes.toSet()
  }

  fun isValidated(route: UriRouteInfo<*, *>): Boolean = route in validatedRoutes

  fun violations(route: UriRouteInfo<*, *>): List<String> {
    if (!isManaged(route)) return emptyList()
    val type = route.declaringType
    var implementation = type
    while (implementation.name.endsWith("\$Intercepted")) implementation = implementation.superclass
    val method =
      implementation.getMethod(
        route.targetMethod.methodName,
        *route.targetMethod.arguments
          .map { it.type }
          .toTypedArray(),
      )
    val declaration = method.getAnnotation(AuthorizeOperation::class.java) ?: return listOf("missing method-level @AuthorizeOperation")
    val errors = mutableListOf<String>()
    if (declaration.access != RouteAccessPolicy.PROTECTED) errors += "workload route must be protected"
    val securedRoles =
      route.annotationMetadata
        .getValue(Secured::class.java, Array<String>::class.java)
        .orElse(emptyArray())
        .toList()
    if (securedRoles != listOf(SecurityRule.IS_AUTHENTICATED)) errors += "workload route requires authentication-only @Secured"
    val rule = declaration.rule.java
    if (rule == OperationRule::class.java || rule.isInterface ||
      java.lang.reflect.Modifier
        .isAbstract(rule.modifiers) || !beans.containsBean(rule)
    ) {
      errors += "workload route requires a concrete DI rule"
    }
    if (!beans.containsBean(AuthorizationInterceptor::class.java) || !beans.getBeanDefinition(type).isProxy) {
      errors += "workload route requires the checker and controller advice"
    }
    return errors
  }

  companion object {
    fun isManaged(route: UriRouteInfo<*, *>): Boolean {
      val path = route.uriMatchTemplate.toString()
      return path == "/api/v1/workload" || path.startsWith("/api/v1/workload/")
    }
  }
}
