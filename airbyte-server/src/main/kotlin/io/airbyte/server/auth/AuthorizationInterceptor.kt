/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteMigration
import io.micronaut.aop.InterceptorBean
import io.micronaut.aop.MethodInterceptor
import io.micronaut.aop.MethodInvocationContext
import jakarta.inject.Singleton

@Singleton
@InterceptorBean(AuthorizeOperation::class)
class AuthorizationInterceptor(
  private val authorizedRequest: AuthorizedRequest,
) : MethodInterceptor<Any, Any> {
  override fun intercept(context: MethodInvocationContext<Any, Any>): Any? =
    if (context.enumValue(AuthorizeOperation::class.java, "migration", RouteMigration::class.java).orElseThrow() == RouteMigration.SHARED) {
      authorizedRequest.execute(context)
    } else {
      context.proceed()
    }
}
