/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.context.BeanContext
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpRequest
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.authentication.Authentication
import io.micronaut.web.router.RouteAttributes
import io.micronaut.web.router.UriRouteMatch
import jakarta.inject.Singleton
import kotlinx.coroutines.flow.Flow
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.IOException
import java.util.concurrent.CompletionStage
import java.util.concurrent.Future

/** Runs the endpoint rule before controller work with the original parsed arguments. */
@Singleton
class AuthorizedRequest(
  private val beans: BeanContext,
  @Value("\${micronaut.security.enabled:false}") private val securityEnabled: Boolean,
) {
  internal fun execute(context: MethodInvocationContext<Any, Any>): Any? {
    if (unsupportedExecution(context)) throw ForbiddenProblem()
    val request = currentRequest()
    if (!securityEnabled && request.userPrincipal.isEmpty) request.setUserPrincipal(Authentication.build(DEFAULT_USER_ID.toString()))
    val caller = request.getUserPrincipal(Authentication::class.java).orElseThrow { ForbiddenProblem() }
    val routeMatch = RouteAttributes.getRouteMatch(request).orElseThrow { ForbiddenProblem() }
    if (routeMatch !is UriRouteMatch<*, *>) throw ForbiddenProblem()
    val route = routeMatch.routeInfo
    val declared: Class<*> = context.classValue(AuthorizeOperation::class.java, "rule").orElseThrow { ForbiddenProblem() }
    val ruleType = declared.asSubclass(OperationRule::class.java)
    if (ruleType == OperationRule::class.java ||
      route.annotationMetadata.classValue(AuthorizeOperation::class.java, "rule").orElseThrow { ForbiddenProblem() } != ruleType ||
      route.annotationMetadata.enumValue(AuthorizeOperation::class.java, "migration", RouteMigration::class.java).orElseThrow() !=
      RouteMigration.SHARED ||
      route.annotationMetadata.enumValue(AuthorizeOperation::class.java, "access", RouteAccessPolicy::class.java).orElseThrow() !=
      RouteAccessPolicy.PROTECTED ||
      route.targetMethod.methodName != context.methodName ||
      !route.declaringType.isInstance(context.target)
    ) {
      throw ForbiddenProblem()
    }

    val rule = beans.getBean(ruleType)
    val input = rule.readInput(context.parameterValues)

    try {
      rule.authorize(caller, input)
    } catch (failure: AuthorizationResourceNotFoundException) {
      throw ResourceNotFoundProblem().apply { initCause(failure) }
    } catch (failure: IOException) {
      // Use the API's dependency-error contract rather than the HTTP input-error handler for IOException.
      throw UnexpectedProblem().apply { initCause(failure) }
    }

    return context.proceed()
  }

  private fun currentRequest(): HttpRequest<*> = ServerRequestContext.currentRequest<Any>().orElseThrow { ForbiddenProblem() }
}

/** Reject unsupported controller execution declarations before controller work. */
internal fun unsupportedExecution(context: MethodInvocationContext<Any, Any>): Boolean {
  val type = context.returnType.type
  val unsupportedPublisher =
    Publisher::class.java.isAssignableFrom(type) &&
      type !in setOf<Class<*>>(Publisher::class.java, Mono::class.java, Flux::class.java)
  return CompletionStage::class.java.isAssignableFrom(type) || Future::class.java.isAssignableFrom(type) ||
    java.util.stream.BaseStream::class.java.isAssignableFrom(type) || kotlin.sequences.Sequence::class.java.isAssignableFrom(type) ||
    Flow::class.java.isAssignableFrom(type) || context.arguments.any { it.type.name == "kotlin.coroutines.Continuation" } || unsupportedPublisher
}
