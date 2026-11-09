/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.order.Ordered
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.RequestFilter
import io.micronaut.http.annotation.ServerFilter
import io.micronaut.http.filter.FilterContinuation
import io.micronaut.http.filter.ServerFilterPhase
import io.micronaut.web.router.RouteAttributes
import io.micronaut.web.router.UriRouteMatch
import org.reactivestreams.Publisher

private val logger = KotlinLogging.logger {}

@ServerFilter("/**")
class WorkloadRouteAccessFilter(
  private val inventory: WorkloadRouteAccessInventory,
) : Ordered {
  override fun getOrder(): Int = ServerFilterPhase.SECURITY.after()

  @RequestFilter
  fun filter(
    request: HttpRequest<*>,
    continuation: FilterContinuation<Publisher<HttpResponse<*>>>,
  ): Publisher<HttpResponse<*>> {
    val matched = RouteAttributes.getRouteMatch(request)
    if (matched.isPresent && matched.get() is UriRouteMatch<*, *>) {
      val route = (matched.get() as UriRouteMatch<*, *>).routeInfo
      if (WorkloadRouteAccessInventory.isManaged(route) && !inventory.isValidated(route)) {
        logger.error { "Blocking unvalidated workload route ${route.httpMethodName} ${route.uriMatchTemplate}" }
        throw ForbiddenProblem()
      }
    }
    return continuation.proceed()
  }
}
