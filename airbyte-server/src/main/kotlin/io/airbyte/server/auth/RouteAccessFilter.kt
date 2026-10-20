/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Filter
import io.micronaut.http.filter.HttpServerFilter
import io.micronaut.http.filter.ServerFilterChain
import io.micronaut.http.filter.ServerFilterPhase
import io.micronaut.web.router.RouteAttributes
import io.micronaut.web.router.UriRouteMatch
import org.reactivestreams.Publisher

/** Independent of SecurityRule short-circuiting: metadata never grants access to a LEGACY route. */
@Filter("/**")
@Requires(property = "micronaut.security.enabled", value = "true")
class RouteAccessFilter(
  private val inventory: RouteAccessInventory,
) : HttpServerFilter {
  override fun getOrder(): Int = ServerFilterPhase.SECURITY.after()

  override fun doFilter(
    request: HttpRequest<*>,
    chain: ServerFilterChain,
  ): Publisher<MutableHttpResponse<*>> {
    val route = RouteAttributes.getRouteMatch(request).orElse(null)
    if (route is UriRouteMatch<*, *> && inventory.violations(route.routeInfo).isNotEmpty()) {
      return Publishers.just(HttpResponse.status<Any>(HttpStatus.FORBIDDEN))
    }
    return chain.proceed(request)
  }
}
