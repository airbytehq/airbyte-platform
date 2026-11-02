/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.micronaut.http.uri.UriMatchTemplate
import org.junit.jupiter.api.Assertions.assertEquals
import java.net.URI

fun assertPublicRouteHttpCoverage(
  entries: List<RouteAccessEntry>,
  securityEnabled: Boolean,
) {
  val served = entries.filter { it.path.startsWith("/api/public/") && it.access == RouteAccessPolicy.PROTECTED && it.httpMethod != "HEAD" }
  val covered =
    publicApiHttpScenarios()
      .values
      .flatten()
      .filter { securityEnabled || !it.path.contains("/embedded/") }
      .map { request ->
        val route = served.single { it.httpMethod == request.method && UriMatchTemplate.of(it.path).match(URI(request.path).path).isPresent }
        "${route.httpMethod} ${route.path}"
      }.toSet()
  assertEquals(
    served
      .map {
        "${it.httpMethod} ${it.path}"
      }.toSet(),
    covered,
    "Every served protected public operation needs an explicit real HTTP request, including Cloud inherited routes",
  )
}
