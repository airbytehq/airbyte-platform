/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.filters

import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Filter
import io.micronaut.http.filter.HttpServerFilter
import io.micronaut.http.filter.ServerFilterChain
import org.reactivestreams.Publisher

/** Normalizes request-binding failures to the SCIM lifecycle API's documented 422 response. */
@Filter("/api/v1/scim_config/**")
class ScimConfigValidationFilter : HttpServerFilter {
  override fun doFilter(
    request: HttpRequest<*>,
    chain: ServerFilterChain,
  ): Publisher<MutableHttpResponse<*>> =
    Publishers.map(chain.proceed(request)) { response ->
      if (response.status == HttpStatus.BAD_REQUEST) {
        response.status(HttpStatus.UNPROCESSABLE_ENTITY)
      } else {
        response
      }
    }
}
