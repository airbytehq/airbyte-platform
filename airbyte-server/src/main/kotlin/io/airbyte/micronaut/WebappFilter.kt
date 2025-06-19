/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.resources.Resources
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Filter
import io.micronaut.http.annotation.Filter.MATCH_ALL_PATTERN
import io.micronaut.http.filter.HttpServerFilter
import io.micronaut.http.filter.ServerFilterChain
import org.reactivestreams.Publisher
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.jvm.optionals.getOrNull

private val log = KotlinLogging.logger {}

/**
 * With the webapp now being hosted by Micronaut, we need a way to tell micronaut how to properly
 * handle the single-page-application (SPA).  This [WebappFilter] will automatically redirect (server side,
 * not an http redirect) the request to point to PATH_INDEX_REDIRECT
 *
 * Will redirect any path that doesnt:
 * - start-with a value from [pathPrefixes]
 * - match a value from [webappContents]
 * - equal `/`
 */
@Filter(MATCH_ALL_PATTERN)
class WebappFilter : HttpServerFilter {
  /**
   * Performs the actual filtering.
   */
  override fun doFilter(
    request: HttpRequest<*>,
    chain: ServerFilterChain,
  ): Publisher<MutableHttpResponse<*>> =
    when {
      // don't redirect `/` or PATH_INDEX_REDIRECT requests, but apply the CSP header
      request.path == "/" || request.path == PATH_INDEX_REDIRECT -> chain.proceedWithCsp(request)
      // redirect /authflow to /oauth-callback
      request.path == PATH_AUTH_FLOW -> chain.proceedWithCsp(request.copy(PATH_AUTH_FLOW_REDIRECT))
      // any matching path proceeds as is, sans CSP
      pathPrefixes.any { request.path.startsWith(it) } -> chain.proceed(request)
      // any path that matches a webapp content entry, proceed but apply the CSP header
      webappContents.any { request.path == it } -> chain.proceedWithCsp(request)
      // any other request, directory to PATH_INDEX_REDIRECT, and apply the CSP header
      else -> chain.proceedWithCsp(request.copy(PATH_INDEX_REDIRECT))
    }
}

/**
 * List of webapp contents which should be ignored by this filter.
 *
 * E.g. if /index.css is requested, we don't want to redirect that to [PATH_INDEX_REDIRECT]
 */
@InternalForTesting
internal val webappContents = Resources.list("webapp").map { "/$it" }.also { log.debug { "webapp contents: $it" } }

/** The auth_flow path has a specific redirect, see [PATH_AUTH_FLOW_REDIRECT]. */
@InternalForTesting
internal const val PATH_AUTH_FLOW = "/auth_flow"

/** Where the [PATH_AUTH_FLOW] should redirect to. */
@InternalForTesting
internal const val PATH_AUTH_FLOW_REDIRECT = "/oauth-callback.html"

/** Where any non-matching request gets redirected. */
@InternalForTesting
internal const val PATH_INDEX_REDIRECT = "/index.html"

/**
 * List path prefixes which MUST NOT be redirected to the [PATH_INDEX_REDIRECT] page.
 */
@InternalForTesting
internal val pathPrefixes =
  listOf(
    // all api endpoints starts with /api
    "/api",
    // the assets are the `webapp/assets` and are handled by a static-resource (see the application.yml)
    "/assets",
    // the fonts are the `webapp/fonts` and are handled by a static-resource (see the application.yml)
    "/fonts",
    // routes is a special endpoint
    "/routes",
  )

@InternalForTesting
internal const val CSP_HEADER = "Content-Security-Policy"

@InternalForTesting
internal const val CSP_VALUE = "script-src * 'unsafe-inline'; worker-src 'self' blob:;"

private fun ServerFilterChain.proceedWithCsp(request: HttpRequest<*>): Publisher<MutableHttpResponse<*>> =
  Publishers.map(this.proceed(request)) {
    it.header(CSP_HEADER, CSP_VALUE)
    it
  }

/**
 * Creates a copy of an [HttpRequest] object.  Keeps the body and headers of the original request, but replaces the uri with [uri].
 */
@InternalForTesting
internal fun HttpRequest<*>.copy(uri: String): HttpRequest<*> =
  HttpRequest
    .create<Any>(this.method, uri)
    .apply {
      // copy the headers
      this@copy.headers.forEach { (key, values) -> values.forEach { header(key, it) } }
      // copy the body
      this@copy.body.getOrNull()?.let { body(it) }
    }
