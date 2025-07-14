/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.resources.Resources
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Filter
import io.micronaut.http.annotation.Filter.MATCH_ALL_PATTERN
import io.micronaut.http.filter.HttpServerFilter
import io.micronaut.http.filter.ServerFilterChain
import org.reactivestreams.Publisher
import java.time.Clock
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.jvm.optionals.getOrNull

private val log = KotlinLogging.logger {}

/**
 * With the webapp now being hosted by Micronaut, we need a way to tell micronaut how to properly
 * handle the single-page-application (SPA).  This [WebappFilter] will automatically redirect (server side,
 * not an http redirect) the request to point to PATH_INDEX
 *
 * Will redirect any path that doesn't:
 * - start-with a value from [pathPrefixes]
 * - match a value from [webappContents]
 * - equal `/`
 */
@Filter(MATCH_ALL_PATTERN)
class WebappFilter : HttpServerFilter {
  var clock: Clock = Clock.system(ZoneOffset.UTC)

  /**
   * Performs the actual filtering.
   */
  override fun doFilter(
    request: HttpRequest<*>,
    chain: ServerFilterChain,
  ): Publisher<MutableHttpResponse<*>> =
    when {
      // don't redirect `/` or PATH_INDEX requests, but apply the CSP header
      request.path == "/" || request.path == PATH_INDEX ->
        chain.modifyResponse(request) {
          it.setCspHeader()
          it.setLastModifiedHeader()
        }
      // redirect /authflow to /oauth-callback
      request.path == PATH_AUTH_FLOW ->
        chain.modifyResponse(request.copy(PATH_AUTH_FLOW_REDIRECT)) {
          it.setCspHeader()
        }
      // any matching path proceeds as is, sans CSP
      pathPrefixes.any { request.path.startsWith(it) } -> chain.proceed(request)
      // any path that matches a webapp content entry, proceed but apply the CSP header
      webappContents.any { request.path == it } ->
        chain.modifyResponse(request) {
          it.setCspHeader()
        }
      // any other request, direct to PATH_INDEX, and apply the CSP header
      else ->
        chain.modifyResponse(request.copy(PATH_INDEX)) {
          it.setCspHeader()
          it.setLastModifiedHeader()
        }
    }

  // We don't preserve timestamps of resources packed into a JAR file.
  // See: https://github.com/airbytehq/airbyte-gradle/blob/4a8d7f18d5ffe279456a4cde8a21a463ed31bfa6/src/main/kotlin/plugins/AirbyteJvmPlugin.kt#L142
  // and:https://docs.gradle.org/current/userguide/working_with_files.html#sec:reproducible_archives
  //
  // That means that the index.html file (and all webapp assets) have a "last-modified" http response
  // header of zero (the unix epoch). This can cause Google CDN to return HTTP 304 responses for assets.
  //
  // We don't want the index page to be cached – it contains all the references to the JS/CSS bundles.
  // We return a last-modified header of "now" for the index page to prevent 304 responses.
  private fun MutableHttpResponse<*>.setLastModifiedHeader() =
    apply {
      header(HttpHeaders.LAST_MODIFIED, clock.instant().atZone(ZoneOffset.UTC).format(lastModifiedFormatter))
    }
}

/**
 * List of webapp contents which should be ignored by this filter.
 *
 * E.g. if /index.css is requested, we don't want to redirect that to [PATH_INDEX]
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
internal const val PATH_INDEX = "/index.html"

/**
 * List path prefixes which MUST NOT be redirected to the [PATH_INDEX] page.
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

private fun MutableHttpResponse<*>.setCspHeader() =
  apply {
    header(CSP_HEADER, CSP_VALUE)
  }

private fun ServerFilterChain.modifyResponse(
  request: HttpRequest<*>,
  block: (MutableHttpResponse<*>) -> MutableHttpResponse<*>,
): Publisher<MutableHttpResponse<*>> = Publishers.map(this.proceed(request), block)

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

private val lastModifiedFormatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US)
