/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.filters

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LoadShedPublicApi
import io.airbyte.featureflag.TokenSubject
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Filter
import io.micronaut.http.filter.HttpServerFilter
import io.micronaut.http.filter.ServerFilterChain
import io.micronaut.http.filter.ServerFilterPhase
import org.reactivestreams.Publisher
import kotlin.jvm.optionals.getOrNull

private val log = KotlinLogging.logger {}

/**
 * Sheds public API traffic for a specific authenticated user when the [LoadShedPublicApi] feature
 * flag is targeted at their token subject.
 *
 * Manual, operator-driven kill switch for incident response: when a single user spikes traffic,
 * target their token subject in the flag and their public-API requests are rejected with a 429
 * before they consume a public-API executor thread, protecting other tenants. Mirrors the
 * sync-path load-shed pattern (`LoadShedWorkloadLauncher`), moved to the inbound API.
 *
 * The key is the token subject (the JWT `sub` / `X-Airbyte-External-Auth-Id`), read straight off
 * the request principal that the security filter already resolved — so this adds no token
 * resolution: one already-bound field read plus one in-memory flag evaluation on the hot path.
 *
 * Granularity is **per-user, not per-Application**: for a public-API Application (client-credentials)
 * token, the `sub` is the `authUserId` of the user who *created* the Application, so every
 * Application that user created shares it. Shedding therefore hits that user across all of their
 * Applications — it is not per-Application, per-credential, or per-organization. That `sub` is the
 * value exposed in Datadog as `airbyte.metadata.authUserId`, which is what an operator targets in
 * the flag.
 *
 * Runs after [ServerFilterPhase.SECURITY] so the principal is populated. Defensive throughout —
 * a failure here must never reject legitimate traffic, so any error fails open (proceeds).
 */
@Filter(PUBLIC_API_PATH_PATTERN)
class PublicApiLoadShedFilter(
  private val featureFlagClient: FeatureFlagClient,
  private val metricClient: MetricClient,
) : HttpServerFilter {
  override fun getOrder(): Int = ServerFilterPhase.SECURITY.after()

  override fun doFilter(
    request: HttpRequest<*>,
    chain: ServerFilterChain,
  ): Publisher<MutableHttpResponse<*>> {
    val tokenSubject = tokenSubject(request)
    if (tokenSubject != null && shouldShed(tokenSubject)) {
      log.info { "Load shedding public API request ${request.method} ${request.path} for token subject $tokenSubject" }
      // Tag by token subject so Datadog shows *who* is being throttled. Cardinality is bounded: this
      // counter only fires for requests we actually shed, i.e. the small set of subjects an operator
      // has explicitly targeted in the flag — not every caller.
      metricClient.count(OssMetricsRegistry.PUBLIC_API_LOAD_SHED, 1L, MetricAttribute(MetricTags.TOKEN_SUBJECT, tokenSubject))
      // Deliberately no Retry-After header: this is a hard rate limit, not a "back off and try again"
      // signal. We do not want the shed caller to keep retrying while the flag is engaged.
      return Publishers.just(
        HttpResponse
          .status<Any>(HttpStatus.TOO_MANY_REQUESTS)
          .body(mapOf("message" to LOAD_SHED_MESSAGE)),
      )
    }
    return chain.proceed(request)
  }

  private fun tokenSubject(request: HttpRequest<*>): String? =
    try {
      request.userPrincipal.getOrNull()?.name
    } catch (e: Exception) {
      // Never let identity resolution break the request path; fall through and proceed.
      log.error(e) { "Failed to resolve token subject for load shedding" }
      null
    }

  private fun shouldShed(tokenSubject: String): Boolean =
    try {
      featureFlagClient.boolVariation(LoadShedPublicApi, TokenSubject(tokenSubject))
    } catch (e: Exception) {
      // Fail open: if the flag can't be evaluated, do not shed.
      log.error(e) { "Failed to evaluate ${LoadShedPublicApi.key} for token subject $tokenSubject" }
      false
    }
}

const val PUBLIC_API_PATH_PATTERN = "/api/public/**"

internal const val LOAD_SHED_MESSAGE = "You are being rate limited."
