/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.metrics

import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.RequestReferences
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Timer
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Filter
import io.micronaut.http.filter.HttpServerFilter
import io.micronaut.http.filter.ServerFilterChain
import io.micronaut.http.filter.ServerFilterPhase
import org.reactivestreams.Publisher

private const val TRACE_ATTR = "io.airbyte.trace"

/**
 * TracingServerFilter records telemetry for all API traffic:
 *   - it records attributes to trace spans,
 *   - it records a metric named "airbyte.api_trace" which counts the number of requests,
 *     and captures a distribution of the request duration.
 *
 *  The attributes recorded on the metric and trace spans include:
 *    - http details (e.g. method, route, status)
 *    - referenced Airbyte IDs (e.g. workspace ID, org ID, source ID, etc)
 *    - error details
 */
@Filter("/api/**")
class TracingServerFilter(
  val authenticationHeaderResolver: AuthenticationHeaderResolver,
  val metricClient: MetricClient,
) : HttpServerFilter {
  private val logger = KotlinLogging.logger {}

  override fun getOrder(): Int = ServerFilterPhase.TRACING.after()

  fun traceRequest(req: HttpRequest<*>) {
    val trace = Trace()

    // Store the trace in the http request attributes, so that we can finalize
    // the trace in the response handler traceResponse() below.
    req.setAttribute(TRACE_ATTR, trace)
  }

  fun traceResponse(
    req: HttpRequest<*>,
    resp: HttpResponse<*>,
    throwable: Throwable?,
  ) {
    // This should never be allowed to fail, as it could cause all API traffic to fail,
    // so be extra cautious here and surround everything with try/catch.
    try {
      val trace = req.getAttribute(TRACE_ATTR, Trace::class.java).orElseThrow()
      try {
        val references = RequestReferences.from(req)
        trace.refs.putAll(references.traceValues())
        val properties = references.asScopeProperties()
        if (trace.refs["workspaceId"] == null) {
          trace.refs["workspaceId"] = authenticationHeaderResolver.resolveWorkspace(properties)?.joinToString { it.toString() }
        }
        if (trace.refs["organizationId"] == null) {
          trace.refs["organizationId"] = authenticationHeaderResolver.resolveOrganization(properties)?.joinToString { it.toString() }
        }
      } catch (e: Exception) {
        logger.error(e) { "failed to resolve trace references" }
      }
      ApmTraceUtils.addTagsToTrace(trace.refs.toMap())

      val metricAttrs = mutableListOf<MetricAttribute>()

      // Only record a subset of attributes to the metric,
      // because each unique tag value costs money in Datadog.
      metricAttrs.add("refs", trace.refs, listOf("workspaceId", "organizationId"))

      if (throwable != null) {
        metricAttrs.add(MetricAttribute("error", "true"))
        metricAttrs.add(MetricAttribute("error_class", throwable.javaClass.name))
        ApmTraceUtils.recordErrorOnRootSpan(throwable)
      }

      metricClient
        .timer(OssMetricsRegistry.API_TRACE, *metricAttrs.toTypedArray())
        ?.also { trace.start.stop(it) }
    } catch (e: Exception) {
      logger.error(e) { "failed to trace response" }
    }
  }

  override fun doFilter(
    request: HttpRequest<*>,
    chain: ServerFilterChain,
  ): Publisher<MutableHttpResponse<*>> {
    traceRequest(request)
    return Publishers.map(chain.proceed(request), {
      traceResponse(request, it, null)
      it
    })
  }
}

private data class Trace(
  val start: Timer.Sample = Timer.start(),
  // The code here makes it seems like these values can be non-null Stings,
  // but the nullability integration between java/kotlin isn't perfect,
  // and it's possible to pass a null String values to a non-nullable String in Kotlin.
  val refs: MutableMap<String, String?> = mutableMapOf(),
  var traceError: Throwable? = null,
)

private fun MutableList<MetricAttribute>.add(
  prefix: String,
  attrs: Map<String, String?>,
  keys: List<String>,
) {
  attrs
    .filterValues { it != null }
    .filterKeys { keys.contains(it) }
    .forEach { (k, v) ->
      this.add(MetricAttribute("$prefix.$k", v!!))
    }
}
