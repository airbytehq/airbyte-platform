/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.airbyte.config.persistence.UserPersistence
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Timer
import io.micronaut.context.annotation.Requires
import io.micronaut.http.BasicHttpAttributes
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.RequestFilter
import io.micronaut.http.annotation.ResponseFilter
import io.micronaut.http.annotation.ServerFilter
import kotlin.jvm.optionals.getOrNull

private val logger = KotlinLogging.logger {}
private const val TRACE_ATTR = "io.airbyte.trace"

/**
 * TracingServerFilter records telemetry for all API traffic:
 *   - it records attributes to trace spans,
 *   - it records a metric named "airbyte.api_trace" which counts the number of requests,
 *     and captures a distribution of the request duration.
 *
 *  The attributes recorded on the metric and trace spans include:
 *    - user details (e.g. user ID)
 *    - http details (e.g. method, route, status)
 *    - referenced Airbyte IDs (e.g. workspace ID, org ID, source ID, etc)
 *    - error details
 *
 *  Note: This filter will only be available if the [UserPersistence] bean is also available.  The reason for this dependency
 *  is that both implementations of the [CurrentUserService] have a dependency on [UserPersistence] but not every `/api/` endpoint
 *  (i.e. connector-builder) has access to the database, which [UserPersistence] requires. A better solution should be sought here.
 */
@ServerFilter("/api/**")
@Requires(beans = [UserPersistence::class])
class TracingServerFilter(
  val currentUserService: CurrentUserService,
  val authenticationHeaderResolver: AuthenticationHeaderResolver,
  val metricClient: MetricClient,
) {
  @RequestFilter
  fun traceRequest(req: HttpRequest<*>) {
    val trace = Trace()

    // Store the trace in the http request attributes, so that we can finalize
    // the trace in the response handler traceResponse() below.
    req.setAttribute(TRACE_ATTR, trace)

    // This should never be allowed to fail, as it could cause all API traffic to fail,
    // so be extra cautious here and surround everything with try/catch.
    try {
      // trace the current user
      try {
        currentUserService.getCurrentUser()?.also {
          trace.user["userId"] = it.userId.toString()
          trace.user["authUserId"] = it.authUserId
          trace.user["authProvider"] = it.authProvider.name
          trace.user["companyName"] = it.companyName
        }
      } catch (e: Exception) {
        // This leads to super noisy logs currently,
        // because some endpoints (such as health) never have a user.
        // The CurrentUserService should probably return an Optional (or kotlin nullable)
        // instead of throwing an exception to distinguish between expected + no user vs unexpected error.
        // logger.debug(e) { "failed to get current user" }
      }

      // trace http details
      try {
        trace.http["method"] = req.method.name
        trace.http["host"] = req.uri.host
        trace.http["scheme"] = req.uri.scheme
        trace.http["path"] = req.uri.path
        // controller route pattern, e.g. "/api/public/v1/workspaces/{workspaceId}"
        trace.http["route"] = BasicHttpAttributes.getUriTemplate(req).orElse("unknown")
        trace.http["user_agent"] = req.headers.get("User-Agent")

        for (param in req.parameters) {
          trace.http["param.${param.key}"] = param.value.joinToString(",")
        }
      } catch (e: Exception) {
        logger.error(e) { "failed to get http details" }
      }

      for (entry in AuthenticationId.entries) {
        // trace IDs found in the request: workspace, organization, etc.
        // This relies on AuthenticationHeaderResolver.
        if (req.headers.contains(entry.httpHeader)) {
          trace.refs[entry.fieldName] = req.headers.getAll(entry.httpHeader).joinToString(",") { it.toString() }
        }
      }

      val authProps = req.headers.asMap(String::class.java, String::class.java).toMutableMap()

      // pull variables from the route
      // e.g. pull workspaceId from a call to "/api/public/v1/workspaces/{workspaceId}"
      val routeVars = getRouteVariables(req)
      for (entry in routeVars.entries) {
        trace.refs[entry.key] = entry.value.toString()

        // The AuthorizationServerHandler doesn't currently support micronaut route variables
        // but we need them to resolve workspaceId and organizationId below (mainly for the public API).
        // This adds route variables to the authProps that are used to resolve those IDs below.
        AuthenticationId.entries
          .firstOrNull { it.fieldName == entry.key }
          ?.also { authProps[it.httpHeader] = entry.value.toString() }
      }

      // resolve workspace and organization IDs
      if (trace.refs["workspaceId"] == null) {
        trace.refs["workspaceId"] = authenticationHeaderResolver.resolveWorkspace(authProps)?.joinToString { it.toString() }
      }
      if (trace.refs["organizationId"] == null) {
        trace.refs["organizationId"] = authenticationHeaderResolver.resolveOrganization(authProps)?.joinToString { it.toString() }
      }
    } catch (e: Exception) {
      logger.error(e) { "failed to trace request" }
      trace.traceError = e
    }
  }

  @ResponseFilter
  fun traceResponse(
    req: HttpRequest<*>,
    resp: HttpResponse<*>,
    throwable: Throwable?,
  ) {
    // This should never be allowed to fail, as it could cause all API traffic to fail,
    // so be extra cautious here and surround everything with try/catch.
    try {
      val trace = req.getAttribute(TRACE_ATTR, Trace::class.java).orElseThrow()

      trace.http["status"] = resp.code().toString()

      ApmTraceUtils.addTagsToTrace(trace.user.toMap(), "user")
      ApmTraceUtils.addTagsToTrace(trace.http.toMap(), "http")
      ApmTraceUtils.addTagsToTrace(trace.refs.toMap(), "refs")

      val metricAttrs = mutableListOf<MetricAttribute>()

      // Only record a subset of attributes to the metric,
      // because each unique tag value costs money in Datadog.
      metricAttrs.add("user", trace.user, listOf("userId", "authUserId"))
      metricAttrs.add("http", trace.http, listOf("route"))
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
}

private data class Trace(
  val start: Timer.Sample = Timer.start(),
  // The code here makes it seems like these values can be non-null Stings,
  // but the nullability integration between java/kotlin isn't perfect,
  // and it's possible to pass a null String values to a non-nullable String in Kotlin.
  val user: MutableMap<String, String?> = mutableMapOf(),
  val http: MutableMap<String, String?> = mutableMapOf(),
  val refs: MutableMap<String, String?> = mutableMapOf(),
  var traceError: Throwable? = null,
)

private fun getRouteVariables(req: HttpRequest<*>): Map<String, Any> =
  BasicHttpAttributes.getRouteMatchInfo(req).getOrNull()?.variableValues ?: emptyMap()

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
