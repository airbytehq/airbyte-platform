/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.filters

import io.airbyte.analytics.TrackingClient
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.ScopeType
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.BasicHttpAttributes
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Filter
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.http.filter.HttpServerFilter
import io.micronaut.http.filter.ServerFilterChain
import io.micronaut.http.filter.ServerFilterPhase
import org.reactivestreams.Publisher
import java.util.UUID
import kotlin.jvm.optionals.getOrNull

private val log = KotlinLogging.logger {}

/**
 * Emits a Segment event for every API request (Config API and Public API alike) that identifies its
 * client via the `X-Airbyte-Analytic-Source` header, so that traffic from clients such as PyAirbyte
 * and the Airbyte MCP server can be attributed without ingesting every API call.
 *
 * Requests without the header, or whose source is in [IGNORED_SOURCES], are not tracked. The webapp
 * is ignored because it sends the header on every Config API call and would dwarf the volume this
 * event is meant to capture.
 *
 * The event is enqueued asynchronously by [TrackingClient] and any failure here is swallowed: this
 * filter must never affect the response returned to the caller.
 */
@Filter("/api/**")
class AttributedApiCallTrackingFilter(
  private val trackingClient: TrackingClient,
  private val currentUserService: CurrentUserService,
  private val authenticationHeaderResolver: AuthenticationHeaderResolver,
) : HttpServerFilter {
  /**
   * Strictly after [io.airbyte.server.apis.publicapi.filters.PublicApiLoadShedFilter] so that
   * load-shed (429) requests are deterministically excluded from tracking.
   */
  override fun getOrder(): Int = ServerFilterPhase.SECURITY.after() + 1

  override fun doFilter(
    request: HttpRequest<*>,
    chain: ServerFilterChain,
  ): Publisher<MutableHttpResponse<*>> {
    if (!shouldTrack(request)) {
      return chain.proceed(request)
    }
    return Publishers.map(chain.proceed(request)) { response ->
      ServerRequestContext.with(request) { track(request, response) }
      response
    }
  }

  private fun shouldTrack(request: HttpRequest<*>): Boolean {
    val source = request.headers[AIRBYTE_ANALYTIC_SOURCE_HEADER]?.trim()?.lowercase()
    return !source.isNullOrEmpty() && source !in IGNORED_SOURCES
  }

  private fun track(
    request: HttpRequest<*>,
    response: HttpResponse<*>,
  ) {
    try {
      val props = authProperties(request)
      val user = if (request.userPrincipal.isPresent) attempt("user") { currentUserService.getCurrentUser() } else null
      val workspaceId = attempt("workspace") { resolveWorkspaceId(props) } ?: user?.defaultWorkspaceId
      val organizationId = attempt("organization") { resolveOrganizationId(props, workspaceId) }

      val (scopeId, scopeType) =
        when {
          workspaceId != null -> workspaceId to ScopeType.WORKSPACE
          organizationId != null -> organizationId to ScopeType.ORGANIZATION
          else -> return
        }

      val payload =
        buildMap<String, Any?> {
          put(ENDPOINT, BasicHttpAttributes.getUriTemplate(request).orElse(request.path))
          put(OPERATION, request.method.name)
          put(STATUS_CODE, response.status.code)
          user?.let { put(AIRBYTE_USER_ID, it.userId.toString()) }
          workspaceId?.let { put(WORKSPACE, it.toString()) }
          organizationId?.let { put(ORGANIZATION, it.toString()) }
        }
      trackingClient.track(scopeId, scopeType, AIRBYTE_ATTRIBUTED_API_CALL, payload)
    } catch (e: Exception) {
      log.warn(e) { "Failed to track attributed API call for ${request.method} ${request.path}" }
    }
  }

  private fun <T> attempt(
    what: String,
    block: () -> T?,
  ): T? =
    try {
      block()
    } catch (e: Exception) {
      log.debug(e) { "Unable to resolve $what for attributed API call tracking" }
      null
    }

  private fun resolveWorkspaceId(props: Map<String, String>): UUID? =
    props[AuthenticationId.WORKSPACE_ID.httpHeader]?.let { runCatching { UUID.fromString(it) }.getOrNull() }
      ?: authenticationHeaderResolver.resolveWorkspace(props)?.singleOrNull()

  /**
   * [AuthenticationHeaderResolver.resolveOrganization] falls back to resolving the workspace and then
   * looking up its organization, so it is only consulted when no workspace could be resolved: the
   * workspace-scoped tracking identity already carries the customer attribution.
   */
  private fun resolveOrganizationId(
    props: Map<String, String>,
    workspaceId: UUID?,
  ): UUID? =
    props[AuthenticationId.ORGANIZATION_ID.httpHeader]?.let { runCatching { UUID.fromString(it) }.getOrNull() }
      ?: if (workspaceId == null) authenticationHeaderResolver.resolveOrganization(props)?.singleOrNull() else null

  /**
   * Authentication properties come from request headers (populated from the request body by the
   * authorization filter) plus route variables (e.g. `{workspaceId}` on Public API paths).
   */
  private fun authProperties(request: HttpRequest<*>): Map<String, String> {
    val props = request.headers.asMap(String::class.java, String::class.java).toMutableMap()
    val routeVars = BasicHttpAttributes.getRouteMatchInfo(request).getOrNull()?.variableValues ?: emptyMap()
    for ((name, value) in routeVars) {
      AuthenticationId.entries.firstOrNull { it.fieldName == name }?.also { props[it.httpHeader] = value.toString() }
    }
    return props
  }

  companion object {
    const val AIRBYTE_ATTRIBUTED_API_CALL = "Airbyte_Attributed_API_Call"
    const val AIRBYTE_ANALYTIC_SOURCE_HEADER = "X-Airbyte-Analytic-Source"
    val IGNORED_SOURCES = setOf("unknown", "webapp")

    const val ENDPOINT = "endpoint"
    const val OPERATION = "operation"
    const val STATUS_CODE = "status_code"
    const val AIRBYTE_USER_ID = "airbyte_user_id"
    const val WORKSPACE = "workspace"
    const val ORGANIZATION = "organization"
  }
}
