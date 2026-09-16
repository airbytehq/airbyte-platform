/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.filters

import io.airbyte.analytics.TrackingClient
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.ScopeType
import io.airbyte.server.apis.filters.AttributedApiCallTrackingFilter.Companion.AIRBYTE_ANALYTIC_SOURCE_HEADER
import io.airbyte.server.apis.filters.AttributedApiCallTrackingFilter.Companion.AIRBYTE_ATTRIBUTED_API_CALL
import io.micronaut.http.HttpAttributes
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.filter.ServerFilterChain
import io.micronaut.http.uri.UriMatchInfo
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.security.Principal
import java.util.UUID

class AttributedApiCallTrackingFilterTest {
  private lateinit var trackingClient: TrackingClient
  private lateinit var currentUserService: CurrentUserService
  private lateinit var authenticationHeaderResolver: AuthenticationHeaderResolver
  private lateinit var filter: AttributedApiCallTrackingFilter

  private val userId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val defaultWorkspaceId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    trackingClient = mockk(relaxed = true)
    currentUserService = mockk()
    authenticationHeaderResolver = mockk(relaxed = true)
    filter = AttributedApiCallTrackingFilter(trackingClient, currentUserService, authenticationHeaderResolver)

    every { currentUserService.getCurrentUser() } returns
      AuthenticatedUser().withUserId(userId).withDefaultWorkspaceId(defaultWorkspaceId)
  }

  @Test
  fun `tracks attributed call scoped to the workspace when a source header is present`() {
    val request =
      HttpRequest
        .POST("/api/v1/web_backend/connections/get", "{}")
        .header(AIRBYTE_ANALYTIC_SOURCE_HEADER, "pyairbyte-mcp-hosted")
        .header(WORKSPACE_ID_HEADER, workspaceId.toString())
        .header(ORGANIZATION_ID_HEADER, organizationId.toString())
    authenticate(request)

    val response = run(request)

    assertEquals(200, response.status.code)
    verify(exactly = 1) {
      trackingClient.track(
        workspaceId,
        ScopeType.WORKSPACE,
        AIRBYTE_ATTRIBUTED_API_CALL,
        mapOf(
          "endpoint" to "/api/v1/web_backend/connections/get",
          "operation" to "POST",
          "status_code" to 200,
          "airbyte_user_id" to userId.toString(),
          "workspace" to workspaceId.toString(),
          "organization" to organizationId.toString(),
        ),
      )
    }
    verify(exactly = 0) { authenticationHeaderResolver.resolveOrganization(any()) }
  }

  @Test
  fun `resolves the workspace from Public API route variables and reports the URI template`() {
    val request =
      HttpRequest
        .GET<Any>("/api/public/v1/workspaces/$workspaceId")
        .header(AIRBYTE_ANALYTIC_SOURCE_HEADER, "pyairbyte")
    val routeMatch = mockk<UriMatchInfo>()
    every { routeMatch.variableValues } returns mapOf("workspaceId" to workspaceId.toString())
    request.setAttribute(HttpAttributes.ROUTE_MATCH, routeMatch)
    request.setAttribute(HttpAttributes.URI_TEMPLATE, "/api/public/v1/workspaces/{workspaceId}")

    run(request)

    verify(exactly = 1) {
      trackingClient.track(
        workspaceId,
        ScopeType.WORKSPACE,
        AIRBYTE_ATTRIBUTED_API_CALL,
        mapOf(
          "endpoint" to "/api/public/v1/workspaces/{workspaceId}",
          "operation" to "GET",
          "status_code" to 200,
          "workspace" to workspaceId.toString(),
        ),
      )
    }
  }

  @Test
  fun `falls back to the organization scope when no workspace is resolvable`() {
    val request =
      HttpRequest
        .GET<Any>("/api/public/v1/organizations")
        .header(AIRBYTE_ANALYTIC_SOURCE_HEADER, "pyairbyte")
        .header(ORGANIZATION_ID_HEADER, organizationId.toString())

    run(request)

    verify(exactly = 1) { trackingClient.track(organizationId, ScopeType.ORGANIZATION, AIRBYTE_ATTRIBUTED_API_CALL, any()) }
  }

  @Test
  fun `falls back to the authenticated user's default workspace when the request carries no scope`() {
    val request =
      HttpRequest
        .GET<Any>("/api/public/v1/jobs")
        .header(AIRBYTE_ANALYTIC_SOURCE_HEADER, "pyairbyte")
    authenticate(request)

    run(request)

    verify(exactly = 1) {
      trackingClient.track(
        defaultWorkspaceId,
        ScopeType.WORKSPACE,
        AIRBYTE_ATTRIBUTED_API_CALL,
        match { it["airbyte_user_id"] == userId.toString() && it["workspace"] == defaultWorkspaceId.toString() },
      )
    }
  }

  @Test
  fun `still tracks against the organization when workspace and user resolution fail`() {
    every { currentUserService.getCurrentUser() } throws IllegalStateException("no user")
    every { authenticationHeaderResolver.resolveWorkspace(any()) } throws IllegalStateException("db down")
    val request =
      HttpRequest
        .GET<Any>("/api/public/v1/jobs")
        .header(AIRBYTE_ANALYTIC_SOURCE_HEADER, "pyairbyte")
        .header(ORGANIZATION_ID_HEADER, organizationId.toString())
    authenticate(request)

    val response = run(request)

    assertEquals(200, response.status.code)
    verify(exactly = 1) {
      trackingClient.track(
        organizationId,
        ScopeType.ORGANIZATION,
        AIRBYTE_ATTRIBUTED_API_CALL,
        mapOf(
          "endpoint" to "/api/public/v1/jobs",
          "operation" to "GET",
          "status_code" to 200,
          "organization" to organizationId.toString(),
        ),
      )
    }
  }

  @Test
  fun `does not track when neither user, workspace nor organization can be resolved`() {
    val request =
      HttpRequest
        .POST("/api/public/v1/applications/token", "{}")
        .header(AIRBYTE_ANALYTIC_SOURCE_HEADER, "pyairbyte-mcp-local")

    run(request)

    verify(exactly = 0) { trackingClient.track(any(), any(), any(), any()) }
    verify(exactly = 0) { currentUserService.getCurrentUser() }
  }

  @ParameterizedTest
  @ValueSource(strings = ["unknown", "webapp", "WebApp", "UNKNOWN", ""])
  fun `does not track ignored or blank sources`(source: String) {
    val request =
      HttpRequest
        .POST("/api/v1/workspaces/get", "{}")
        .header(AIRBYTE_ANALYTIC_SOURCE_HEADER, source)
        .header(WORKSPACE_ID_HEADER, workspaceId.toString())

    run(request)

    verify(exactly = 0) { trackingClient.track(any(), any(), any(), any()) }
  }

  @Test
  fun `does not track when the header is absent`() {
    val request =
      HttpRequest
        .POST("/api/v1/workspaces/get", "{}")
        .header(WORKSPACE_ID_HEADER, workspaceId.toString())

    run(request)

    verify(exactly = 0) { trackingClient.track(any(), any(), any(), any()) }
  }

  @Test
  fun `tracking failures never affect the response`() {
    every { trackingClient.track(any(), any(), any(), any()) } throws IllegalStateException("segment down")
    val request =
      HttpRequest
        .POST("/api/v1/workspaces/get", "{}")
        .header(AIRBYTE_ANALYTIC_SOURCE_HEADER, "pyairbyte")
        .header(WORKSPACE_ID_HEADER, workspaceId.toString())

    val response = run(request)

    assertEquals(200, response.status.code)
  }

  private fun authenticate(request: HttpRequest<*>) {
    request.setAttribute(HttpAttributes.PRINCIPAL, Principal { "auth-user" })
  }

  private fun run(request: HttpRequest<*>): MutableHttpResponse<*> {
    val chain = mockk<ServerFilterChain>()
    every { chain.proceed(request) } returns Mono.just(HttpResponse.ok<Any>())
    return Flux.from(filter.doFilter(request, chain)).blockFirst()!!
  }
}
