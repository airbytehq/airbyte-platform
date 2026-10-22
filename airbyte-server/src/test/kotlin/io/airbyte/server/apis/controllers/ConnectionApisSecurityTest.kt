/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.MatchSearchHandler
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.StateHandler
import io.airbyte.commons.server.handlers.StreamRefreshesHandler
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.services.CatalogDiffService
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.server.handlers.StreamStatusesHandler
import io.airbyte.server.handlers.WebBackendCronExpressionHandler
import io.airbyte.server.handlers.WebBackendMappersHandler
import io.airbyte.server.services.CommandService
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.MutableHttpRequest
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.util.UUID
import io.airbyte.commons.server.services.ConnectionService as ConnectionHandlerService
import io.airbyte.data.services.ConnectionService as DataConnectionService

private const val SPEC = "ConnectionApisSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Exercises every connection, web backend, operation, state, scheduler, command and catalog route over
 * HTTP with security enabled, at the exact path clients call, so a route moved by the controller's
 * generated interface fails here rather than as a generic 403 in prod.
 */
@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class ConnectionApisSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun connectionsHandler(): ConnectionsHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun operationsHandler(): OperationsHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun schedulerHandler(): SchedulerHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun streamStatusesHandler(): StreamStatusesHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun matchSearchHandler(): MatchSearchHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun streamRefreshesHandler(): StreamRefreshesHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun jobHistoryHandler(): JobHistoryHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun connectionHandlerService(): ConnectionHandlerService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun webBackendConnectionsHandler(): WebBackendConnectionsHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun webBackendCheckUpdatesHandler(): WebBackendCheckUpdatesHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun webBackendCronExpressionHandler(): WebBackendCronExpressionHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun webBackendMappersHandler(): WebBackendMappersHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun roleResolver(): RoleResolver = mockk(relaxed = true)

    @Singleton
    @Primary
    fun stateHandler(): StateHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun actorRepository(): ActorRepository = mockk(relaxed = true)

    @Singleton
    @Primary
    fun catalogConverter(): CatalogConverter = mockk(relaxed = true)

    @Singleton
    @Primary
    fun commandService(): CommandService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun workspaceService(): WorkspaceService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun catalogDiffService(): CatalogDiffService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun catalogService(): CatalogService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun dataConnectionService(): DataConnectionService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun sourceService(): SourceService = mockk(relaxed = true)
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `route is reachable with its role`(route: Route) {
    assertThat(statusOf(route.request(), route.allowedRole))
      .withFailMessage { "${route.path} with ${route.allowedRole} should be routed and authorized" }
      .isNotIn(HttpStatus.UNAUTHORIZED, HttpStatus.FORBIDDEN, HttpStatus.NOT_FOUND, HttpStatus.METHOD_NOT_ALLOWED)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("securedRoutes")
  fun `route is forbidden without its role`(route: Route) {
    assertThat(statusOf(route.request(), AuthRoleConstants.AUTHENTICATED_USER)).isEqualTo(HttpStatus.FORBIDDEN)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routesWithRequiredFields")
  fun `missing required field is a client error`(route: Route) {
    val status = statusOf(route.request(body = emptyMap()), route.allowedRole)
    assertThat(status.code)
      .withFailMessage { "${route.path} with an empty body returned $status" }
      .isBetween(400, 499)
      .isNotIn(HttpStatus.UNAUTHORIZED.code, HttpStatus.FORBIDDEN.code, HttpStatus.NOT_FOUND.code)
  }

  private fun statusOf(
    request: MutableHttpRequest<*>,
    role: String,
  ): HttpStatus =
    try {
      client.toBlocking().exchange(request.header(TEST_ROLES_HEADER, role), Any::class.java).status
    } catch (e: HttpClientResponseException) {
      e.status
    }

  data class Route(
    val method: HttpMethod,
    val path: String,
    val allowedRole: String,
    val body: Map<String, Any>,
    val requiresBody: Boolean,
  ) {
    fun request(body: Map<String, Any> = this.body): MutableHttpRequest<*> =
      when (method) {
        HttpMethod.GET -> HttpRequest.GET<Any>(path)
        else -> HttpRequest.POST(path, body).contentType(MediaType.APPLICATION_JSON_TYPE)
      }

    override fun toString() = "$method $path"
  }

  companion object {
    private val id = UUID.randomUUID().toString()
    private const val V1 = "/api/v1"
    private const val ADMIN = AuthRoleConstants.ADMIN
    private const val READER = AuthRoleConstants.WORKSPACE_READER
    private const val EDITOR = AuthRoleConstants.WORKSPACE_EDITOR
    private const val RUNNER = AuthRoleConstants.WORKSPACE_RUNNER
    private const val AUTHENTICATED = AuthRoleConstants.AUTHENTICATED_USER

    private val routes =
      listOf(
        Route(
          HttpMethod.POST,
          "$V1/connections/apply_schema_changes",
          EDITOR,
          mapOf<String, Any>(
            "catalog" to mapOf<String, Any>("streams" to emptyList<Any>()),
            "catalogId" to id,
            "connectionId" to id,
            "workspaceId" to id,
          ),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/connections/auto_disable", ADMIN, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/connections/events/backfill",
          ADMIN,
          mapOf<String, Any>(
            "connectionId" to id,
            "createdAtStart" to "2026-01-01T00:00:00Z",
            "createdAtEnd" to "2026-01-01T00:00:00Z",
          ),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/connections/clear", EDITOR, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/connections/clear/stream",
          EDITOR,
          mapOf<String, Any>("connectionId" to id, "streams" to emptyList<Any>()),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/connections/create",
          EDITOR,
          mapOf<String, Any>("sourceId" to id, "destinationId" to id, "status" to "active"),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/connections/delete", EDITOR, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/connections/get", READER, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/connections/get_context", ADMIN, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/connections/history/data",
          READER,
          mapOf<String, Any>("connectionId" to id, "numberOfJobs" to 1),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/connections/events/get",
          READER,
          mapOf<String, Any>("connectionEventId" to id, "connectionId" to id),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/connections/getForJob", READER, mapOf<String, Any>("connectionId" to id, "jobId" to 1), requiresBody = true),
        Route(HttpMethod.POST, "$V1/connections/last_job_per_stream", READER, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/connections/status", READER, mapOf<String, Any>("connectionIds" to emptyList<Any>()), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/connections/stream_history",
          READER,
          mapOf<String, Any>("connectionId" to id, "timezone" to "s"),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/connections/sync_progress", READER, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/connections/history/uptime", READER, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/connections/list_all", READER, mapOf<String, Any>("workspaceId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/connections/events/list", READER, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/connections/events/list_for_job", READER, mapOf<String, Any>("id" to 1), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/connections/events/list_minimal",
          AUTHENTICATED,
          mapOf<String, Any>(
            "workspaceId" to id,
            "eventTypes" to emptyList<Any>(),
            "createdAtStart" to "2026-01-01T00:00:00Z",
            "createdAtEnd" to "2026-01-01T00:00:00Z",
          ),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/connections/list_by_actor_definition",
          AUTHENTICATED,
          mapOf<String, Any>(
            "actorDefinitionId" to id,
            "actorType" to "source",
          ),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/connections/list", READER, mapOf<String, Any>("workspaceId" to id), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/connections/list_paginated",
          READER,
          mapOf<String, Any>("workspaceIds" to emptyList<Any>(), "userId" to id),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/connections/postprocess_discovered_catalog",
          EDITOR,
          mapOf<String, Any>("catalogId" to id, "connectionId" to id),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/connections/refresh",
          RUNNER,
          mapOf<String, Any>("connectionId" to id, "refreshMode" to "Truncate"),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/connections/reset", EDITOR, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/connections/reset/stream",
          EDITOR,
          mapOf<String, Any>("connectionId" to id, "streams" to emptyList<Any>()),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/connections/search", AUTHENTICATED, emptyMap<String, Any>(), requiresBody = false),
        Route(HttpMethod.POST, "$V1/connections/sync", RUNNER, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/connections/update", EDITOR, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/connections/update_with_reason",
          EDITOR,
          mapOf<String, Any>("connectionUpdate" to mapOf<String, Any>("connectionId" to id)),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/web_backend/state/get_type", READER, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(HttpMethod.GET, "$V1/web_backend/config", AUTHENTICATED, emptyMap<String, Any>(), requiresBody = false),
        Route(HttpMethod.POST, "$V1/web_backend/check_updates", AUTHENTICATED, emptyMap<String, Any>(), requiresBody = false),
        Route(
          HttpMethod.POST,
          "$V1/web_backend/connections/create",
          EDITOR,
          mapOf<String, Any>(
            "sourceId" to id,
            "destinationId" to id,
            "status" to "active",
          ),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/web_backend/describe_cron_expression",
          AUTHENTICATED,
          mapOf<String, Any>("cronExpression" to "s"),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/web_backend/connections/get", READER, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/web_backend/connections/status_counts", READER, mapOf<String, Any>("workspaceId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/web_backend/workspace/state", READER, mapOf<String, Any>("workspaceId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/web_backend/connections/list", READER, mapOf<String, Any>("workspaceId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/web_backend/connections/update", EDITOR, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/web_backend/mappers/validate",
          EDITOR,
          mapOf<String, Any>(
            "connectionId" to id,
            "streamDescriptor" to mapOf<String, Any>("name" to "s"),
            "mappers" to emptyList<Any>(),
          ),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/operations/check", AUTHENTICATED, mapOf<String, Any>("operatorType" to "normalization"), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/operations/create",
          EDITOR,
          mapOf<String, Any>(
            "workspaceId" to id,
            "name" to "s",
            "operatorConfiguration" to mapOf<String, Any>("operatorType" to "normalization"),
          ),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/operations/delete", EDITOR, mapOf<String, Any>("operationId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/operations/get", READER, mapOf<String, Any>("operationId" to id), requiresBody = true),
        Route(HttpMethod.POST, "$V1/operations/list", READER, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/operations/update",
          EDITOR,
          mapOf<String, Any>(
            "operationId" to id,
            "name" to "s",
            "operatorConfiguration" to mapOf<String, Any>("operatorType" to "normalization"),
          ),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/state/create_or_update",
          ADMIN,
          mapOf<String, Any>(
            "connectionId" to id,
            "connectionState" to mapOf<String, Any>("stateType" to "global", "connectionId" to id),
          ),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/state/create_or_update_safe",
          EDITOR,
          mapOf<String, Any>(
            "connectionId" to id,
            "connectionState" to mapOf<String, Any>("stateType" to "global", "connectionId" to id),
          ),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/state/get", READER, mapOf<String, Any>("connectionId" to id), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/scheduler/destinations/check_connection",
          EDITOR,
          mapOf<String, Any>(
            "destinationDefinitionId" to id,
            "connectionConfiguration" to emptyMap<String, Any>(),
            "workspaceId" to id,
          ),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/scheduler/sources/check_connection",
          EDITOR,
          mapOf<String, Any>(
            "sourceDefinitionId" to id,
            "connectionConfiguration" to emptyMap<String, Any>(),
            "workspaceId" to id,
          ),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/scheduler/sources/discover_schema",
          EDITOR,
          mapOf<String, Any>(
            "sourceDefinitionId" to id,
            "connectionConfiguration" to emptyMap<String, Any>(),
            "workspaceId" to id,
          ),
          requiresBody = true,
        ),
        Route(HttpMethod.POST, "$V1/commands/cancel", AUTHENTICATED, mapOf<String, Any>("id" to "s"), requiresBody = true),
        Route(HttpMethod.POST, "$V1/commands/output/check", AUTHENTICATED, mapOf<String, Any>("id" to "s"), requiresBody = true),
        Route(HttpMethod.POST, "$V1/commands/get", AUTHENTICATED, mapOf<String, Any>("id" to "s"), requiresBody = true),
        Route(HttpMethod.POST, "$V1/commands/status", AUTHENTICATED, mapOf<String, Any>("id" to "s"), requiresBody = true),
        Route(HttpMethod.POST, "$V1/commands/output/discover", AUTHENTICATED, mapOf<String, Any>("id" to "s"), requiresBody = true),
        Route(HttpMethod.POST, "$V1/commands/output/replicate", AUTHENTICATED, mapOf<String, Any>("id" to "s"), requiresBody = true),
        Route(HttpMethod.POST, "$V1/commands/output/spec", AUTHENTICATED, mapOf<String, Any>("id" to "s"), requiresBody = true),
        Route(HttpMethod.POST, "$V1/commands/run/check", AUTHENTICATED, mapOf<String, Any>("id" to "s"), requiresBody = true),
        Route(HttpMethod.POST, "$V1/commands/run/discover", AUTHENTICATED, mapOf<String, Any>("id" to "s", "actor_id" to id), requiresBody = true),
        Route(
          HttpMethod.POST,
          "$V1/commands/run/replicate",
          RUNNER,
          mapOf<String, Any>(
            "id" to "s",
            "connection_id" to id,
            "job_id" to "s",
            "attempt_number" to 1,
          ),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/commands/run/spec",
          AUTHENTICATED,
          mapOf<String, Any>("id" to "s", "workspace_id" to id, "docker_image_tag" to "s"),
          requiresBody = true,
        ),
        Route(
          HttpMethod.POST,
          "$V1/catalogs/diff",
          AUTHENTICATED,
          mapOf<String, Any>("current_catalog_id" to id, "new_catalog_id" to id),
          requiresBody = true,
        ),
      )

    @JvmStatic
    fun routes() = routes

    // routes open to any authenticated caller have no role to withhold
    @JvmStatic
    fun securedRoutes() = routes.filter { it.allowedRole != AUTHENTICATED }

    @JvmStatic
    fun routesWithRequiredFields() = routes.filter { it.requiresBody }
  }
}

@Requires(property = "spec.name", value = SPEC)
@Singleton
class ConnectionApisAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val roles = listOfNotNull(AuthRoleConstants.AUTHENTICATED_USER, request.headers[TEST_ROLES_HEADER]).distinct()
    return Flux.just(Authentication.build("connection-apis-security-test-user", roles))
  }
}
