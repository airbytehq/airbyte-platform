/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.ConnectorRolloutHandler
import io.airbyte.commons.server.handlers.ConnectorRolloutHandlerManual
import io.airbyte.commons.server.handlers.DiagnosticToolHandler
import io.airbyte.commons.server.handlers.ScopedConfigurationHandler
import io.airbyte.commons.server.handlers.SecretPersistenceConfigHandler
import io.airbyte.commons.server.handlers.helpers.ConnectorRolloutHelper
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
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

private const val SPEC = "PlatformAdminApisSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Exercises every applications, diagnostic tool, scoped configuration, secrets persistence config,
 * organization semantic search and connector rollout route over HTTP with security enabled, at the exact
 * path clients call, so a route moved by the controller's generated interface fails here rather than as a
 * generic 403 in prod.
 */
@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class PlatformAdminApisSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun applicationService(): ApplicationService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun currentUserService(): CurrentUserService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun diagnosticToolHandler(): DiagnosticToolHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun scopedConfigurationHandler(): ScopedConfigurationHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun secretPersistenceConfigService(): SecretPersistenceConfigService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun secretPersistenceConfigHandler(): SecretPersistenceConfigHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun connectorRolloutHandler(): ConnectorRolloutHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun connectorRolloutHandlerManual(): ConnectorRolloutHandlerManual = mockk(relaxed = true)

    @Singleton
    @Primary
    fun connectorRolloutHelper(): ConnectorRolloutHelper = mockk(relaxed = true)
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
    val path: String,
    val allowedRole: String,
    val body: Map<String, Any>,
    val requiresBody: Boolean,
  ) {
    fun request(body: Map<String, Any> = this.body): MutableHttpRequest<*> = HttpRequest.POST(path, body).contentType(MediaType.APPLICATION_JSON_TYPE)

    override fun toString() = "POST $path"
  }

  companion object {
    private val id = UUID.randomUUID().toString()
    private const val V1 = "/api/v1"
    private const val ADMIN = AuthRoleConstants.ADMIN
    private const val READER = AuthRoleConstants.WORKSPACE_READER
    private const val ORG_ADMIN = AuthRoleConstants.ORGANIZATION_ADMIN
    private const val AUTHENTICATED = AuthRoleConstants.AUTHENTICATED_USER

    private val routes =
      listOf(
        Route("$V1/applications/token", AUTHENTICATED, mapOf<String, Any>("client_id" to "s", "client_secret" to "s"), requiresBody = true),
        Route("$V1/applications/create", AUTHENTICATED, mapOf<String, Any>("name" to "s"), requiresBody = true),
        Route("$V1/applications/delete", AUTHENTICATED, mapOf<String, Any>("applicationId" to id), requiresBody = true),
        Route("$V1/applications/list", AUTHENTICATED, emptyMap<String, Any>(), requiresBody = false),
        Route("$V1/diagnostic_tool/generate_report", READER, mapOf<String, Any>("organizationId" to id), requiresBody = true),
        Route(
          "$V1/scoped_configuration/create",
          ADMIN,
          mapOf<String, Any>(
            "config_key" to "s",
            "scope_id" to "s",
            "scope_type" to "s",
            "origin" to "s",
            "origin_type" to "s",
          ),
          requiresBody = true,
        ),
        Route("$V1/scoped_configuration/delete", ADMIN, mapOf<String, Any>("scopedConfigurationId" to id), requiresBody = true),
        Route("$V1/scoped_configuration/get", ADMIN, mapOf<String, Any>("scopedConfigurationId" to id), requiresBody = true),
        Route(
          "$V1/scoped_configuration/get_context",
          ADMIN,
          mapOf<String, Any>("config_key" to "s", "scope_id" to id, "scope_type" to "s"),
          requiresBody = true,
        ),
        Route("$V1/scoped_configuration/list", ADMIN, emptyMap<String, Any>(), requiresBody = false),
        Route("$V1/scoped_configuration/update", ADMIN, mapOf<String, Any>("scopedConfigurationId" to id), requiresBody = true),
        Route(
          "$V1/secrets_persistence_config/create_or_update",
          ADMIN,
          mapOf<String, Any>(
            "secretPersistenceType" to "testing",
            "configuration" to emptyMap<String, Any>(),
            "scope" to "workspace",
            "scopeId" to id,
          ),
          requiresBody = true,
        ),
        Route("$V1/secrets_persistence_config/get", ADMIN, mapOf<String, Any>("scopeType" to "workspace", "scopeId" to id), requiresBody = true),
        Route("$V1/organizations/get_semantic_search_enablement", AUTHENTICATED, mapOf<String, Any>("organizationId" to id), requiresBody = true),
        Route(
          "$V1/organizations/update_semantic_search_enablement",
          ORG_ADMIN,
          mapOf<String, Any>(
            "organizationId" to id,
            "semanticSearchEnabled" to false,
          ),
          requiresBody = true,
        ),
        Route("$V1/connector_rollout/rollout", ADMIN, mapOf<String, Any>("id" to id, "rollout_strategy" to "manual"), requiresBody = true),
        Route(
          "$V1/connector_rollout/finalize",
          ADMIN,
          mapOf<String, Any>("id" to id, "state" to "succeeded", "rollout_strategy" to "manual"),
          requiresBody = true,
        ),
        Route("$V1/connector_rollout/get_actor_sync_info", ADMIN, mapOf<String, Any>("id" to id), requiresBody = true),
        Route("$V1/connector_rollout/get", ADMIN, mapOf<String, Any>("id" to id), requiresBody = true),
        Route("$V1/connector_rollout/list", ADMIN, mapOf<String, Any>("docker_image_tag" to "s", "actor_definition_id" to id), requiresBody = true),
        Route("$V1/connector_rollout/list_all", ADMIN, emptyMap<String, Any>(), requiresBody = false),
        Route("$V1/connector_rollout/list_by_actor_definition_id", ADMIN, mapOf<String, Any>("actor_definition_id" to id), requiresBody = true),
        Route(
          "$V1/connector_rollout/manual_rollout",
          ADMIN,
          mapOf<String, Any>(
            "docker_repository" to "s",
            "docker_image_tag" to "s",
            "actor_definition_id" to id,
            "id" to id,
            "updated_by" to id,
          ),
          requiresBody = true,
        ),
        Route(
          "$V1/connector_rollout/manual_finalize",
          ADMIN,
          mapOf<String, Any>(
            "docker_repository" to "s",
            "docker_image_tag" to "s",
            "actor_definition_id" to id,
            "id" to id,
            "updated_by" to id,
            "state" to "succeeded",
          ),
          requiresBody = true,
        ),
        Route("$V1/connector_rollout/manual_pause", ADMIN, mapOf<String, Any>("state" to "initialized"), requiresBody = true),
        Route(
          "$V1/connector_rollout/manual_start",
          ADMIN,
          mapOf<String, Any>(
            "docker_repository" to "s",
            "docker_image_tag" to "s",
            "actor_definition_id" to id,
            "updated_by" to id,
            "rollout_strategy" to "manual",
          ),
          requiresBody = true,
        ),
        Route(
          "$V1/connector_rollout/start",
          ADMIN,
          mapOf<String, Any>("id" to id, "workflow_run_id" to "s", "rollout_strategy" to "manual"),
          requiresBody = true,
        ),
        Route("$V1/connector_rollout/update_state", ADMIN, mapOf<String, Any>("state" to "initialized"), requiresBody = true),
      )

    @JvmStatic
    fun routes() = routes

    // routes open to any authenticated (or anonymous) caller have no role to withhold
    @JvmStatic
    fun securedRoutes() = routes.filter { it.allowedRole != AUTHENTICATED }

    @JvmStatic
    fun routesWithRequiredFields() = routes.filter { it.requiresBody }
  }
}

@Requires(property = "spec.name", value = SPEC)
@Singleton
class PlatformAdminApisAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val roles = listOfNotNull(AuthRoleConstants.AUTHENTICATED_USER, request.headers[TEST_ROLES_HEADER]).distinct()
    return Flux.just(Authentication.build("platform-admin-apis-security-test-user", roles))
  }
}
