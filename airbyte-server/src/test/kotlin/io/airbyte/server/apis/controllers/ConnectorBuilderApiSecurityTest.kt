/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler
import io.airbyte.commons.server.handlers.ConnectorContributionHandler
import io.airbyte.commons.server.handlers.DeclarativeSourceDefinitionsHandler
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

private const val SPEC = "ConnectorBuilderApiSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Exercises every connector builder project and declarative source definition route over HTTP with
 * security enabled, at the exact path clients call, so a route moved by the controller's generated
 * interface fails here rather than as a generic 403 in prod.
 */
@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class ConnectorBuilderApiSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun connectorBuilderProjectsHandler(): ConnectorBuilderProjectsHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun connectorContributionHandler(): ConnectorContributionHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun declarativeSourceDefinitionsHandler(): DeclarativeSourceDefinitionsHandler = mockk(relaxed = true)
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
  @MethodSource("routes")
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
  ) {
    fun request(body: Map<String, Any> = this.body): MutableHttpRequest<*> = HttpRequest.POST(path, body).contentType(MediaType.APPLICATION_JSON_TYPE)

    override fun toString() = "POST $path"
  }

  companion object {
    private val id = UUID.randomUUID().toString()
    private const val PROJECTS = "/api/v1/connector_builder_projects"
    private const val DEFINITIONS = "/api/v1/declarative_source_definitions"
    private const val READER = AuthRoleConstants.WORKSPACE_READER
    private const val EDITOR = AuthRoleConstants.WORKSPACE_EDITOR
    private const val AUTHENTICATED = AuthRoleConstants.AUTHENTICATED_USER

    private val projectIds = mapOf("workspaceId" to id, "builderProjectId" to id)
    private val manifest = mapOf("version" to "1.0.0")
    private val declarativeManifest = mapOf("description" to "d", "manifest" to manifest, "spec" to manifest, "version" to 1)

    private val routes =
      listOf(
        Route("$PROJECTS/capabilities", READER, mapOf("workspaceId" to id)),
        Route("$PROJECTS/complete_oauth", READER, projectIds),
        Route("$PROJECTS/contribute/check", AUTHENTICATED, mapOf("connector_image_name" to "source-x")),
        Route(
          "$PROJECTS/contribute/generate",
          AUTHENTICATED,
          mapOf(
            "name" to "X",
            "connector_image_name" to "source-x",
            "connector_description" to "d",
            "github_token" to "t",
            "manifest_yaml" to "version: 1.0.0",
            "base_image" to "b",
            "contribution_description" to "d",
          ),
        ),
        Route("$PROJECTS/create", EDITOR, mapOf("workspaceId" to id, "builderProject" to mapOf("name" to "p"))),
        Route("$PROJECTS/delete", EDITOR, projectIds),
        Route("$PROJECTS/fork", EDITOR, mapOf("workspaceId" to id, "baseActorDefinitionId" to id)),
        Route("$PROJECTS/full_resolve", READER, mapOf("manifest" to manifest)),
        Route("$PROJECTS/get_base_image", AUTHENTICATED, mapOf("manifest" to manifest)),
        Route("$PROJECTS/get_for_definition_id", READER, mapOf("actorDefinitionId" to id, "workspaceId" to id)),
        Route("$PROJECTS/get_oauth_consent_url", READER, projectIds + ("redirectUrl" to "https://example.com")),
        Route("$PROJECTS/get_with_manifest", READER, projectIds),
        Route("$PROJECTS/list", READER, mapOf("workspaceId" to id)),
        Route("$PROJECTS/publish", EDITOR, projectIds + mapOf("name" to "p", "initialDeclarativeManifest" to declarativeManifest)),
        Route("$PROJECTS/read_stream", READER, projectIds + mapOf("manifest" to manifest, "streamName" to "s")),
        Route("$PROJECTS/resolve", READER, mapOf("manifest" to manifest)),
        Route("$PROJECTS/update", EDITOR, projectIds + ("builderProject" to mapOf("name" to "p"))),
        Route("$PROJECTS/update_testing_values", EDITOR, projectIds + mapOf("spec" to manifest, "testingValues" to manifest)),
        Route(
          "$DEFINITIONS/create_manifest",
          EDITOR,
          mapOf("workspaceId" to id, "sourceDefinitionId" to id, "setAsActiveManifest" to true, "declarativeManifest" to declarativeManifest),
        ),
        Route("$DEFINITIONS/list_manifests", EDITOR, mapOf("workspaceId" to id, "sourceDefinitionId" to id)),
        Route("$DEFINITIONS/update_active_manifest", EDITOR, mapOf("workspaceId" to id, "sourceDefinitionId" to id, "version" to 1)),
      )

    @JvmStatic
    fun routes() = routes

    @JvmStatic
    fun securedRoutes() = routes.filter { it.allowedRole != AUTHENTICATED }
  }
}

@Requires(property = "spec.name", value = SPEC)
@Singleton
class ConnectorBuilderApiAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val roles = listOfNotNull(AuthRoleConstants.AUTHENTICATED_USER, request.headers[TEST_ROLES_HEADER]).distinct()
    return Flux.just(Authentication.build("connector-builder-security-test-user", roles))
  }
}
