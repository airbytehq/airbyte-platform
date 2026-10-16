/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.ActorDefinitionCreateResponse
import io.airbyte.api.server.generated.models.ActorDefinitionResultResponse
import io.airbyte.api.server.generated.models.ActorDefinitionUpdateResponse
import io.airbyte.api.server.generated.models.ActorDefinitionVersionRead
import io.airbyte.api.server.generated.models.DestinationDefinitionRead
import io.airbyte.api.server.generated.models.DestinationDefinitionReadList
import io.airbyte.api.server.generated.models.DestinationDefinitionSpecificationRead
import io.airbyte.api.server.generated.models.DestinationDefinitionUpdate
import io.airbyte.api.server.generated.models.EnterpriseConnectorStubsReadList
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.PrivateDestinationDefinitionRead
import io.airbyte.api.server.generated.models.PrivateDestinationDefinitionReadList
import io.airbyte.api.server.generated.models.PrivateSourceDefinitionRead
import io.airbyte.api.server.generated.models.PrivateSourceDefinitionReadList
import io.airbyte.api.server.generated.models.ResolveActorDefinitionVersionResponse
import io.airbyte.api.server.generated.models.SourceDefinitionRead
import io.airbyte.api.server.generated.models.SourceDefinitionReadList
import io.airbyte.api.server.generated.models.SourceDefinitionSpecificationRead
import io.airbyte.api.server.generated.models.SourceDefinitionUpdate
import io.airbyte.api.server.generated.models.SupportState
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.ActorDefinitionVersionHandler
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.handlers.EnterpriseConnectorStubsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.server.handlers.ActorDefinitionsHandler
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
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.net.URI
import java.util.UUID

private const val SPEC = "DefinitionApisSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Calls every definition, specification and actor definition version route over HTTP with security
 * enabled, at the exact path clients call. A route moved by the controller's generated interface
 * fails here rather than as a generic 403 in prod.
 */
@MicronautTest(environments = ["test"])
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class DefinitionApisSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun sourceDefinitionsHandler(): SourceDefinitionsHandler =
      mockk(relaxed = true) {
        every { createCustomSourceDefinition(any()) } returns sourceDefinitionRead()
        every { getSourceDefinition(any(), any()) } returns sourceDefinitionRead()
        every { getSourceDefinitionForScope(any()) } returns sourceDefinitionRead()
        every { getSourceDefinitionForWorkspace(any()) } returns sourceDefinitionRead()
        every { grantSourceDefinitionToWorkspaceOrOrganization(any()) } returns
          PrivateSourceDefinitionRead(sourceDefinition = sourceDefinitionRead(), granted = true)
        every { listLatestSourceDefinitions() } returns SourceDefinitionReadList(sourceDefinitions = listOf(sourceDefinitionRead()))
        every { listPrivateSourceDefinitions(any()) } returns PrivateSourceDefinitionReadList(sourceDefinitions = emptyList())
        every { listSourceDefinitions() } returns SourceDefinitionReadList(sourceDefinitions = listOf(sourceDefinitionRead()))
        every { listSourceDefinitionsForWorkspace(any()) } returns SourceDefinitionReadList(sourceDefinitions = listOf(sourceDefinitionRead()))
        every { updateSourceDefinition(any<SourceDefinitionUpdate>()) } returns sourceDefinitionRead()
      }

    @Singleton
    @Primary
    fun destinationDefinitionsHandler(): DestinationDefinitionsHandler =
      mockk(relaxed = true) {
        every { createCustomDestinationDefinition(any()) } returns destinationDefinitionRead()
        every { getDestinationDefinition(any(), any()) } returns destinationDefinitionRead()
        every { getDestinationDefinitionForScope(any()) } returns destinationDefinitionRead()
        every { getDestinationDefinitionForWorkspace(any()) } returns destinationDefinitionRead()
        every { grantDestinationDefinitionToWorkspaceOrOrganization(any()) } returns
          PrivateDestinationDefinitionRead(destinationDefinition = destinationDefinitionRead(), granted = true)
        every { listDestinationDefinitions() } returns DestinationDefinitionReadList(destinationDefinitions = listOf(destinationDefinitionRead()))
        every { listDestinationDefinitionsForWorkspace(any()) } returns
          DestinationDefinitionReadList(destinationDefinitions = listOf(destinationDefinitionRead()))
        every { listLatestDestinationDefinitions() } returns
          DestinationDefinitionReadList(destinationDefinitions = listOf(destinationDefinitionRead()))
        every { listPrivateDestinationDefinitions(any()) } returns PrivateDestinationDefinitionReadList(destinationDefinitions = emptyList())
        every { updateDestinationDefinition(any<DestinationDefinitionUpdate>()) } returns
          destinationDefinitionRead()
      }

    @Singleton
    @Primary
    fun enterpriseConnectorStubsHandler(): EnterpriseConnectorStubsHandler =
      mockk {
        every { listEnterpriseSourceStubs() } returns EnterpriseConnectorStubsReadList(enterpriseConnectorStubs = emptyList())
        every { listEnterpriseSourceStubsForWorkspace(any()) } returns EnterpriseConnectorStubsReadList(enterpriseConnectorStubs = emptyList())
        every { listEnterpriseDestinationStubsForWorkspace(any()) } returns EnterpriseConnectorStubsReadList(enterpriseConnectorStubs = emptyList())
      }

    @Singleton
    @Primary
    fun actorDefinitionsHandler(): ActorDefinitionsHandler =
      mockk {
        every { createActorDefinition(any()) } returns ActorDefinitionCreateResponse(requestId = "request")
        every { updateActorDefinition(any()) } returns ActorDefinitionUpdateResponse(requestId = "request")
        every { finishActorDefinitionUpdate(any(), any(), any(), any(), any()) } returns
          ActorDefinitionsHandler.ActorDefinitionFinishResult(UUID.randomUUID(), null)
        every { getActorDefinitionResult(any()) } returns ActorDefinitionResultResponse(status = ActorDefinitionResultResponse.Status.RUNNING)
      }

    @Singleton
    @Primary
    fun actorDefinitionVersionHandler(): ActorDefinitionVersionHandler =
      mockk {
        every { getActorDefinitionVersionForSourceId(any()) } returns actorDefinitionVersionRead()
        every { getActorDefinitionVersionForDestinationId(any()) } returns actorDefinitionVersionRead()
        every { getDefaultVersion(any()) } returns actorDefinitionVersionRead()
        every { resolveActorDefinitionVersionByTag(any()) } returns
          ResolveActorDefinitionVersionResponse(
            versionId = UUID.randomUUID(),
            dockerRepository = "airbyte/source-test",
            dockerImageTag = "0.1.0",
            supportRefreshes = false,
            supportFileTransfer = false,
            supportDataActivation = false,
          )
      }

    @Singleton
    @Primary
    fun connectorDefinitionSpecificationHandler(): ConnectorDefinitionSpecificationHandler =
      mockk {
        every { getSourceDefinitionSpecification(any()) } returns sourceDefinitionSpecificationRead()
        every { getSpecificationForSourceId(any()) } returns sourceDefinitionSpecificationRead()
        every { getDestinationSpecification(any()) } returns destinationDefinitionSpecificationRead()
        every { getSpecificationForDestinationId(any()) } returns destinationDefinitionSpecificationRead()
      }
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `route succeeds with its role`(route: Route) {
    val status = statusOf(route.request(), route.allowedRole)
    assertThat(status.code)
      .withFailMessage { "${route.path} with ${route.allowedRole} returned $status" }
      .isBetween(200, 299)
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
  ) {
    fun request(body: Map<String, Any> = this.body): MutableHttpRequest<*> = HttpRequest.POST(path, body).contentType(MediaType.APPLICATION_JSON_TYPE)

    override fun toString() = "POST $path"
  }

  companion object {
    private val id = UUID.randomUUID().toString()

    // UploadCustomConnector intent
    private const val INTENT_ROLE = AuthRoleConstants.WORKSPACE_EDITOR

    private val definitionCreate =
      mapOf("name" to "test", "dockerRepository" to "airbyte/test", "dockerImageTag" to "0.1.0", "documentationUrl" to "https://docs.airbyte.com")
    private val scope = mapOf("actorDefinitionId" to id, "scopeId" to id, "scopeType" to "workspace")

    private val routes =
      listOf(
        Route("/api/v1/source_definitions/create_custom", INTENT_ROLE, mapOf("workspaceId" to id, "sourceDefinition" to definitionCreate)),
        Route("/api/v1/source_definitions/delete", AuthRoleConstants.AUTHENTICATED_USER, mapOf("sourceDefinitionId" to id)),
        Route("/api/v1/source_definitions/get", AuthRoleConstants.AUTHENTICATED_USER, mapOf("sourceDefinitionId" to id)),
        Route("/api/v1/source_definitions/get_for_scope", AuthRoleConstants.WORKSPACE_READER, scope),
        Route(
          "/api/v1/source_definitions/get_for_workspace",
          AuthRoleConstants.WORKSPACE_READER,
          mapOf(
            "sourceDefinitionId" to id,
            "workspaceId" to id,
          ),
        ),
        Route("/api/v1/source_definitions/grant_definition", AuthRoleConstants.ADMIN, scope),
        Route("/api/v1/source_definitions/list_enterprise_source_stubs", AuthRoleConstants.AUTHENTICATED_USER, emptyMap()),
        Route("/api/v1/source_definitions/list_enterprise_stubs_for_workspace", AuthRoleConstants.WORKSPACE_READER, mapOf("workspaceId" to id)),
        Route("/api/v1/source_definitions/list_latest", AuthRoleConstants.AUTHENTICATED_USER, emptyMap()),
        Route("/api/v1/source_definitions/list_private", AuthRoleConstants.ADMIN, mapOf("workspaceId" to id)),
        Route("/api/v1/source_definitions/list", AuthRoleConstants.AUTHENTICATED_USER, emptyMap()),
        Route("/api/v1/source_definitions/list_for_workspace", AuthRoleConstants.WORKSPACE_READER, mapOf("workspaceId" to id)),
        Route("/api/v1/source_definitions/revoke_definition", AuthRoleConstants.ADMIN, scope),
        Route(
          "/api/v1/source_definitions/update",
          AuthRoleConstants.AUTHENTICATED_USER,
          mapOf("sourceDefinitionId" to id, "dockerImageTag" to "0.2.0", "workspaceId" to id),
        ),
        Route("/api/v1/destination_definitions/create_custom", INTENT_ROLE, mapOf("workspaceId" to id, "destinationDefinition" to definitionCreate)),
        Route("/api/v1/destination_definitions/delete", AuthRoleConstants.AUTHENTICATED_USER, mapOf("destinationDefinitionId" to id)),
        Route("/api/v1/destination_definitions/get", AuthRoleConstants.AUTHENTICATED_USER, mapOf("destinationDefinitionId" to id)),
        Route("/api/v1/destination_definitions/get_for_scope", AuthRoleConstants.WORKSPACE_READER, scope),
        Route(
          "/api/v1/destination_definitions/get_for_workspace",
          AuthRoleConstants.WORKSPACE_READER,
          mapOf("destinationDefinitionId" to id, "workspaceId" to id),
        ),
        Route("/api/v1/destination_definitions/grant_definition", AuthRoleConstants.ADMIN, scope),
        Route("/api/v1/destination_definitions/list", AuthRoleConstants.AUTHENTICATED_USER, emptyMap()),
        Route("/api/v1/destination_definitions/list_for_workspace", AuthRoleConstants.WORKSPACE_READER, mapOf("workspaceId" to id)),
        Route("/api/v1/destination_definitions/list_enterprise_stubs_for_workspace", AuthRoleConstants.WORKSPACE_READER, mapOf("workspaceId" to id)),
        Route("/api/v1/destination_definitions/list_latest", AuthRoleConstants.AUTHENTICATED_USER, emptyMap()),
        Route("/api/v1/destination_definitions/list_private", AuthRoleConstants.ADMIN, mapOf("workspaceId" to id)),
        Route("/api/v1/destination_definitions/revoke_definition", AuthRoleConstants.ADMIN, scope),
        Route(
          "/api/v1/destination_definitions/update",
          AuthRoleConstants.AUTHENTICATED_USER,
          mapOf("destinationDefinitionId" to id, "dockerImageTag" to "0.2.0", "workspaceId" to id),
        ),
        Route(
          "/api/v1/actor_definitions/create",
          INTENT_ROLE,
          mapOf("actorType" to "source", "actorDefinition" to definitionCreate, "workspaceId" to id),
        ),
        Route(
          "/api/v1/actor_definitions/update",
          AuthRoleConstants.AUTHENTICATED_USER,
          mapOf("actorDefinitionId" to id, "actorType" to "source", "dockerImageTag" to "0.2.0", "workspaceId" to id),
        ),
        Route(
          "/api/v1/actor_definitions/finish",
          INTENT_ROLE,
          mapOf(
            "actorUpdateRequest" to mapOf("actorType" to "source", "imageName" to "airbyte/test", "imageTag" to "0.1.0"),
            "metadata" to emptyMap<String, Any>(),
            "workspaceId" to id,
          ),
        ),
        Route("/api/v1/actor_definitions/result", AuthRoleConstants.AUTHENTICATED_USER, mapOf("requestId" to "request")),
        Route("/api/v1/actor_definition_versions/get_for_source", AuthRoleConstants.AUTHENTICATED_USER, mapOf("sourceId" to id)),
        Route("/api/v1/actor_definition_versions/get_for_destination", AuthRoleConstants.AUTHENTICATED_USER, mapOf("destinationId" to id)),
        Route("/api/v1/actor_definition_versions/get_default", AuthRoleConstants.ADMIN, mapOf("actorDefinitionId" to id)),
        Route(
          "/api/v1/actor_definition_versions/resolve",
          AuthRoleConstants.ADMIN,
          mapOf("actorDefinitionId" to id, "actorType" to "source", "dockerImageTag" to "0.1.0"),
        ),
        Route(
          "/api/v1/source_definition_specifications/get",
          AuthRoleConstants.AUTHENTICATED_USER,
          mapOf("sourceDefinitionId" to id, "workspaceId" to id),
        ),
        Route("/api/v1/source_definition_specifications/get_for_source", AuthRoleConstants.AUTHENTICATED_USER, mapOf("sourceId" to id)),
        Route(
          "/api/v1/destination_definition_specifications/get",
          AuthRoleConstants.AUTHENTICATED_USER,
          mapOf("destinationDefinitionId" to id, "workspaceId" to id),
        ),
        Route(
          "/api/v1/destination_definition_specifications/get_for_destination",
          AuthRoleConstants.AUTHENTICATED_USER,
          mapOf("destinationId" to id),
        ),
      )

    @JvmStatic
    fun routes() = routes

    @JvmStatic
    fun securedRoutes() = routes.filter { it.allowedRole != AuthRoleConstants.AUTHENTICATED_USER }

    @JvmStatic
    fun routesWithRequiredFields() = routes.filter { it.body.isNotEmpty() }
  }
}

private fun sourceDefinitionRead() =
  SourceDefinitionRead(
    sourceDefinitionId = UUID.randomUUID(),
    name = "test",
    dockerRepository = "airbyte/source-test",
    dockerImageTag = "0.1.0",
  )

private fun destinationDefinitionRead() =
  DestinationDefinitionRead(
    destinationDefinitionId = UUID.randomUUID(),
    name = "test",
    dockerRepository = "airbyte/destination-test",
    dockerImageTag = "0.1.0",
    documentationUrl = URI.create("https://docs.airbyte.com"),
  )

private fun actorDefinitionVersionRead() =
  ActorDefinitionVersionRead(
    dockerRepository = "airbyte/source-test",
    dockerImageTag = "0.1.0",
    supportsRefreshes = false,
    isVersionOverrideApplied = false,
    supportState = SupportState.SUPPORTED,
    supportsFileTransfer = false,
    supportsDataActivation = false,
  )

private fun specJobInfo() =
  SynchronousJobRead(
    id = UUID.randomUUID(),
    configType = JobConfigType.GET_SPEC,
    createdAt = 0,
    endedAt = 0,
    succeeded = true,
  )

private fun sourceDefinitionSpecificationRead() = SourceDefinitionSpecificationRead(sourceDefinitionId = UUID.randomUUID(), jobInfo = specJobInfo())

private fun destinationDefinitionSpecificationRead() =
  DestinationDefinitionSpecificationRead(destinationDefinitionId = UUID.randomUUID(), jobInfo = specJobInfo())

@Requires(property = "spec.name", value = SPEC)
@Singleton
class DefinitionApisAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val roles = listOfNotNull(AuthRoleConstants.AUTHENTICATED_USER, request.headers[TEST_ROLES_HEADER]).distinct()
    return Flux.just(Authentication.build("definition-apis-security-test-user", roles))
  }
}
