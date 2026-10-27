/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.server.generated.models.ActorCatalogWithUpdatedAt
import io.airbyte.api.server.generated.models.CheckConnectionRead
import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.DestinationReadList
import io.airbyte.api.server.generated.models.DiscoverCatalogResult
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRead
import io.airbyte.api.server.generated.models.SourceMetadata
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SourceReadList
import io.airbyte.api.server.generated.models.SourceReadWithMetadata
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.server.services.DestinationDiscoverService
import io.airbyte.config.DestinationCatalog
import io.airbyte.config.DestinationCatalogWithId
import io.airbyte.data.auth.TokenType
import io.airbyte.domain.models.ConnectionId
import io.airbyte.domain.models.DestinationCatalogId
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.util.UUID

private const val SPEC = "SourceDestinationApisSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Calls every source and destination route over HTTP with security enabled, at the exact path clients
 * call. A route moved by the controller's generated interface fails here rather than as a generic 403
 * in prod.
 */
@MicronautTest(environments = ["test"])
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class SourceDestinationApisSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun authorizationDataService(): AuthorizationDataService {
      val resourceId = UUID.fromString(id)
      return mockk {
        every { savedSourceWorkspaceAndOrganization(resourceId) } returns WorkspaceAndOrganization(resourceId, resourceId)
      }
    }

    @Singleton
    @Primary
    fun sourceHandler(): SourceHandler =
      mockk(relaxed = true) {
        every { createSourceWithOptionalSecret(any()) } returns sourceRead()
        every { getSource(any()) } returns sourceRead()
        every { getSourceWithMetadata(any(), any()) } returns sourceReadWithMetadata()
        every { getMostRecentSourceActorCatalogWithUpdatedAt(any()) } returns ActorCatalogWithUpdatedAt()
        every { listSourcesForWorkspace(any()) } returns SourceReadList(sources = listOf(sourceRead()))
        every { listSourcesForWorkspaces(any()) } returns SourceReadList(sources = listOf(sourceRead()))
        every { searchSources(any()) } returns SourceReadList(sources = listOf(sourceRead()))
        every { updateSource(any()) } returns sourceRead()
        every { updateSourceWithOptionalSecret(any(), any()) } returns sourceRead()
        every { writeDiscoverCatalogResult(any()) } returns DiscoverCatalogResult(catalogId = UUID.randomUUID())
      }

    @Singleton
    @Primary
    fun destinationHandler(): DestinationHandler =
      mockk(relaxed = true) {
        every { createDestination(any()) } returns destinationRead()
        every { getDestination(any()) } returns destinationRead()
        every { listDestinationsForWorkspace(any()) } returns DestinationReadList(destinations = listOf(destinationRead()))
        every { listDestinationsForWorkspaces(any()) } returns DestinationReadList(destinations = listOf(destinationRead()))
        every { searchDestinations(any()) } returns DestinationReadList(destinations = listOf(destinationRead()))
        every { updateDestination(any()) } returns destinationRead()
        every { partialDestinationUpdate(any()) } returns destinationRead()
      }

    // the controllers serialize these through JSON, so they must be real objects
    @Singleton
    @Primary
    fun schedulerHandler(): SchedulerHandler =
      mockk(relaxed = true) {
        every { checkSourceConnectionFromSourceId(any()) } returns checkConnectionRead()
        every { checkSourceConnectionFromSourceIdForUpdate(any()) } returns checkConnectionRead()
        every { checkDestinationConnectionFromDestinationId(any()) } returns checkConnectionRead()
        every { checkDestinationConnectionFromDestinationIdForUpdate(any()) } returns checkConnectionRead()
        every { discoverSchemaForSourceFromSourceId(any()) } returns SourceDiscoverSchemaRead(jobInfo = jobInfo())
      }
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  // DestinationApiControllerTest's factory already replaces this service with a mock for every test in the module,
  // so this test stubs that shared mock instead of declaring a second replacement
  @Inject
  lateinit var destinationDiscoverService: DestinationDiscoverService

  @BeforeEach
  fun stubDestinationDiscoverService() {
    every { destinationDiscoverService.getDestinationCatalog(any(), any<Boolean>()) } returns destinationCatalog()
    every { destinationDiscoverService.getDestinationCatalog(any<ConnectionId>()) } returns destinationCatalog()
    every { destinationDiscoverService.writeDiscoverCatalogResult(any(), any(), any(), any()) } returns DestinationCatalogId(UUID.randomUUID())
  }

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

    private const val SOURCE_EDITOR = AuthRoleConstants.WORKSPACE_SOURCE_EDITOR
    private const val DESTINATION_EDITOR = AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR
    private const val READER = AuthRoleConstants.WORKSPACE_READER

    private val config = mapOf("host" to "localhost")
    private val emptyCatalog = mapOf("streams" to emptyList<Any>())
    private val sourceUpdate = mapOf("sourceId" to id, "connectionConfiguration" to config, "name" to "source")
    private val destinationUpdate = mapOf("destinationId" to id, "connectionConfiguration" to config, "name" to "destination")
    private val listBody = mapOf("workspaceIds" to listOf(id), "pagination" to mapOf("pageSize" to 10, "rowOffset" to 0))

    private val routes =
      listOf(
        Route(
          "/api/v1/sources/apply_schema_changes",
          SOURCE_EDITOR,
          mapOf("sourceId" to id, "workspaceId" to id, "catalogId" to id, "catalog" to emptyCatalog),
        ),
        Route("/api/v1/sources/check_connection", SOURCE_EDITOR, mapOf("sourceId" to id)),
        Route("/api/v1/sources/check_connection_for_update", SOURCE_EDITOR, sourceUpdate),
        Route(
          "/api/v1/sources/create",
          SOURCE_EDITOR,
          mapOf("sourceDefinitionId" to id, "workspaceId" to id, "connectionConfiguration" to config, "name" to "source"),
        ),
        Route("/api/v1/sources/delete", SOURCE_EDITOR, mapOf("sourceId" to id)),
        Route("/api/v1/sources/discover_schema", SOURCE_EDITOR, mapOf("sourceId" to id)),
        Route("/api/v1/sources/most_recent_source_actor_catalog", READER, mapOf("sourceId" to id)),
        Route("/api/v1/sources/get", READER, mapOf("sourceId" to id)),
        Route("/api/v1/sources/get_with_metadata", READER, mapOf("sourceId" to id)),
        Route("/api/v1/sources/list", READER, mapOf("workspaceId" to id)),
        Route("/api/v1/sources/list_paginated", READER, listBody),
        Route("/api/v1/sources/partial_update", SOURCE_EDITOR, mapOf("sourceId" to id)),
        // every search field is optional, so an empty body is a valid search
        Route("/api/v1/sources/search", AuthRoleConstants.AUTHENTICATED_USER, emptyMap()),
        Route("/api/v1/sources/update", SOURCE_EDITOR, sourceUpdate),
        Route("/api/v1/sources/upgrade_version", SOURCE_EDITOR, mapOf("sourceId" to id)),
        Route("/api/v1/sources/write_discover_catalog_result", AuthRoleConstants.DATAPLANE, mapOf("sourceId" to id, "catalog" to emptyCatalog)),
        Route("/api/v1/destinations/check_connection", DESTINATION_EDITOR, mapOf("destinationId" to id)),
        Route("/api/v1/destinations/check_connection_for_update", DESTINATION_EDITOR, destinationUpdate),
        Route(
          "/api/v1/destinations/create",
          DESTINATION_EDITOR,
          mapOf("destinationDefinitionId" to id, "workspaceId" to id, "connectionConfiguration" to config, "name" to "destination"),
        ),
        Route("/api/v1/destinations/delete", DESTINATION_EDITOR, mapOf("destinationId" to id)),
        Route("/api/v1/destinations/discover_schema", READER, mapOf("destinationId" to id)),
        Route("/api/v1/destinations/get_catalog_for_connection", READER, mapOf("connectionId" to id)),
        Route("/api/v1/destinations/get", READER, mapOf("destinationId" to id)),
        Route("/api/v1/destinations/list", READER, mapOf("workspaceId" to id)),
        Route("/api/v1/destinations/list_paginated", READER, listBody),
        Route("/api/v1/destinations/partial_update", DESTINATION_EDITOR, mapOf("destinationId" to id)),
        Route("/api/v1/destinations/search", AuthRoleConstants.AUTHENTICATED_USER, emptyMap()),
        Route("/api/v1/destinations/update", DESTINATION_EDITOR, destinationUpdate),
        Route("/api/v1/destinations/upgrade_version", DESTINATION_EDITOR, mapOf("destinationId" to id)),
        Route(
          "/api/v1/destinations/write_discover_catalog_result",
          AuthRoleConstants.ADMIN,
          mapOf(
            "catalog" to mapOf("operations" to emptyList<Any>()),
            "destinationId" to id,
            "configurationHash" to "hash",
            "connectorVersion" to "0.1.0",
          ),
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

private val connectionConfiguration: JsonNode = Jsons.jsonNode(mapOf("host" to "localhost"))

private fun sourceRead() =
  SourceRead(
    sourceDefinitionId = UUID.randomUUID(),
    sourceId = UUID.randomUUID(),
    workspaceId = UUID.randomUUID(),
    connectionConfiguration = connectionConfiguration,
    name = "source",
    sourceName = "test",
    createdAt = 0,
  )

private fun sourceReadWithMetadata() =
  SourceReadWithMetadata(
    sourceDefinitionId = UUID.randomUUID(),
    sourceId = UUID.randomUUID(),
    workspaceId = UUID.randomUUID(),
    connectionConfiguration = connectionConfiguration,
    name = "source",
    sourceName = "test",
    createdAt = 0,
    metadata = SourceMetadata(workspaceName = "workspace"),
  )

private fun destinationRead() =
  DestinationRead(
    destinationDefinitionId = UUID.randomUUID(),
    destinationId = UUID.randomUUID(),
    workspaceId = UUID.randomUUID(),
    connectionConfiguration = connectionConfiguration,
    name = "destination",
    destinationName = "test",
    createdAt = 0,
  )

private fun jobInfo() =
  SynchronousJobRead(
    id = UUID.randomUUID(),
    configType = JobConfigType.CHECK_CONNECTION_SOURCE,
    createdAt = 0,
    endedAt = 0,
    succeeded = true,
  )

private fun checkConnectionRead() = CheckConnectionRead(status = CheckConnectionRead.Status.SUCCEEDED, jobInfo = jobInfo())

private fun destinationCatalog() = DestinationCatalogWithId(DestinationCatalogId(UUID.randomUUID()), DestinationCatalog(operations = emptyList()))

@Requires(property = "spec.name", value = SPEC)
@Singleton
class SourceDestinationApisAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val role = request.headers[TEST_ROLES_HEADER] ?: AuthRoleConstants.AUTHENTICATED_USER
    val roles = listOf(AuthRoleConstants.AUTHENTICATED_USER, role).distinct()
    val claims = if (role == AuthRoleConstants.DATAPLANE) mapOf(TokenType.INTERNAL_CLIENT.toClaim()) else emptyMap()
    return Flux.just(Authentication.build(role, roles, claims))
  }
}
