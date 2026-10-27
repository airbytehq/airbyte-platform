/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.throwable.generated.ActorNotReadyProblem
import io.airbyte.api.server.generated.models.ActorCatalogWithUpdatedAt
import io.airbyte.api.server.generated.models.ActorListCursorPaginatedRequestBody
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.CheckConnectionRead
import io.airbyte.api.server.generated.models.DiscoverCatalogResult
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.SourceCreate
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRead
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRequestBody
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.SourceMetadata
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SourceReadList
import io.airbyte.api.server.generated.models.SourceReadWithMetadata
import io.airbyte.api.server.generated.models.SourceSearch
import io.airbyte.api.server.generated.models.SourceUpdate
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
internal class SourceApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var schedulerHandler: SchedulerHandler
  lateinit var sourceHandler: SourceHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    schedulerHandler = mockk()
    context.registerSingleton(SchedulerHandler::class.java, schedulerHandler)
    sourceHandler = mockk()
    context.registerSingleton(SourceHandler::class.java, sourceHandler)
    val owner = WorkspaceAndOrganization(WORKSPACE_ID, UUID(0, 503))
    val authorization = mockk<AuthorizationDataService>()
    every { authorization.savedSourceWorkspaceAndOrganization(any()) } returns owner
    every { authorization.workspaceAndOrganization(WORKSPACE_ID) } returns owner
    every { authorization.requireActorDefinitionType(any(), any()) } returns Unit
    every { authorization.canUseActorDefinition(any(), any()) } returns true
    every { authorization.actorConnectionIds(any(), any(), any()) } returns emptyList()
    context.registerSingleton(AuthorizationDataService::class.java, authorization)
  }

  @Test
  fun testCheckConnectionToSource() {
    every { schedulerHandler.checkSourceConnectionFromSourceId(any()) } returns checkConnectionRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/check_connection"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, sourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, sourceIdRequestBody())))
  }

  @Test
  fun testCheckConnectionToSourceForUpdate() {
    every { schedulerHandler.checkSourceConnectionFromSourceIdForUpdate(any()) } returns checkConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/sources/check_connection_for_update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, sourceUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, sourceUpdate())))
  }

  @Test
  fun testCreateSource() {
    every { sourceHandler.createSourceWithOptionalSecret(any()) } returns sourceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, sourceCreate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, sourceCreate())))
  }

  @Test
  fun testDeleteSource() {
    every { sourceHandler.deleteSource(any<SourceIdRequestBody>()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, sourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, sourceIdRequestBody())))
  }

  @Test
  fun testDiscoverSchemaForSource() {
    every { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) } returns sourceDiscoverSchemaRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/sources/discover_schema"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceDiscoverSchemaRequestBody(sourceId = ACTOR_ID))))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDiscoverSchemaRequestBody(sourceId = ACTOR_ID))))
  }

  @Test
  fun `draft source discovery returns the stable actor-not-ready problem body`() {
    every { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) } throws ActorNotReadyProblem()

    val error =
      assertThrows<HttpClientResponseException> {
        client
          .toBlocking()
          .exchange(
            HttpRequest.POST(
              "/api/v1/sources/discover_schema",
              SourceDiscoverSchemaRequestBody(sourceId = ACTOR_ID),
            ),
            JsonNode::class.java,
          )
      }

    assertEquals(HttpStatus.CONFLICT, error.status)
    val body = error.response.getBody(JsonNode::class.java).orElseThrow()
    assertEquals(409, body.path("status").asInt())
    assertEquals("actor-not-ready", body.path("title").asText())
    assertEquals("https://reference.airbyte.com/reference/errors#409-actor-not-ready", body.path("type").asText())
    assertEquals(
      "The source or destination is still a draft and cannot be used yet. Finish its setup with a successful connection check, then retry.",
      body.path("detail").asText(),
    )
  }

  @Test
  fun testGetSource() {
    every { sourceHandler.getSource(any()) } returns sourceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, sourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, sourceIdRequestBody())))
  }

  @Test
  fun testGetSourceWithMetadata() {
    every { sourceHandler.getSourceWithMetadata(any(), any()) } returns sourceReadWithMetadata() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/get_with_metadata"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, sourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, sourceIdRequestBody())))
  }

  @Test
  fun testGetMostRecentSourceActorCatalog() {
    every { sourceHandler.getMostRecentSourceActorCatalogWithUpdatedAt(any()) } returns ActorCatalogWithUpdatedAt()

    val path = "/api/v1/sources/most_recent_source_actor_catalog"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, sourceIdRequestBody())))
  }

  @Test
  fun testListSourcesForWorkspace() {
    every { sourceHandler.listSourcesForWorkspace(any()) } returns SourceReadList(sources = emptyList()) andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ActorListCursorPaginatedRequestBody(workspaceId = WORKSPACE_ID))))
    assertStatus(
      HttpStatus.NOT_FOUND,
      client.statusException(HttpRequest.POST(path, ActorListCursorPaginatedRequestBody(workspaceId = WORKSPACE_ID))),
    )
  }

  @Test
  fun testSearchSources() {
    every { sourceHandler.searchSources(any()) } returns SourceReadList(sources = emptyList()) andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/search"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceSearch())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceSearch())))
  }

  @Test
  fun testUpdateSources() {
    every { sourceHandler.updateSource(any()) } returns sourceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, sourceUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, sourceUpdate())))
  }

  @Test
  fun testWriteDiscoverCatalogResult() {
    every { sourceHandler.writeDiscoverCatalogResult(any()) } returns DiscoverCatalogResult(catalogId = UUID.randomUUID())

    val sourceId = ACTOR_ID
    val workspaceId = WORKSPACE_ID
    val actors = mockk<ActorRepository>()
    every { actors.findSourceWorkspaceId(sourceId) } returns Optional.of(workspaceId)
    context.registerSingleton(ActorRepository::class.java, actors)
    val workspaces = mockk<WorkspaceService>()
    every { workspaces.getStandardWorkspaceNoSecrets(workspaceId, false) } returns
      StandardWorkspace().withWorkspaceId(workspaceId).withOrganizationId(UUID.randomUUID())
    context.registerSingleton(WorkspaceService::class.java, workspaces)

    val path = "/api/v1/sources/write_discover_catalog_result"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(path, SourceDiscoverSchemaWriteRequestBody(catalog = AirbyteCatalog(streams = emptyList()), sourceId = sourceId)),
      ),
    )
  }

  @Test
  fun testUpgradeSourceVersion() {
    every { sourceHandler.upgradeSourceVersion(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/upgrade_version"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, sourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, sourceIdRequestBody())))
  }

  @Test
  fun `source write endpoints allow the source editor and reject the destination editor`() {
    val writeEndpoints =
      setOf(
        "applySchemaChangeForSource",
        "checkConnectionToSource",
        "checkConnectionToSourceForUpdate",
        "createSource",
        "deleteSource",
        "discoverSchemaForSource",
        "updateSource",
        "upgradeSourceVersion",
        "partialUpdateSource",
      )

    assertActorControllerPermissions(
      SourceApiController::class.java,
      writeEndpoints,
      Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
    )
  }

  @Test
  fun `source read endpoints allow saved readers and reject organization members`() {
    val readEndpoints = setOf("getSource", "getSourceWithMetadata", "listSourcesForWorkspace", "getMostRecentSourceActorCatalog")

    assertActorControllerPermissions(
      SourceApiController::class.java,
      readEndpoints,
      Permission.PermissionType.WORKSPACE_READER,
      Permission.PermissionType.ORGANIZATION_MEMBER,
    )
  }

  companion object {
    val ACTOR_ID: UUID = UUID(0, 500)
    val WORKSPACE_ID: UUID = UUID(0, 501)
    val DEFINITION_ID: UUID = UUID(0, 502)
  }

  private fun sourceIdRequestBody() = SourceIdRequestBody(sourceId = ACTOR_ID)

  private fun sourceUpdate() = SourceUpdate(sourceId = ACTOR_ID, connectionConfiguration = Jsons.emptyObject(), name = "source")

  private fun sourceCreate() =
    SourceCreate(
      sourceDefinitionId = DEFINITION_ID,
      connectionConfiguration = Jsons.emptyObject(),
      workspaceId = WORKSPACE_ID,
      name = "source",
    )

  private fun sourceRead() =
    SourceRead(
      sourceDefinitionId = DEFINITION_ID,
      sourceId = ACTOR_ID,
      workspaceId = WORKSPACE_ID,
      connectionConfiguration = Jsons.emptyObject(),
      name = "source",
      sourceName = "source-definition",
      createdAt = 0L,
    )

  private fun sourceReadWithMetadata() =
    SourceReadWithMetadata(
      sourceDefinitionId = DEFINITION_ID,
      sourceId = ACTOR_ID,
      workspaceId = WORKSPACE_ID,
      connectionConfiguration = Jsons.emptyObject(),
      name = "source",
      sourceName = "source-definition",
      createdAt = 0L,
      metadata = SourceMetadata(workspaceName = "workspace"),
    )

  private fun synchronousJobRead(configType: JobConfigType) =
    SynchronousJobRead(
      id = UUID.randomUUID(),
      configType = configType,
      createdAt = 0L,
      endedAt = 0L,
      succeeded = true,
    )

  private fun checkConnectionRead() =
    CheckConnectionRead(
      status = CheckConnectionRead.Status.SUCCEEDED,
      jobInfo = synchronousJobRead(JobConfigType.CHECK_CONNECTION_SOURCE),
    )

  private fun sourceDiscoverSchemaRead() = SourceDiscoverSchemaRead(jobInfo = synchronousJobRead(JobConfigType.DISCOVER_SCHEMA))
}
