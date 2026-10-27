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
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.WorkspaceService
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.security.annotation.Secured
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
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
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceDiscoverSchemaRequestBody(sourceId = UUID.randomUUID()))))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDiscoverSchemaRequestBody(sourceId = UUID.randomUUID()))))
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
              SourceDiscoverSchemaRequestBody(sourceId = UUID.randomUUID()),
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
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ActorListCursorPaginatedRequestBody(workspaceId = UUID.randomUUID()))))
    assertStatus(
      HttpStatus.NOT_FOUND,
      client.statusException(HttpRequest.POST(path, ActorListCursorPaginatedRequestBody(workspaceId = UUID.randomUUID()))),
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

    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
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

    val rolesByEndpoint =
      SourceApiController::class.java.declaredMethods
        .filter { it.name in writeEndpoints && it.getAnnotation(Secured::class.java) != null }
        .associate { it.name to it.getAnnotation(Secured::class.java).value.toList() }

    assertEquals(writeEndpoints, rolesByEndpoint.keys)

    rolesByEndpoint.forEach { (name, roles) ->
      assertTrue(roles.contains(AuthRoleConstants.WORKSPACE_SOURCE_EDITOR), "$name should allow the source editor")
      assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR), "$name should not allow the destination editor")
      assertTrue(roles.contains(AuthRoleConstants.WORKSPACE_EDITOR), "$name should still allow the workspace editor")
    }
  }

  @Test
  fun `source read endpoints do not gain either actor-scoped editor role`() {
    val readEndpoints = setOf("getSource", "getSourceWithMetadata", "listSourcesForWorkspace", "getMostRecentSourceActorCatalog")

    SourceApiController::class.java.declaredMethods
      .filter { it.name in readEndpoints && it.getAnnotation(Secured::class.java) != null }
      .forEach {
        val roles = it.getAnnotation(Secured::class.java).value.toList()
        assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_SOURCE_EDITOR), "${it.name} should stay reader-scoped")
        assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR), "${it.name} should stay reader-scoped")
      }
  }

  private fun sourceIdRequestBody() = SourceIdRequestBody(sourceId = UUID.randomUUID())

  private fun sourceUpdate() = SourceUpdate(sourceId = UUID.randomUUID(), connectionConfiguration = Jsons.emptyObject(), name = "source")

  private fun sourceCreate() =
    SourceCreate(
      sourceDefinitionId = UUID.randomUUID(),
      connectionConfiguration = Jsons.emptyObject(),
      workspaceId = UUID.randomUUID(),
      name = "source",
    )

  private fun sourceRead() =
    SourceRead(
      sourceDefinitionId = UUID.randomUUID(),
      sourceId = UUID.randomUUID(),
      workspaceId = UUID.randomUUID(),
      connectionConfiguration = Jsons.emptyObject(),
      name = "source",
      sourceName = "source-definition",
      createdAt = 0L,
    )

  private fun sourceReadWithMetadata() =
    SourceReadWithMetadata(
      sourceDefinitionId = UUID.randomUUID(),
      sourceId = UUID.randomUUID(),
      workspaceId = UUID.randomUUID(),
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
