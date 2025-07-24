/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ActorCatalogWithUpdatedAt
import io.airbyte.api.model.generated.CheckConnectionRead
import io.airbyte.api.model.generated.DiscoverCatalogResult
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.model.generated.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SourceReadList
import io.airbyte.api.model.generated.SourceSearch
import io.airbyte.api.model.generated.SourceUpdate
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

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
    every { schedulerHandler.checkSourceConnectionFromSourceId(any()) } returns CheckConnectionRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/check_connection"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testCheckConnectionToSourceForUpdate() {
    every { schedulerHandler.checkSourceConnectionFromSourceIdForUpdate(any()) } returns CheckConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/sources/check_connection_for_update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceUpdate())))
  }

  @Test
  fun testCreateSource() {
    every { sourceHandler.createSourceWithOptionalSecret(any()) } returns SourceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceCreate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceCreate())))
  }

  @Test
  fun testDeleteSource() {
    every { sourceHandler.deleteSource(any<SourceIdRequestBody>()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testDiscoverSchemaForSource() {
    every { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) } returns SourceDiscoverSchemaRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/sources/discover_schema"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceDiscoverSchemaRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDiscoverSchemaRequestBody())))
  }

  @Test
  fun testGetSource() {
    every { sourceHandler.getSource(any()) } returns SourceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testGetMostRecentSourceActorCatalog() {
    every { sourceHandler.getMostRecentSourceActorCatalogWithUpdatedAt(any()) } returns ActorCatalogWithUpdatedAt()

    val path = "/api/v1/sources/most_recent_source_actor_catalog"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testListSourcesForWorkspace() {
    every { sourceHandler.listSourcesForWorkspace(any()) } returns SourceReadList() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, WorkspaceIdRequestBody())))
  }

  @Test
  fun testSearchSources() {
    every { sourceHandler.searchSources(any()) } returns SourceReadList() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/search"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceSearch())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceSearch())))
  }

  @Test
  fun testUpdateSources() {
    every { sourceHandler.updateSource(any()) } returns SourceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceUpdate())))
  }

  @Test
  fun testWriteDiscoverCatalogResult() {
    every { sourceHandler.writeDiscoverCatalogResult(any()) } returns DiscoverCatalogResult()

    val path = "/api/v1/sources/write_discover_catalog_result"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceDiscoverSchemaWriteRequestBody())))
  }

  @Test
  fun testUpgradeSourceVersion() {
    every { sourceHandler.upgradeSourceVersion(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/sources/upgrade_version"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceIdRequestBody())))
  }
}
