/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.throwable.generated.ActorNotReadyProblem
import io.airbyte.api.server.generated.models.ActorListCursorPaginatedRequestBody
import io.airbyte.api.server.generated.models.CheckConnectionRead
import io.airbyte.api.server.generated.models.DestinationCreate
import io.airbyte.api.server.generated.models.DestinationDiscoverSchemaRequestBody
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.DestinationReadList
import io.airbyte.api.server.generated.models.DestinationSearch
import io.airbyte.api.server.generated.models.DestinationUpdate
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.services.DestinationDiscoverService
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import jakarta.validation.ConstraintViolationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
internal class DestinationApiControllerTest {
  @Factory
  class TestFactory {
    @Singleton
    @Replaces(DestinationDiscoverService::class)
    fun destinationDiscoverService(): DestinationDiscoverService = mockk()
  }

  @Inject
  lateinit var context: ApplicationContext

  lateinit var schedulerHandler: SchedulerHandler

  lateinit var destinationHandler: DestinationHandler

  @Inject
  lateinit var destinationDiscoverService: DestinationDiscoverService

  @BeforeAll
  fun setupMock() {
    schedulerHandler = mockk()
    context.registerSingleton(SchedulerHandler::class.java, schedulerHandler)

    destinationHandler = mockk()
    context.registerSingleton(DestinationHandler::class.java, destinationHandler)
    val owner = WorkspaceAndOrganization(WORKSPACE_ID, UUID(0, 503))
    val authorization = mockk<AuthorizationDataService>()
    every { authorization.savedDestinationWorkspaceAndOrganization(any()) } returns owner
    every { authorization.workspaceAndOrganization(WORKSPACE_ID) } returns owner
    every { authorization.requireActorDefinitionType(any(), any()) } returns Unit
    every { authorization.canUseActorDefinition(any(), any()) } returns true
    every { authorization.actorConnectionIds(any(), any(), any()) } returns emptyList()
    context.registerSingleton(AuthorizationDataService::class.java, authorization)
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Test
  fun testCheckConnectionToDestination() {
    every { schedulerHandler.checkDestinationConnectionFromDestinationId(any()) } returns checkConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/check_connection"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, destinationIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, destinationIdRequestBody())))
  }

  @Test
  fun testCheckConnectionToDestinationForUpdate() {
    every { schedulerHandler.checkDestinationConnectionFromDestinationIdForUpdate(any()) } returns checkConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/check_connection_for_update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, destinationUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, destinationUpdate())))
  }

  @Test
  fun testCreateDestination() {
    every { destinationHandler.createDestination(any()) } returns destinationRead() andThenThrows ConstraintViolationException(setOf())

    val path = "/api/v1/destinations/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, destinationCreate())))
    assertStatus(HttpStatus.BAD_REQUEST, client.statusException(HttpRequest.POST(path, destinationCreate())))
  }

  @Test
  fun testDeleteDestination() {
    every { destinationHandler.deleteDestination(any<DestinationIdRequestBody>()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, destinationIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, destinationIdRequestBody())))
  }

  @Test
  fun `draft destination discovery returns the stable actor-not-ready problem body`() {
    every { destinationDiscoverService.getDestinationCatalog(any(), any()) } throws ActorNotReadyProblem()

    val error =
      assertThrows<HttpClientResponseException> {
        client
          .toBlocking()
          .exchange(
            HttpRequest.POST(
              "/api/v1/destinations/discover_schema",
              DestinationDiscoverSchemaRequestBody(destinationId = ACTOR_ID, disableCache = false),
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
  fun testGetDestination() {
    every { destinationHandler.getDestination(any()) } returns destinationRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, destinationIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, destinationIdRequestBody())))
  }

  @Test
  fun testListDestination() {
    every { destinationHandler.listDestinationsForWorkspace(any()) } returns DestinationReadList(destinations = emptyList()) andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ActorListCursorPaginatedRequestBody(workspaceId = WORKSPACE_ID))))
    assertStatus(
      HttpStatus.NOT_FOUND,
      client.statusException(HttpRequest.POST(path, ActorListCursorPaginatedRequestBody(workspaceId = WORKSPACE_ID))),
    )
  }

  @Test
  fun testSearchDestination() {
    every { destinationHandler.searchDestinations(any()) } returns DestinationReadList(destinations = emptyList()) andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/search"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationSearch())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationSearch())))
  }

  @Test
  fun testUpdateDestination() {
    every { destinationHandler.updateDestination(any()) } returns destinationRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, destinationUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, destinationUpdate())))
  }

  @Test
  fun testUpgradeDestinationVersion() {
    every { destinationHandler.upgradeDestinationVersion(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/upgrade_version"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, destinationIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, destinationIdRequestBody())))
  }

  @Test
  fun `destination write endpoints allow the destination editor and reject the source editor`() {
    val writeEndpoints =
      setOf(
        "checkConnectionToDestination",
        "checkConnectionToDestinationForUpdate",
        "createDestination",
        "deleteDestination",
        "updateDestination",
        "upgradeDestinationVersion",
        "partialUpdateDestination",
      )

    assertActorControllerPermissions(
      DestinationApiController::class.java,
      writeEndpoints,
      Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
      Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
    )
  }

  @Test
  fun `destination read endpoints allow saved readers and reject organization members`() {
    val readEndpoints =
      setOf("getDestination", "listDestinationsForWorkspace", "discoverCatalogForDestination", "getCatalogForConnection")

    assertActorControllerPermissions(
      DestinationApiController::class.java,
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

  private fun destinationIdRequestBody() = DestinationIdRequestBody(destinationId = ACTOR_ID)

  private fun destinationUpdate() = DestinationUpdate(destinationId = ACTOR_ID, connectionConfiguration = Jsons.emptyObject(), name = "destination")

  private fun destinationCreate() =
    DestinationCreate(
      workspaceId = WORKSPACE_ID,
      name = "destination",
      destinationDefinitionId = DEFINITION_ID,
      connectionConfiguration = Jsons.emptyObject(),
    )

  private fun destinationRead() =
    DestinationRead(
      destinationDefinitionId = DEFINITION_ID,
      destinationId = ACTOR_ID,
      workspaceId = WORKSPACE_ID,
      connectionConfiguration = Jsons.emptyObject(),
      name = "destination",
      destinationName = "destination-definition",
      createdAt = 0L,
    )

  private fun checkConnectionRead() =
    CheckConnectionRead(
      status = CheckConnectionRead.Status.SUCCEEDED,
      jobInfo =
        SynchronousJobRead(
          id = UUID.randomUUID(),
          configType = JobConfigType.CHECK_CONNECTION_DESTINATION,
          createdAt = 0L,
          endedAt = 0L,
          succeeded = true,
        ),
    )
}
