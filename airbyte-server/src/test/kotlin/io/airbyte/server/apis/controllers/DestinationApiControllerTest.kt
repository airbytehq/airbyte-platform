/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.CheckConnectionRead
import io.airbyte.api.model.generated.DestinationCreate
import io.airbyte.api.model.generated.DestinationDiscoverSchemaRequestBody
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationReadList
import io.airbyte.api.model.generated.DestinationSearch
import io.airbyte.api.model.generated.DestinationUpdate
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.problems.throwable.generated.ActorNotReadyProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.services.DestinationDiscoverService
import io.airbyte.data.ConfigNotFoundException
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
import io.micronaut.security.annotation.Secured
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import jakarta.validation.ConstraintViolationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
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
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Test
  fun testCheckConnectionToDestination() {
    every { schedulerHandler.checkDestinationConnectionFromDestinationId(any()) } returns CheckConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/check_connection"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationIdRequestBody())))
  }

  @Test
  fun testCheckConnectionToDestinationForUpdate() {
    every { schedulerHandler.checkDestinationConnectionFromDestinationIdForUpdate(any()) } returns CheckConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/check_connection_for_update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationUpdate())))
  }

  @Test
  fun testCreateDestination() {
    every { destinationHandler.createDestination(any()) } returns DestinationRead() andThenThrows ConstraintViolationException(setOf())

    val path = "/api/v1/destinations/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationCreate())))
    assertStatus(HttpStatus.BAD_REQUEST, client.statusException(HttpRequest.POST(path, DestinationCreate())))
  }

  @Test
  fun testDeleteDestination() {
    every { destinationHandler.deleteDestination(any<DestinationIdRequestBody>()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, DestinationIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationIdRequestBody())))
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
              DestinationDiscoverSchemaRequestBody().destinationId(UUID.randomUUID()).disableCache(false),
            ),
            JsonNode::class.java,
          )
      }

    assertEquals(HttpStatus.CONFLICT, error.status)
    val body = error.response.getBody(JsonNode::class.java).orElseThrow()
    assertEquals(409, body.path("status").asInt())
    assertEquals("actor-not-ready", body.path("title").asText())
    assertEquals("https://reference.airbyte.com/reference/errors#409-actor-not-ready", body.path("type").asText())
    assertEquals("The source or destination is not ready. Complete connector setup first.", body.path("detail").asText())
  }

  @Test
  fun testGetDestination() {
    every { destinationHandler.getDestination(any()) } returns DestinationRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationIdRequestBody())))
  }

  @Test
  fun testListDestination() {
    every { destinationHandler.listDestinationsForWorkspace(any()) } returns DestinationReadList() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, WorkspaceIdRequestBody())))
  }

  @Test
  fun testSearchDestination() {
    every { destinationHandler.searchDestinations(any()) } returns DestinationReadList() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/search"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationSearch())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationSearch())))
  }

  @Test
  fun testUpdateDestination() {
    every { destinationHandler.updateDestination(any()) } returns DestinationRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationUpdate())))
  }

  @Test
  fun testUpgradeDestinationVersion() {
    every { destinationHandler.upgradeDestinationVersion(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destinations/upgrade_version"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, DestinationIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationIdRequestBody())))
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

    val rolesByEndpoint =
      DestinationApiController::class.java.declaredMethods
        .filter { it.name in writeEndpoints && it.getAnnotation(Secured::class.java) != null }
        .associate { it.name to it.getAnnotation(Secured::class.java).value.toList() }

    assertEquals(writeEndpoints, rolesByEndpoint.keys)

    rolesByEndpoint.forEach { (name, roles) ->
      assertTrue(roles.contains(AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR), "$name should allow the destination editor")
      assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_SOURCE_EDITOR), "$name should not allow the source editor")
      assertTrue(roles.contains(AuthRoleConstants.WORKSPACE_EDITOR), "$name should still allow the workspace editor")
    }
  }

  @Test
  fun `destination read endpoints do not gain either actor-scoped editor role`() {
    val readEndpoints =
      setOf("getDestination", "listDestinationsForWorkspace", "discoverCatalogForDestination", "getCatalogForConnection")

    DestinationApiController::class.java.declaredMethods
      .filter { it.name in readEndpoints && it.getAnnotation(Secured::class.java) != null }
      .forEach {
        val roles = it.getAnnotation(Secured::class.java).value.toList()
        assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_SOURCE_EDITOR), "${it.name} should stay reader-scoped")
        assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR), "${it.name} should stay reader-scoped")
      }
  }
}
