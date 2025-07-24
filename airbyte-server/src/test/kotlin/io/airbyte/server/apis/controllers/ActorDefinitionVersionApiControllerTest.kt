/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.commons.server.handlers.ActorDefinitionVersionHandler
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
internal class ActorDefinitionVersionApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var actorDefinitionVersionHandler: ActorDefinitionVersionHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    actorDefinitionVersionHandler = mockk()
    context.registerSingleton(ActorDefinitionVersionHandler::class.java, actorDefinitionVersionHandler)
  }

  @Test
  fun testGetActorDefinitionForSource() {
    every { actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(any()) } returns
      ActorDefinitionVersionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/actor_definition_versions/get_for_source"

    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testGetActorDefinitionForDestination() {
    every { actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(any()) } returns ActorDefinitionVersionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/actor_definition_versions/get_for_destination"

    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationIdRequestBody())))
  }
}
