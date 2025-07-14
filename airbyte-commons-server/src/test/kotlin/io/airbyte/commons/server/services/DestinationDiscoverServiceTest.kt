/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services

import io.airbyte.api.problems.throwable.generated.DestinationCatalogNotFoundProblem
import io.airbyte.api.problems.throwable.generated.DestinationDiscoverNotSupportedProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.toModel
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.ConnectionId
import io.airbyte.domain.models.DestinationCatalogId
import io.airbyte.protocol.models.v0.DestinationOperation
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID
import io.airbyte.protocol.models.v0.DestinationCatalog as ProtocolDestinationCatalog

class DestinationDiscoverServiceTest {
  private val destinationService: DestinationService = mockk()
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper = mockk()
  private val catalogService: CatalogService = mockk()
  private val connectionService: ConnectionService = mockk()
  private val synchronousSchedulerClient: SynchronousSchedulerClient = mockk()

  private val service =
    DestinationDiscoverService(
      destinationService,
      actorDefinitionVersionHelper,
      catalogService,
      connectionService,
      synchronousSchedulerClient,
    )

  private val destinationId = ActorId(UUID.randomUUID())
  private val destinationDefinitionId = UUID.randomUUID()
  private val connectionId = ConnectionId(UUID.randomUUID())
  private val workspaceId = UUID.randomUUID()
  private val catalogId = UUID.randomUUID()
  private val mockCatalog =
    ProtocolDestinationCatalog().withOperations(
      listOf(
        DestinationOperation()
          .withObjectName("person_events")
          .withSyncMode(DestinationSyncMode.APPEND_DEDUP)
          .withJsonSchema(
            Jsons.jsonNode(
              mapOf(
                "type" to "object",
                "additionalProperties" to true,
                "properties" to
                  mapOf(
                    "person_cio_id" to
                      mapOf(
                        "type" to "string",
                        "description" to "The unique identifier for the person.",
                      ),
                    "email" to
                      mapOf(
                        "type" to "string",
                        "description" to "The email address of the person.",
                      ),
                  ),
              ),
            ),
          ).withMatchingKeys(
            listOf(
              listOf("person_cio_id"),
              listOf("email"),
            ),
          ),
      ),
    )

  @Nested
  inner class GetDestinationCatalog {
    @Test
    fun `should return cached catalog when available`() {
      val destination =
        DestinationConnection()
          .withWorkspaceId(workspaceId)
          .withDestinationDefinitionId(destinationDefinitionId)
          .withDestinationId(destinationId.value)
          .withConfiguration(Jsons.emptyObject())

      val destinationDefinition = StandardDestinationDefinition()
      val destinationVersion =
        ActorDefinitionVersion()
          .withSupportsDataActivation(true)
          .withDockerImageTag("1.0.0")

      val cachedCatalog =
        ActorCatalog()
          .withId(catalogId)
          .withCatalog(Jsons.jsonNode(mockCatalog))

      every { destinationService.getDestinationConnection(destinationId.value) } returns destination
      every { destinationService.getStandardDestinationDefinition(destinationDefinitionId) } returns destinationDefinition
      every { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, destinationId.value) } returns destinationVersion
      every { catalogService.getActorCatalog(destinationId.value, "1.0.0", any()) } returns Optional.of(cachedCatalog)

      val result = service.getDestinationCatalog(destinationId)

      result.catalogId shouldBe DestinationCatalogId(catalogId)
      result.catalog shouldBe mockCatalog.toModel()
    }

    @Test
    fun `should discover and return new catalog when cache miss`() {
      val destination =
        DestinationConnection()
          .withWorkspaceId(workspaceId)
          .withDestinationDefinitionId(destinationDefinitionId)
          .withDestinationId(destinationId.value)
          .withConfiguration(Jsons.emptyObject())

      val destinationDefinition = StandardDestinationDefinition()
      val destinationVersion =
        ActorDefinitionVersion()
          .withDockerImageTag("1.0.0")
          .withSupportsDataActivation(true)

      val discoveredCatalog =
        ActorCatalog()
          .withId(catalogId)
          .withCatalog(Jsons.jsonNode(mockCatalog))

      every { destinationService.getDestinationConnection(destinationId.value) } returns destination
      every { destinationService.getStandardDestinationDefinition(destinationDefinitionId) } returns destinationDefinition
      every { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, destinationId.value) } returns destinationVersion
      every { catalogService.getActorCatalog(destinationId.value, "1.0.0", any()) } returns Optional.empty()
      every { synchronousSchedulerClient.createDestinationDiscoverJob(destination, destinationDefinition, destinationVersion) } returns
        mockk {
          every { output } returns catalogId
        }
      every { catalogService.getActorCatalogById(catalogId) } returns discoveredCatalog

      val result = service.getDestinationCatalog(destinationId)

      result.catalogId shouldBe DestinationCatalogId(catalogId)
      result.catalog shouldBe mockCatalog.toModel()

      verify {
        catalogService.getActorCatalog(destinationId.value, "1.0.0", any())
        synchronousSchedulerClient.createDestinationDiscoverJob(destination, destinationDefinition, destinationVersion)
        catalogService.getActorCatalogById(catalogId)
      }
    }

    @Test
    fun `should throw DestinationDiscoverNotSupportedProblem when destination version does not support data activation`() {
      val destination =
        DestinationConnection()
          .withWorkspaceId(workspaceId)
          .withDestinationDefinitionId(destinationDefinitionId)
          .withDestinationId(destinationId.value)
          .withConfiguration(Jsons.emptyObject())

      val destinationDefinition = StandardDestinationDefinition()
      val destinationVersion =
        ActorDefinitionVersion()
          .withDockerImageTag("1.0.0")
          .withSupportsDataActivation(false)

      every { destinationService.getDestinationConnection(destinationId.value) } returns destination
      every { destinationService.getStandardDestinationDefinition(destinationDefinitionId) } returns destinationDefinition
      every { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, destinationId.value) } returns destinationVersion

      assertThrows<DestinationDiscoverNotSupportedProblem> {
        service.getDestinationCatalog(destinationId)
      }
    }

    @Test
    fun `should throw DestinationCatalogNotFoundProblem when connection has no catalog id`() {
      val connection =
        StandardSync()
          .withDestinationCatalogId(null)

      every { connectionService.getStandardSync(connectionId.value) } returns connection

      assertThrows<DestinationCatalogNotFoundProblem> {
        service.getDestinationCatalog(connectionId)
      }
    }

    @Test
    fun `should return catalog for connection when catalog id exists`() {
      val connection =
        StandardSync()
          .withDestinationCatalogId(catalogId)

      val catalog =
        ActorCatalog()
          .withId(catalogId)
          .withCatalog(Jsons.jsonNode(mockCatalog))

      every { connectionService.getStandardSync(connectionId.value) } returns connection
      every { catalogService.getActorCatalogById(catalogId) } returns catalog

      val result = service.getDestinationCatalog(connectionId)

      result.catalogId shouldBe DestinationCatalogId(catalogId)
      result.catalog shouldBe mockCatalog.toModel()
    }
  }

  @Nested
  inner class WriteCatalogResult {
    @Test
    fun `should write discovered catalog result for destination`() {
      val destinationId = ActorId(UUID.randomUUID())
      val catalogId = DestinationCatalogId(UUID.randomUUID())
      val destinationVersion = "1.0.0"
      val configHash = "my-hash"

      every { catalogService.writeActorCatalogWithFetchEvent(any<ProtocolDestinationCatalog>(), any(), any(), any()) } returns catalogId.value

      val res = service.writeDiscoverCatalogResult(destinationId, mockCatalog.toModel(), configHash, destinationVersion)

      res shouldBe catalogId

      verify {
        catalogService.writeActorCatalogWithFetchEvent(
          mockCatalog,
          destinationId.value,
          destinationVersion,
          configHash,
        )
      }
    }
  }
}
