/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ActorType
import io.airbyte.api.model.generated.ConnectorDocumentationRead
import io.airbyte.api.model.generated.ConnectorDocumentationRequestBody
import io.airbyte.commons.server.errors.NotFoundException
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.Optional
import java.util.UUID

internal class ConnectorDocumentationHandlerTest {
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var remoteDefinitionsProvider: RemoteDefinitionsProvider
  private lateinit var connectorDocumentationHandler: ConnectorDocumentationHandler

  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService

  @BeforeEach
  fun setup() {
    actorDefinitionVersionHelper = mock()
    remoteDefinitionsProvider = mock()
    sourceService = mock()
    destinationService = mock()

    connectorDocumentationHandler =
      ConnectorDocumentationHandler(actorDefinitionVersionHelper, remoteDefinitionsProvider, sourceService, destinationService)
  }

  // SOURCE
  @Test
  fun testNoSourceDocumentationFound() {
    val sourceDefinitionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    whenever(sourceService.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION)
    whenever(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, sourceId)).thenReturn(SOURCE_DEFINITION_VERSION_OLD)
    whenever(remoteDefinitionsProvider.getConnectorDocumentation(any(), any())).thenReturn(Optional.empty())

    val request =
      ConnectorDocumentationRequestBody()
        .actorType(ActorType.SOURCE)
        .actorDefinitionId(sourceDefinitionId)
        .workspaceId(workspaceId)
        .actorId(sourceId)

    Assertions.assertThrows(NotFoundException::class.java) {
      connectorDocumentationHandler.getConnectorDocumentation(request)
    }
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetVersionedExistingSourceDocumentation() {
    val sourceDefinitionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION)
    whenever<ActorDefinitionVersion?>(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, sourceId)).thenReturn(
      SOURCE_DEFINITION_VERSION_OLD,
    )

    whenever<Optional<String>?>(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_OLD))
      .thenReturn(Optional.of<String>(DOC_CONTENTS_OLD))

    val request =
      ConnectorDocumentationRequestBody()
        .actorType(ActorType.SOURCE)
        .actorDefinitionId(sourceDefinitionId)
        .workspaceId(workspaceId)
        .actorId(sourceId)

    val expectedResult =
      ConnectorDocumentationRead().doc(DOC_CONTENTS_OLD)
    val actualResult = connectorDocumentationHandler.getConnectorDocumentation(request)

    Assertions.assertEquals(expectedResult, actualResult)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetLatestExistingSourceDocumentation() {
    val sourceDefinitionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION)
    whenever<ActorDefinitionVersion?>(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, sourceId)).thenReturn(
      SOURCE_DEFINITION_VERSION_OLD,
    )

    whenever<Optional<String>?>(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_OLD)).thenReturn(
      Optional.empty<String>(),
    )
    whenever<Optional<String>?>(
      remoteDefinitionsProvider.getConnectorDocumentation(
        SOURCE_DOCKER_REPO,
        ConnectorDocumentationHandler.Companion.LATEST,
      ),
    ).thenReturn(
      Optional.of<String>(DOC_CONTENTS_LATEST),
    )

    val request =
      ConnectorDocumentationRequestBody()
        .actorType(ActorType.SOURCE)
        .actorDefinitionId(sourceDefinitionId)
        .workspaceId(workspaceId)
        .actorId(sourceId)

    val expectedResult =
      ConnectorDocumentationRead().doc(DOC_CONTENTS_LATEST)
    val actualResult = connectorDocumentationHandler.getConnectorDocumentation(request)

    Assertions.assertEquals(expectedResult, actualResult)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetVersionedNewSourceDocumentation() {
    val sourceDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION)
    whenever<ActorDefinitionVersion?>(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, null)).thenReturn(
      SOURCE_DEFINITION_VERSION_LATEST,
    )

    whenever<Optional<String>?>(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_LATEST))
      .thenReturn(Optional.of<String>(DOC_CONTENTS_LATEST))

    val request =
      ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE).actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId)

    val expectedResult =
      ConnectorDocumentationRead().doc(DOC_CONTENTS_LATEST)
    val actualResult = connectorDocumentationHandler.getConnectorDocumentation(request)

    Assertions.assertEquals(expectedResult, actualResult)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetLatestNewSourceDocumentation() {
    val sourceDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION)
    whenever<ActorDefinitionVersion?>(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, null)).thenReturn(
      SOURCE_DEFINITION_VERSION_LATEST,
    )

    whenever<Optional<String>?>(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_LATEST))
      .thenReturn(
        Optional.empty<String>(),
      )
    whenever<Optional<String>?>(
      remoteDefinitionsProvider.getConnectorDocumentation(
        SOURCE_DOCKER_REPO,
        ConnectorDocumentationHandler.Companion.LATEST,
      ),
    ).thenReturn(
      Optional.of<String>(DOC_CONTENTS_LATEST),
    )

    val request =
      ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE).actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId)

    val expectedResult =
      ConnectorDocumentationRead().doc(DOC_CONTENTS_LATEST)
    val actualResult = connectorDocumentationHandler.getConnectorDocumentation(request)

    Assertions.assertEquals(expectedResult, actualResult)
  }

  // DESTINATION
  @Test
  fun testNoDestinationDocumentationFound() {
    val destinationDefinitionId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    whenever(destinationService.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(DESTINATION_DEFINITION)
    whenever(
      actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, destinationId),
    ).thenReturn(DESTINATION_DEFINITION_VERSION_OLD)

    whenever(remoteDefinitionsProvider.getConnectorDocumentation(any(), any())).thenReturn(Optional.empty())

    val request =
      ConnectorDocumentationRequestBody()
        .actorType(ActorType.DESTINATION)
        .actorDefinitionId(destinationDefinitionId)
        .workspaceId(workspaceId)
        .actorId(destinationId)

    Assertions.assertThrows(NotFoundException::class.java) {
      connectorDocumentationHandler.getConnectorDocumentation(request)
    }
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetVersionedExistingDestinationDocumentation() {
    val destinationDefinitionId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    whenever<StandardDestinationDefinition?>(destinationService.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(
      DESTINATION_DEFINITION,
    )
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getDestinationVersion(
        DESTINATION_DEFINITION,
        workspaceId,
        destinationId,
      ),
    ).thenReturn(DESTINATION_DEFINITION_VERSION_OLD)

    whenever<Optional<String>?>(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_OLD))
      .thenReturn(Optional.of<String>(DOC_CONTENTS_OLD))

    val request =
      ConnectorDocumentationRequestBody()
        .actorType(ActorType.DESTINATION)
        .actorDefinitionId(destinationDefinitionId)
        .workspaceId(workspaceId)
        .actorId(destinationId)

    val expectedResult =
      ConnectorDocumentationRead().doc(DOC_CONTENTS_OLD)
    val actualResult = connectorDocumentationHandler.getConnectorDocumentation(request)

    Assertions.assertEquals(expectedResult, actualResult)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetLatestExistingDestinationDocumentation() {
    val destinationDefinitionId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    whenever<StandardDestinationDefinition?>(destinationService.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(
      DESTINATION_DEFINITION,
    )
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getDestinationVersion(
        DESTINATION_DEFINITION,
        workspaceId,
        destinationId,
      ),
    ).thenReturn(DESTINATION_DEFINITION_VERSION_OLD)

    whenever<Optional<String>?>(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_OLD))
      .thenReturn(
        Optional.empty<String>(),
      )
    whenever<Optional<String>?>(
      remoteDefinitionsProvider.getConnectorDocumentation(
        DESTINATION_DOCKER_REPO,
        ConnectorDocumentationHandler.Companion.LATEST,
      ),
    ).thenReturn(Optional.of<String>(DOC_CONTENTS_LATEST))

    val request =
      ConnectorDocumentationRequestBody()
        .actorType(ActorType.DESTINATION)
        .actorDefinitionId(destinationDefinitionId)
        .workspaceId(workspaceId)
        .actorId(destinationId)

    val expectedResult =
      ConnectorDocumentationRead().doc(DOC_CONTENTS_LATEST)
    val actualResult = connectorDocumentationHandler.getConnectorDocumentation(request)

    Assertions.assertEquals(expectedResult, actualResult)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetVersionedNewDestinationDocumentation() {
    val destinationDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    whenever<StandardDestinationDefinition?>(destinationService.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(
      DESTINATION_DEFINITION,
    )
    whenever<ActorDefinitionVersion?>(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, null))
      .thenReturn(DESTINATION_DEFINITION_VERSION_LATEST)

    whenever<Optional<String>?>(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_LATEST))
      .thenReturn(Optional.of<String>(DOC_CONTENTS_LATEST))

    val request =
      ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION).actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId)

    val expectedResult =
      ConnectorDocumentationRead().doc(DOC_CONTENTS_LATEST)
    val actualResult = connectorDocumentationHandler.getConnectorDocumentation(request)

    Assertions.assertEquals(expectedResult, actualResult)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetLatestNewDestinationDocumentation() {
    val destinationDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    whenever<StandardDestinationDefinition?>(destinationService.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(
      DESTINATION_DEFINITION,
    )
    whenever<ActorDefinitionVersion?>(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, null))
      .thenReturn(DESTINATION_DEFINITION_VERSION_LATEST)

    whenever<Optional<String>?>(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_LATEST))
      .thenReturn(Optional.empty<String>())
    whenever<Optional<String>?>(
      remoteDefinitionsProvider.getConnectorDocumentation(
        DESTINATION_DOCKER_REPO,
        ConnectorDocumentationHandler.Companion.LATEST,
      ),
    ).thenReturn(Optional.of<String>(DOC_CONTENTS_LATEST))

    val request =
      ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION).actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId)

    val expectedResult =
      ConnectorDocumentationRead().doc(DOC_CONTENTS_LATEST)
    val actualResult = connectorDocumentationHandler.getConnectorDocumentation(request)

    Assertions.assertEquals(expectedResult, actualResult)
  }

  companion object {
    private const val SOURCE_DOCKER_REPO = "airbyte/source-test"
    private const val SOURCE_VERSION_OLD = "0.0.1"
    private const val SOURCE_VERSION_LATEST = "0.0.9"
    private val SOURCE_DEFINITION = StandardSourceDefinition()
    private val SOURCE_DEFINITION_VERSION_OLD: ActorDefinitionVersion? =
      ActorDefinitionVersion().withDockerRepository(SOURCE_DOCKER_REPO).withDockerImageTag(
        SOURCE_VERSION_OLD,
      )
    private val SOURCE_DEFINITION_VERSION_LATEST: ActorDefinitionVersion? =
      ActorDefinitionVersion().withDockerRepository(SOURCE_DOCKER_REPO).withDockerImageTag(
        SOURCE_VERSION_LATEST,
      )

    private const val DESTINATION_DOCKER_REPO = "airbyte/destination-test"
    private const val DESTINATION_VERSION_OLD = "0.0.1"
    private const val DESTINATION_VERSION_LATEST = "0.0.9"
    private val DESTINATION_DEFINITION = StandardDestinationDefinition()
    private val DESTINATION_DEFINITION_VERSION_OLD: ActorDefinitionVersion? =
      ActorDefinitionVersion()
        .withDockerRepository(
          DESTINATION_DOCKER_REPO,
        ).withDockerImageTag(
          DESTINATION_VERSION_OLD,
        )
    private val DESTINATION_DEFINITION_VERSION_LATEST: ActorDefinitionVersion? =
      ActorDefinitionVersion()
        .withDockerRepository(
          DESTINATION_DOCKER_REPO,
        ).withDockerImageTag(
          DESTINATION_VERSION_LATEST,
        )

    private const val DOC_CONTENTS_OLD = "The doc contents for the old version"
    private const val DOC_CONTENTS_LATEST = "The doc contents for the latest version"
  }
}
