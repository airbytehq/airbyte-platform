/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody
import io.airbyte.api.model.generated.DeclarativeSourceManifest
import io.airbyte.api.model.generated.ListDeclarativeManifestsRequestBody
import io.airbyte.api.model.generated.UpdateActiveManifestRequestBody
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.server.errors.DeclarativeSourceNotFoundException
import io.airbyte.commons.server.errors.SourceIsNotDeclarativeException
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionConfigInjection
import io.airbyte.config.DeclarativeManifest
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import java.io.IOException
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Stream

internal class DeclarativeSourceDefinitionsHandlerTest {
  private lateinit var declarativeManifestImageVersionService: DeclarativeManifestImageVersionService
  private lateinit var connectorBuilderService: ConnectorBuilderService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var manifestInjector: DeclarativeSourceManifestInjector
  private lateinit var adaptedConnectorSpecification: ConnectorSpecification
  private lateinit var manifestConfigInjection: ActorDefinitionConfigInjection
  private lateinit var airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator

  private var handler: DeclarativeSourceDefinitionsHandler? = null

  @BeforeEach
  @Throws(JsonProcessingException::class)
  fun setUp() {
    declarativeManifestImageVersionService =
      Mockito.mock(DeclarativeManifestImageVersionService::class.java)
    connectorBuilderService = Mockito.mock(ConnectorBuilderService::class.java)
    workspaceService = Mockito.mock(WorkspaceService::class.java)
    manifestInjector = Mockito.mock(DeclarativeSourceManifestInjector::class.java)
    adaptedConnectorSpecification = Mockito.mock(ConnectorSpecification::class.java)
    manifestConfigInjection = Mockito.mock(ActorDefinitionConfigInjection::class.java)
    airbyteCompatibleConnectorsValidator = Mockito.mock(AirbyteCompatibleConnectorsValidator::class.java)

    handler =
      DeclarativeSourceDefinitionsHandler(
        declarativeManifestImageVersionService,
        connectorBuilderService,
        workspaceService,
        manifestInjector,
        airbyteCompatibleConnectorsValidator,
      )
    whenever(
      declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(
        anyOrNull(),
      ),
    ).thenReturn(A_DECLARATIVE_MANIFEST_IMAGE_VERSION)
  }

  @Test
  @Throws(IOException::class)
  fun givenSourceNotAvailableInWorkspaceWhenCreateDeclarativeSourceDefinitionManifestThenThrowException() {
    whenever(workspaceService.workspaceCanUseCustomDefinition(A_SOURCE_DEFINITION_ID, A_WORKSPACE_ID)).thenReturn(false)
    Assertions.assertThrows(DeclarativeSourceNotFoundException::class.java) {
      handler!!.createDeclarativeSourceDefinitionManifest(
        DeclarativeSourceDefinitionCreateManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID),
      )
    }
  }

  @Test
  @Throws(IOException::class)
  fun givenNoDeclarativeManifestForSourceDefinitionIdWhenCreateDeclarativeSourceDefinitionManifestThenThrowException() {
    givenSourceDefinitionAvailableInWorkspace()
    Mockito
      .`when`(connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID))
      .thenReturn(
        Stream.of(),
      )

    Assertions.assertThrows(
      SourceIsNotDeclarativeException::class.java,
    ) {
      handler!!.createDeclarativeSourceDefinitionManifest(
        DeclarativeSourceDefinitionCreateManifestRequestBody()
          .workspaceId(A_WORKSPACE_ID)
          .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
          .declarativeManifest(anyDeclarativeManifest()!!.version(A_VERSION)),
      )
    }
  }

  @Test
  @Throws(IOException::class)
  fun givenVersionAlreadyExistsWhenCreateDeclarativeSourceDefinitionManifestThenThrowException() {
    givenSourceDefinitionAvailableInWorkspace()
    Mockito
      .`when`(connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID))
      .thenReturn(Stream.of(DeclarativeManifest().withVersion(A_VERSION)))

    Assertions.assertThrows(
      ValueConflictKnownException::class.java,
    ) {
      handler!!.createDeclarativeSourceDefinitionManifest(
        DeclarativeSourceDefinitionCreateManifestRequestBody()
          .workspaceId(A_WORKSPACE_ID)
          .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
          .declarativeManifest(anyDeclarativeManifest()!!.version(A_VERSION)),
      )
    }
  }

  @Test
  @Throws(IOException::class)
  fun givenSetAsActiveWhenCreateDeclarativeSourceDefinitionManifestThenCreateDeclarativeManifest() {
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    Mockito
      .`when`(manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC))
      .thenReturn(adaptedConnectorSpecification)
    Mockito
      .`when`(
        manifestInjector.getManifestConnectorInjections(
          A_SOURCE_DEFINITION_ID,
          A_MANIFEST,
          null,
        ),
      ).thenReturn(
        listOf(manifestConfigInjection),
      )
    whenever(manifestInjector.getCdkVersion(A_MANIFEST)).thenReturn(A_CDK_VERSION)

    handler!!.createDeclarativeSourceDefinitionManifest(
      DeclarativeSourceDefinitionCreateManifestRequestBody()
        .workspaceId(A_WORKSPACE_ID)
        .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .setAsActiveManifest(true)
        .declarativeManifest(
          anyDeclarativeManifest()!!
            .manifest(A_MANIFEST)
            .spec(A_SPEC)
            .version(A_VERSION)
            .description(A_DESCRIPTION),
        ),
    )

    Mockito.verify(manifestInjector, Mockito.times(1)).addInjectedDeclarativeManifest(A_SPEC)
    Mockito.verify(manifestInjector, Mockito.times(1)).getCdkVersion(A_MANIFEST)
    Mockito.verify(connectorBuilderService, Mockito.times(1)).createDeclarativeManifestAsActiveVersion(
      eq(
        DeclarativeManifest()
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withVersion(A_VERSION)
          .withDescription(A_DESCRIPTION)
          .withManifest(A_MANIFEST)
          .withSpec(A_SPEC),
      ),
      eq(listOf(manifestConfigInjection)),
      eq(adaptedConnectorSpecification),
      eq(AN_IMAGE_VERSION),
    )
  }

  @Test
  @Throws(IOException::class)
  fun givenNotSetAsActiveWhenCreateDeclarativeSourceDefinitionManifestThenCreateDeclarativeManifest() {
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()

    handler!!.createDeclarativeSourceDefinitionManifest(
      DeclarativeSourceDefinitionCreateManifestRequestBody()
        .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .workspaceId(A_WORKSPACE_ID)
        .setAsActiveManifest(false)
        .declarativeManifest(
          anyDeclarativeManifest()!!
            .manifest(A_MANIFEST)
            .spec(A_SPEC)
            .version(A_VERSION)
            .description(A_DESCRIPTION),
        ),
    )

    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .insertDeclarativeManifest(
        eq(
          DeclarativeManifest()
            .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
            .withVersion(A_VERSION)
            .withDescription(A_DESCRIPTION)
            .withManifest(A_MANIFEST)
            .withSpec(A_SPEC),
        ),
      )

    Mockito
      .verify(manifestInjector, Mockito.never())
      .getCdkVersion(anyOrNull())

    Mockito
      .verify(connectorBuilderService, Mockito.times(0))
      .createDeclarativeManifestAsActiveVersion(
        anyOrNull(),
        anyOrNull(),
        anyOrNull(),
        anyOrNull(),
      )
  }

  @Test
  @Throws(IOException::class)
  fun whenCreateDeclarativeSourceDefinitionManifestThenManifestDraftDeleted() {
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()

    handler!!.createDeclarativeSourceDefinitionManifest(
      DeclarativeSourceDefinitionCreateManifestRequestBody()
        .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .workspaceId(A_WORKSPACE_ID)
        .setAsActiveManifest(false)
        .declarativeManifest(
          anyDeclarativeManifest()!!
            .manifest(A_MANIFEST)
            .spec(A_SPEC)
            .version(A_VERSION)
            .description(A_DESCRIPTION),
        ),
    )

    Mockito.verify(connectorBuilderService, Mockito.times(1)).deleteManifestDraftForActorDefinition(
      A_SOURCE_DEFINITION_ID,
      A_WORKSPACE_ID,
    )
  }

  @Test
  @Throws(IOException::class)
  fun givenSourceNotAvailableInWorkspaceWhenUpdateDeclarativeManifestVersionThenThrowException() {
    whenever(workspaceService.workspaceCanUseCustomDefinition(A_SOURCE_DEFINITION_ID, A_WORKSPACE_ID)).thenReturn(false)
    Assertions.assertThrows(DeclarativeSourceNotFoundException::class.java) {
      handler!!.updateDeclarativeManifestVersion(
        UpdateActiveManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION),
      )
    }
  }

  @Test
  @Throws(IOException::class)
  fun givenNoDeclarativeManifestForSourceDefinitionIdWhenUpdateDeclarativeManifestVersionThenThrowException() {
    givenSourceDefinitionAvailableInWorkspace()
    Mockito
      .`when`(connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID))
      .thenReturn(
        Stream.of(),
      )

    Assertions.assertThrows(SourceIsNotDeclarativeException::class.java) {
      handler!!.updateDeclarativeManifestVersion(
        UpdateActiveManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION),
      )
    }
  }

  @Test
  @DisplayName("updateDeclarativeManifest throws a helpful error if no associated CDK version is found")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testUpdateDeclarativeManifestVersionNoCdkVersion() {
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    Mockito
      .`when`(
        connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(
          A_SOURCE_DEFINITION_ID,
          A_VERSION,
        ),
      ).thenReturn(
        DeclarativeManifest()
          .withVersion(A_VERSION)
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withManifest(A_MANIFEST)
          .withSpec(A_SPEC),
      )
    Mockito
      .`when`(manifestInjector.createManifestConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST))
      .thenReturn(manifestConfigInjection)
    Mockito
      .`when`(manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC))
      .thenReturn(adaptedConnectorSpecification)
    whenever(manifestInjector.getCdkVersion(A_MANIFEST)).thenReturn(A_CDK_VERSION)
    Mockito
      .`when`(declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(0))
      .thenThrow(IllegalStateException("No declarative manifest image version found in database for major version 0"))

    Assertions.assertEquals(
      "No declarative manifest image version found in database for major version 0",
      Assertions
        .assertThrows(IllegalStateException::class.java) {
          handler!!.updateDeclarativeManifestVersion(
            UpdateActiveManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION),
          )
        }.message,
    )
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun givenNotFoundWhenUpdateDeclarativeManifestVersionThenThrowException() {
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    Mockito
      .`when`(connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(anyOrNull(), anyOrNull()))
      .thenThrow(ConfigNotFoundException::class.java)

    Assertions.assertThrows(ConfigNotFoundException::class.java) {
      handler!!.updateDeclarativeManifestVersion(
        UpdateActiveManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION),
      )
    }
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun whenUpdateDeclarativeManifestVersionThenSetDeclarativeSourceActiveVersion() {
    Mockito
      .`when`<ConnectorPlatformCompatibilityValidationResult?>(
        airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(
          eq(
            AN_IMAGE_VERSION,
          ),
        ),
      ).thenReturn(ConnectorPlatformCompatibilityValidationResult(true, ""))
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    Mockito
      .`when`<DeclarativeManifest?>(
        connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(
          A_SOURCE_DEFINITION_ID,
          A_VERSION,
        ),
      ).thenReturn(
        DeclarativeManifest()
          .withVersion(A_VERSION)
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withManifest(A_MANIFEST)
          .withSpec(A_SPEC),
      )
    Mockito
      .`when`(
        manifestInjector.getManifestConnectorInjections(
          A_SOURCE_DEFINITION_ID,
          A_MANIFEST,
          null,
        ),
      ).thenReturn(
        listOf(manifestConfigInjection),
      )
    Mockito
      .`when`(manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC))
      .thenReturn(adaptedConnectorSpecification)
    whenever(manifestInjector.getCdkVersion(A_MANIFEST)).thenReturn(A_CDK_VERSION)

    handler!!.updateDeclarativeManifestVersion(
      UpdateActiveManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION),
    )

    Mockito.verify(manifestInjector, Mockito.times(1)).getCdkVersion(A_MANIFEST)
    Mockito.verify(connectorBuilderService, Mockito.times(1)).setDeclarativeSourceActiveVersion(
      A_SOURCE_DEFINITION_ID,
      A_VERSION,
      listOf(manifestConfigInjection),
      adaptedConnectorSpecification,
      AN_IMAGE_VERSION,
    )
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun updateShouldNotWorkIfValidationFails() {
    Mockito
      .`when`<ConnectorPlatformCompatibilityValidationResult?>(
        airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(
          eq(
            AN_IMAGE_VERSION,
          ),
        ),
      ).thenReturn(ConnectorPlatformCompatibilityValidationResult(false, "Can't update definition"))
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    Mockito
      .`when`<DeclarativeManifest?>(
        connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(
          A_SOURCE_DEFINITION_ID,
          A_VERSION,
        ),
      ).thenReturn(
        DeclarativeManifest()
          .withVersion(A_VERSION)
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withManifest(A_MANIFEST)
          .withSpec(A_SPEC),
      )
    Mockito
      .`when`<ActorDefinitionConfigInjection?>(manifestInjector.createManifestConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST))
      .thenReturn(manifestConfigInjection)
    Mockito
      .`when`(manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC))
      .thenReturn(adaptedConnectorSpecification)
    whenever(manifestInjector.getCdkVersion(A_MANIFEST)).thenReturn(A_CDK_VERSION)

    Assertions.assertThrows<BadRequestProblem?>(BadRequestProblem::class.java) {
      handler!!.updateDeclarativeManifestVersion(
        UpdateActiveManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION),
      )
    }
    Mockito.verify(connectorBuilderService, Mockito.times(0)).setDeclarativeSourceActiveVersion(
      A_SOURCE_DEFINITION_ID,
      A_VERSION,
      listOf(manifestConfigInjection),
      adaptedConnectorSpecification,
      AN_IMAGE_VERSION,
    )
    Mockito.verify(manifestInjector, Mockito.times(1)).getCdkVersion(A_MANIFEST)
  }

  @Test
  @DisplayName("listManifestVersions should return a list of all manifest versions with their descriptions and status")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testListManifestVersions() {
    val sourceDefinitionId = UUID.randomUUID()
    givenSourceDefinitionAvailableInWorkspace()

    val manifest1 = DeclarativeManifest().withVersion(1L).withDescription("first version")
    val manifest2 = DeclarativeManifest().withVersion(2L).withDescription("second version")
    val manifest3 = DeclarativeManifest().withVersion(3L).withDescription("third version")

    Mockito
      .`when`(connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(sourceDefinitionId))
      .thenReturn(Stream.of(manifest1, manifest2, manifest3))
    Mockito
      .`when`(connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(sourceDefinitionId))
      .thenReturn(manifest2)

    val response =
      handler!!.listManifestVersions(ListDeclarativeManifestsRequestBody().workspaceId(A_WORKSPACE_ID).sourceDefinitionId(sourceDefinitionId))
    Assertions.assertEquals(3, response.getManifestVersions().size)

    Assertions.assertFalse(response.getManifestVersions().get(0).getIsActive())
    Assertions.assertTrue(response.getManifestVersions().get(1).getIsActive())
    Assertions.assertFalse(response.getManifestVersions().get(2).getIsActive())

    Assertions.assertEquals(manifest1.getDescription(), response.getManifestVersions().get(0).getDescription())
    Assertions.assertEquals(manifest2.getDescription(), response.getManifestVersions().get(1).getDescription())
    Assertions.assertEquals(manifest3.getDescription(), response.getManifestVersions().get(2).getDescription())

    Assertions.assertEquals(manifest1.getVersion(), response.getManifestVersions().get(0).getVersion())
    Assertions.assertEquals(manifest2.getVersion(), response.getManifestVersions().get(1).getVersion())
    Assertions.assertEquals(manifest3.getVersion(), response.getManifestVersions().get(2).getVersion())
  }

  @Throws(IOException::class)
  private fun givenSourceDefinitionAvailableInWorkspace() {
    whenever(workspaceService.workspaceCanUseCustomDefinition(anyOrNull(), anyOrNull()))
      .thenReturn(true)
  }

  @Throws(IOException::class)
  private fun givenSourceIsDeclarative() {
    Mockito
      .`when`(connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID))
      .thenReturn(Stream.of(DeclarativeManifest().withVersion(ANOTHER_VERSION)))
  }

  companion object {
    private val A_SOURCE_DEFINITION_ID: UUID = UUID.randomUUID()
    private val A_WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val A_VERSION = 32L
    private const val ANOTHER_VERSION = 99L
    private const val A_DESCRIPTION = "a description"
    private val A_CDK_VERSION = Version("0.0.1")
    private const val AN_IMAGE_VERSION = "0.79.0"
    private val A_DECLARATIVE_MANIFEST_IMAGE_VERSION =
      DeclarativeManifestImageVersion(
        0,
        AN_IMAGE_VERSION,
        "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c",
        OffsetDateTime.now(),
        OffsetDateTime.now(),
      )
    private val A_MANIFEST: JsonNode
    private val A_SPEC: JsonNode

    init {
      try {
        A_MANIFEST = ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}")
        A_SPEC = ObjectMapper().readTree("{\"a_spec\": \"spec_value\"}")
      } catch (e: JsonProcessingException) {
        throw RuntimeException(e)
      }
    }

    private fun anyDeclarativeManifest(): DeclarativeSourceManifest? = DeclarativeSourceManifest().version(A_VERSION)
  }
}
