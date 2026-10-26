/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.DeclarativeSourceDefinitionCreateManifestRequestBody
import io.airbyte.api.server.generated.models.DeclarativeSourceManifest
import io.airbyte.api.server.generated.models.ListDeclarativeManifestsRequestBody
import io.airbyte.api.server.generated.models.UpdateActiveManifestRequestBody
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
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
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
  fun setUp() {
    declarativeManifestImageVersionService = mockk<DeclarativeManifestImageVersionService>()
    connectorBuilderService = mockk<ConnectorBuilderService>()
    workspaceService = mockk<WorkspaceService>()
    manifestInjector = mockk<DeclarativeSourceManifestInjector>()
    adaptedConnectorSpecification = mockk<ConnectorSpecification>()
    manifestConfigInjection = mockk<ActorDefinitionConfigInjection>()
    airbyteCompatibleConnectorsValidator = mockk<AirbyteCompatibleConnectorsValidator>()

    every { manifestInjector.addInjectedDeclarativeManifest(any()) } just Runs
    every { connectorBuilderService.insertDeclarativeManifest(any()) } just Runs
    every { connectorBuilderService.deleteManifestDraftForActorDefinition(any(), any()) } just Runs
    every { connectorBuilderService.createDeclarativeManifestAsActiveVersion(any(), any(), any(), any(), any()) } just Runs
    every { connectorBuilderService.setDeclarativeSourceActiveVersion(any(), any(), any(), any(), any(), any()) } just Runs

    handler =
      DeclarativeSourceDefinitionsHandler(
        declarativeManifestImageVersionService,
        connectorBuilderService,
        workspaceService,
        manifestInjector,
        airbyteCompatibleConnectorsValidator,
      )
    every {
      declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(
        any(),
      )
    } returns A_DECLARATIVE_MANIFEST_IMAGE_VERSION
  }

  @Test
  fun givenSourceNotAvailableInWorkspaceWhenCreateDeclarativeSourceDefinitionManifestThenThrowException() {
    every { workspaceService.workspaceCanUseCustomDefinition(A_SOURCE_DEFINITION_ID, A_WORKSPACE_ID) } returns false
    Assertions.assertThrows(DeclarativeSourceNotFoundException::class.java) {
      handler!!.createDeclarativeSourceDefinitionManifest(
        DeclarativeSourceDefinitionCreateManifestRequestBody(
          sourceDefinitionId = A_SOURCE_DEFINITION_ID,
          workspaceId = A_WORKSPACE_ID,
          setAsActiveManifest = false,
          declarativeManifest = anyDeclarativeManifest(),
        ),
      )
    }
  }

  @Test
  fun givenNoDeclarativeManifestForSourceDefinitionIdWhenCreateDeclarativeSourceDefinitionManifestThenThrowException() {
    givenSourceDefinitionAvailableInWorkspace()
    every { connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID) } returns Stream.of()

    Assertions.assertThrows(
      SourceIsNotDeclarativeException::class.java,
    ) {
      handler!!.createDeclarativeSourceDefinitionManifest(
        DeclarativeSourceDefinitionCreateManifestRequestBody(
          workspaceId = A_WORKSPACE_ID,
          sourceDefinitionId = A_SOURCE_DEFINITION_ID,
          setAsActiveManifest = false,
          declarativeManifest = anyDeclarativeManifest(),
        ),
      )
    }
  }

  @Test
  fun givenVersionAlreadyExistsWhenCreateDeclarativeSourceDefinitionManifestThenThrowException() {
    givenSourceDefinitionAvailableInWorkspace()
    every { connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID) } returns
      listOf(DeclarativeManifest().withVersion(A_VERSION)).stream()

    Assertions.assertThrows(
      ValueConflictKnownException::class.java,
    ) {
      handler!!.createDeclarativeSourceDefinitionManifest(
        DeclarativeSourceDefinitionCreateManifestRequestBody(
          workspaceId = A_WORKSPACE_ID,
          sourceDefinitionId = A_SOURCE_DEFINITION_ID,
          setAsActiveManifest = false,
          declarativeManifest = anyDeclarativeManifest(),
        ),
      )
    }
  }

  @Test
  fun givenSetAsActiveWhenCreateDeclarativeSourceDefinitionManifestThenCreateDeclarativeManifest() {
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    every { manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC) } returns adaptedConnectorSpecification
    every {
      manifestInjector.getManifestConnectorInjections(
        A_SOURCE_DEFINITION_ID,
        A_MANIFEST,
        null,
      )
    } returns listOf(manifestConfigInjection)
    every { manifestInjector.getCdkVersion(A_MANIFEST) } returns A_CDK_VERSION

    handler!!.createDeclarativeSourceDefinitionManifest(
      DeclarativeSourceDefinitionCreateManifestRequestBody(
        workspaceId = A_WORKSPACE_ID,
        sourceDefinitionId = A_SOURCE_DEFINITION_ID,
        setAsActiveManifest = true,
        declarativeManifest = anyDeclarativeManifest(),
      ),
    )

    verify(exactly = 1) { manifestInjector.addInjectedDeclarativeManifest(A_SPEC) }
    verify(exactly = 1) { manifestInjector.getCdkVersion(A_MANIFEST) }
    verify(exactly = 1) {
      connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        DeclarativeManifest()
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withVersion(A_VERSION)
          .withDescription(A_DESCRIPTION)
          .withManifest(A_MANIFEST)
          .withSpec(A_SPEC),
        listOf(manifestConfigInjection),
        adaptedConnectorSpecification,
        AN_IMAGE_VERSION,
        false,
      )
    }
  }

  @Test
  fun givenNotSetAsActiveWhenCreateDeclarativeSourceDefinitionManifestThenCreateDeclarativeManifest() {
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()

    handler!!.createDeclarativeSourceDefinitionManifest(
      DeclarativeSourceDefinitionCreateManifestRequestBody(
        sourceDefinitionId = A_SOURCE_DEFINITION_ID,
        workspaceId = A_WORKSPACE_ID,
        setAsActiveManifest = false,
        declarativeManifest = anyDeclarativeManifest(),
      ),
    )

    verify(exactly = 1) {
      connectorBuilderService.insertDeclarativeManifest(
        DeclarativeManifest()
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withVersion(A_VERSION)
          .withDescription(A_DESCRIPTION)
          .withManifest(A_MANIFEST)
          .withSpec(A_SPEC),
      )
    }

    verify(exactly = 0) { manifestInjector.getCdkVersion(any()) }

    verify(exactly = 0) {
      connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    }
  }

  @Test
  fun whenCreateDeclarativeSourceDefinitionManifestThenManifestDraftDeleted() {
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()

    handler!!.createDeclarativeSourceDefinitionManifest(
      DeclarativeSourceDefinitionCreateManifestRequestBody(
        sourceDefinitionId = A_SOURCE_DEFINITION_ID,
        workspaceId = A_WORKSPACE_ID,
        setAsActiveManifest = false,
        declarativeManifest = anyDeclarativeManifest(),
      ),
    )

    verify(exactly = 1) {
      connectorBuilderService.deleteManifestDraftForActorDefinition(
        A_SOURCE_DEFINITION_ID,
        A_WORKSPACE_ID,
      )
    }
  }

  @Test
  fun givenSourceNotAvailableInWorkspaceWhenUpdateDeclarativeManifestVersionThenThrowException() {
    every { workspaceService.workspaceCanUseCustomDefinition(A_SOURCE_DEFINITION_ID, A_WORKSPACE_ID) } returns false
    Assertions.assertThrows(DeclarativeSourceNotFoundException::class.java) {
      handler!!.updateDeclarativeManifestVersion(
        UpdateActiveManifestRequestBody(
          sourceDefinitionId = A_SOURCE_DEFINITION_ID,
          workspaceId = A_WORKSPACE_ID,
          version = A_VERSION,
        ),
      )
    }
  }

  @Test
  fun givenNoDeclarativeManifestForSourceDefinitionIdWhenUpdateDeclarativeManifestVersionThenThrowException() {
    givenSourceDefinitionAvailableInWorkspace()
    every { connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID) } returns Stream.of()

    Assertions.assertThrows(SourceIsNotDeclarativeException::class.java) {
      handler!!.updateDeclarativeManifestVersion(
        UpdateActiveManifestRequestBody(
          sourceDefinitionId = A_SOURCE_DEFINITION_ID,
          workspaceId = A_WORKSPACE_ID,
          version = A_VERSION,
        ),
      )
    }
  }

  @Test
  @DisplayName("updateDeclarativeManifest throws a helpful error if no associated CDK version is found")
  fun testUpdateDeclarativeManifestVersionNoCdkVersion() {
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    every {
      connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(
        A_SOURCE_DEFINITION_ID,
        A_VERSION,
      )
    } returns
      DeclarativeManifest()
        .withVersion(A_VERSION)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)
    every { manifestInjector.createManifestConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST) } returns manifestConfigInjection
    every { manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC) } returns adaptedConnectorSpecification
    every { manifestInjector.getCdkVersion(A_MANIFEST) } returns A_CDK_VERSION
    every { declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(0) } throws
      IllegalStateException("No declarative manifest image version found in database for major version 0")

    Assertions.assertEquals(
      "No declarative manifest image version found in database for major version 0",
      Assertions
        .assertThrows(IllegalStateException::class.java) {
          handler!!.updateDeclarativeManifestVersion(
            UpdateActiveManifestRequestBody(
              sourceDefinitionId = A_SOURCE_DEFINITION_ID,
              workspaceId = A_WORKSPACE_ID,
              version = A_VERSION,
            ),
          )
        }.message,
    )
  }

  @Test
  fun givenNotFoundWhenUpdateDeclarativeManifestVersionThenThrowException() {
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    every { connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(any(), any()) } throws
      ConfigNotFoundException("test", "test")

    Assertions.assertThrows(ConfigNotFoundException::class.java) {
      handler!!.updateDeclarativeManifestVersion(
        UpdateActiveManifestRequestBody(
          sourceDefinitionId = A_SOURCE_DEFINITION_ID,
          workspaceId = A_WORKSPACE_ID,
          version = A_VERSION,
        ),
      )
    }
  }

  @Test
  fun whenUpdateDeclarativeManifestVersionThenSetDeclarativeSourceActiveVersion() {
    every {
      airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(
        AN_IMAGE_VERSION,
      )
    } returns ConnectorPlatformCompatibilityValidationResult(true, "")
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    every {
      connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(
        A_SOURCE_DEFINITION_ID,
        A_VERSION,
      )
    } returns
      DeclarativeManifest()
        .withVersion(A_VERSION)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)
    every {
      manifestInjector.getManifestConnectorInjections(
        A_SOURCE_DEFINITION_ID,
        A_MANIFEST,
        null,
      )
    } returns listOf(manifestConfigInjection)
    every { manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC) } returns adaptedConnectorSpecification
    every { manifestInjector.getCdkVersion(A_MANIFEST) } returns A_CDK_VERSION

    handler!!.updateDeclarativeManifestVersion(
      UpdateActiveManifestRequestBody(
        sourceDefinitionId = A_SOURCE_DEFINITION_ID,
        workspaceId = A_WORKSPACE_ID,
        version = A_VERSION,
      ),
    )

    verify(exactly = 1) { manifestInjector.getCdkVersion(A_MANIFEST) }
    verify(exactly = 1) {
      connectorBuilderService.setDeclarativeSourceActiveVersion(
        A_SOURCE_DEFINITION_ID,
        A_VERSION,
        listOf(manifestConfigInjection),
        adaptedConnectorSpecification,
        AN_IMAGE_VERSION,
        false,
      )
    }
  }

  @Test
  fun updateShouldNotWorkIfValidationFails() {
    every {
      airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(
        AN_IMAGE_VERSION,
      )
    } returns ConnectorPlatformCompatibilityValidationResult(false, "Can't update definition")
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    every {
      connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(
        A_SOURCE_DEFINITION_ID,
        A_VERSION,
      )
    } returns
      DeclarativeManifest()
        .withVersion(A_VERSION)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)
    every { manifestInjector.createManifestConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST) } returns manifestConfigInjection
    every { manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC) } returns adaptedConnectorSpecification
    every { manifestInjector.getCdkVersion(A_MANIFEST) } returns A_CDK_VERSION

    Assertions.assertThrows(BadRequestProblem::class.java) {
      handler!!.updateDeclarativeManifestVersion(
        UpdateActiveManifestRequestBody(
          sourceDefinitionId = A_SOURCE_DEFINITION_ID,
          workspaceId = A_WORKSPACE_ID,
          version = A_VERSION,
        ),
      )
    }
    verify(exactly = 0) {
      connectorBuilderService.setDeclarativeSourceActiveVersion(
        A_SOURCE_DEFINITION_ID,
        A_VERSION,
        listOf(manifestConfigInjection),
        adaptedConnectorSpecification,
        AN_IMAGE_VERSION,
        false,
      )
    }
    verify(exactly = 1) { manifestInjector.getCdkVersion(A_MANIFEST) }
  }

  @Test
  @DisplayName("listManifestVersions should return a list of all manifest versions with their descriptions and status")
  fun testListManifestVersions() {
    val sourceDefinitionId = UUID.randomUUID()
    givenSourceDefinitionAvailableInWorkspace()

    val manifest1 = DeclarativeManifest().withVersion(1L).withDescription("first version")
    val manifest2 = DeclarativeManifest().withVersion(2L).withDescription("second version")
    val manifest3 = DeclarativeManifest().withVersion(3L).withDescription("third version")

    every { connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(sourceDefinitionId) } returns
      listOf(manifest1, manifest2, manifest3).stream()
    every { connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(sourceDefinitionId) } returns manifest2

    val response =
      handler!!.listManifestVersions(
        ListDeclarativeManifestsRequestBody(
          workspaceId = A_WORKSPACE_ID,
          sourceDefinitionId = sourceDefinitionId,
        ),
      )
    Assertions.assertEquals(3, response.manifestVersions.size)

    Assertions.assertFalse(response.manifestVersions[0].isActive)
    Assertions.assertTrue(response.manifestVersions[1].isActive)
    Assertions.assertFalse(response.manifestVersions[2].isActive)

    Assertions.assertEquals(manifest1.description, response.manifestVersions[0].description)
    Assertions.assertEquals(manifest2.description, response.manifestVersions[1].description)
    Assertions.assertEquals(manifest3.description, response.manifestVersions[2].description)

    Assertions.assertEquals(manifest1.version, response.manifestVersions[0].version)
    Assertions.assertEquals(manifest2.version, response.manifestVersions[1].version)
    Assertions.assertEquals(manifest3.version, response.manifestVersions[2].version)
  }

  private fun givenSourceDefinitionAvailableInWorkspace() {
    every { workspaceService.workspaceCanUseCustomDefinition(any(), any()) } returns true
  }

  @Test
  fun givenManifestWithFileUploaderWhenCreateAsActiveThenSupportsFileTransferIsTrue() {
    val fileUploaderManifest =
      ObjectMapper().readTree(
        """{"streams": [{"type": "DeclarativeStream", "name": "file_stream", "file_uploader": {"type": "FileUploader"}}]}""",
      )
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    every { manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC) } returns adaptedConnectorSpecification
    every {
      manifestInjector.getManifestConnectorInjections(
        A_SOURCE_DEFINITION_ID,
        fileUploaderManifest,
        null,
      )
    } returns listOf(manifestConfigInjection)
    every { manifestInjector.getCdkVersion(fileUploaderManifest) } returns A_CDK_VERSION

    handler!!.createDeclarativeSourceDefinitionManifest(
      DeclarativeSourceDefinitionCreateManifestRequestBody(
        workspaceId = A_WORKSPACE_ID,
        sourceDefinitionId = A_SOURCE_DEFINITION_ID,
        setAsActiveManifest = true,
        declarativeManifest = anyDeclarativeManifest().copy(manifest = fileUploaderManifest),
      ),
    )

    verify(exactly = 1) {
      connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        DeclarativeManifest()
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withVersion(A_VERSION)
          .withDescription(A_DESCRIPTION)
          .withManifest(fileUploaderManifest)
          .withSpec(A_SPEC),
        listOf(manifestConfigInjection),
        adaptedConnectorSpecification,
        AN_IMAGE_VERSION,
        true,
      )
    }
  }

  @Test
  fun givenManifestWithFileUploaderWhenUpdateVersionThenSupportsFileTransferIsTrue() {
    val fileUploaderManifest =
      ObjectMapper().readTree(
        """{"streams": [{"type": "DeclarativeStream", "name": "file_stream", "file_uploader": {"type": "FileUploader"}}]}""",
      )
    every { airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(AN_IMAGE_VERSION) } returns
      ConnectorPlatformCompatibilityValidationResult(true, "")
    givenSourceDefinitionAvailableInWorkspace()
    givenSourceIsDeclarative()
    every { connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(A_SOURCE_DEFINITION_ID, A_VERSION) } returns
      DeclarativeManifest()
        .withVersion(A_VERSION)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withManifest(fileUploaderManifest)
        .withSpec(A_SPEC)
    every { manifestInjector.getManifestConnectorInjections(A_SOURCE_DEFINITION_ID, fileUploaderManifest, null) } returns
      listOf(manifestConfigInjection)
    every { manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC) } returns adaptedConnectorSpecification
    every { manifestInjector.getCdkVersion(fileUploaderManifest) } returns A_CDK_VERSION

    handler!!.updateDeclarativeManifestVersion(
      UpdateActiveManifestRequestBody(
        sourceDefinitionId = A_SOURCE_DEFINITION_ID,
        workspaceId = A_WORKSPACE_ID,
        version = A_VERSION,
      ),
    )

    verify(exactly = 1) {
      connectorBuilderService.setDeclarativeSourceActiveVersion(
        A_SOURCE_DEFINITION_ID,
        A_VERSION,
        listOf(manifestConfigInjection),
        adaptedConnectorSpecification,
        AN_IMAGE_VERSION,
        true,
      )
    }
  }

  private fun givenSourceIsDeclarative() {
    every { connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID) } returns
      listOf(DeclarativeManifest().withVersion(ANOTHER_VERSION)).stream()
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

    private fun anyDeclarativeManifest(): DeclarativeSourceManifest =
      DeclarativeSourceManifest(description = A_DESCRIPTION, manifest = A_MANIFEST, spec = A_SPEC, version = A_VERSION)
  }
}
