/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.server.generated.models.ActorDefinitionVersionBreakingChanges
import io.airbyte.api.server.generated.models.DeadlineAction
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata.Companion.mock
import io.airbyte.commons.server.scheduler.SynchronousResponse
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient
import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.BreakingChanges
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.ConnectorReleasesSource
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.ReleaseStage
import io.airbyte.config.SupportLevel
import io.airbyte.config.VersionBreakingChange
import io.airbyte.config.helpers.ConnectorRegistryConverters.toActorDefinitionBreakingChanges
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult
import io.airbyte.config.persistence.ActorDefinitionVersionResolver
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.micronaut.http.uri.UriBuilder
import io.mockk.Called
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.URI
import java.time.LocalDate
import java.util.Optional
import java.util.UUID

internal class ActorDefinitionHandlerHelperTest {
  private lateinit var synchronousSchedulerClient: SynchronousSchedulerClient
  private lateinit var actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper
  private lateinit var actorDefinitionVersionResolver: ActorDefinitionVersionResolver
  private lateinit var remoteDefinitionsProvider: RemoteDefinitionsProvider
  private lateinit var actorDefinitionService: ActorDefinitionService
  private val apiPojoConverters = ApiPojoConverters(CatalogConverter(FieldGenerator(), mutableListOf()))

  @BeforeEach
  fun setUp() {
    val compatibleConnectorsValidator = mockk<AirbyteCompatibleConnectorsValidator>()
    every { compatibleConnectorsValidator.validate(any(), any()) } returns ConnectorPlatformCompatibilityValidationResult(true, null)
    synchronousSchedulerClient = mockk<SynchronousSchedulerClient>()
    val protocolVersionRange = AirbyteProtocolVersionRange(Version("0.0.0"), Version("0.3.0"))
    actorDefinitionVersionResolver = mockk<ActorDefinitionVersionResolver>()
    remoteDefinitionsProvider = mockk<RemoteDefinitionsProvider>()
    actorDefinitionService = mockk<ActorDefinitionService>()
    actorDefinitionHandlerHelper =
      ActorDefinitionHandlerHelper(
        synchronousSchedulerClient,
        protocolVersionRange,
        actorDefinitionVersionResolver,
        remoteDefinitionsProvider,
        actorDefinitionService,
        apiPojoConverters,
        compatibleConnectorsValidator,
      )
  }

  @Nested
  internal inner class TestDefaultDefinitionVersionFromCreate {
    @Test
    @DisplayName("The ActorDefinitionVersion created fromCreate should always be custom")
    fun testDefaultDefinitionVersionFromCreate() {
      every {
        synchronousSchedulerClient.createGetSpecJob(
          getDockerImageForTag(
            DOCKER_IMAGE_TAG,
          ),
          true,
          WORKSPACE_ID,
        )
      } returns
        SynchronousResponse(
          ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION),
          mock(ConfigType.GET_SPEC),
        )

      val expectedNewVersion =
        ActorDefinitionVersion()
          .withActorDefinitionId(null)
          .withDockerImageTag(DOCKER_IMAGE_TAG)
          .withDockerRepository(DOCKER_REPOSITORY)
          .withSpec(ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION))
          .withDocumentationUrl(DOCUMENTATION_URL.toString())
          .withSupportLevel(SupportLevel.NONE)
          .withInternalSupportLevel(100L)
          .withProtocolVersion(VALID_PROTOCOL_VERSION)
          .withReleaseStage(ReleaseStage.CUSTOM)

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
          DOCKER_REPOSITORY,
          DOCKER_IMAGE_TAG,
          DOCUMENTATION_URL,
          WORKSPACE_ID,
        )
      verify { synchronousSchedulerClient.createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID) }
      Assertions.assertEquals(expectedNewVersion, newVersion)

      confirmVerified(synchronousSchedulerClient)
      verify { actorDefinitionVersionResolver wasNot Called }
      verify { remoteDefinitionsProvider wasNot Called }
    }

    @Test
    @DisplayName("The ActorDefinitionVersion created fromCreate should read supportsFileTransfer from spec additionalProperties")
    fun testDefaultDefinitionVersionFromCreateWithSupportsFileTransfer() {
      val specWithFileTransfer = ConnectorSpecification()
        .withProtocolVersion(VALID_PROTOCOL_VERSION)
        .withAdditionalProperty("supportsFileTransfer", true)

      Mockito
        .`when`(
          synchronousSchedulerClient.createGetSpecJob(
            getDockerImageForTag(
              DOCKER_IMAGE_TAG,
            ),
            true,
            WORKSPACE_ID,
          ),
        ).thenReturn(
          SynchronousResponse(
            specWithFileTransfer,
            mock(ConfigType.GET_SPEC),
          ),
        )

      val expectedNewVersion =
        ActorDefinitionVersion()
          .withActorDefinitionId(null)
          .withDockerImageTag(DOCKER_IMAGE_TAG)
          .withDockerRepository(DOCKER_REPOSITORY)
          .withSpec(specWithFileTransfer)
          .withDocumentationUrl(DOCUMENTATION_URL.toString())
          .withSupportLevel(SupportLevel.NONE)
          .withInternalSupportLevel(100L)
          .withProtocolVersion(VALID_PROTOCOL_VERSION)
          .withReleaseStage(ReleaseStage.CUSTOM)
          .withSupportsFileTransfer(true)

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
          DOCKER_REPOSITORY,
          DOCKER_IMAGE_TAG,
          DOCUMENTATION_URL,
          WORKSPACE_ID,
        )
      Mockito
        .verify(synchronousSchedulerClient)
        .createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID)
      Assertions.assertEquals(expectedNewVersion, newVersion)

      Mockito.verifyNoMoreInteractions(synchronousSchedulerClient)
      Mockito.verifyNoInteractions(actorDefinitionVersionResolver, remoteDefinitionsProvider)
    }

    @Test
    @DisplayName("The ActorDefinitionVersion created fromCreate should default supportsFileTransfer to false when not present in spec")
    fun testDefaultDefinitionVersionFromCreateWithoutSupportsFileTransfer() {
      val specWithoutFileTransfer = ConnectorSpecification()
        .withProtocolVersion(VALID_PROTOCOL_VERSION)

      Mockito
        .`when`(
          synchronousSchedulerClient.createGetSpecJob(
            getDockerImageForTag(
              DOCKER_IMAGE_TAG,
            ),
            true,
            WORKSPACE_ID,
          ),
        ).thenReturn(
          SynchronousResponse(
            specWithoutFileTransfer,
            mock(ConfigType.GET_SPEC),
          ),
        )

      val expectedNewVersion =
        ActorDefinitionVersion()
          .withActorDefinitionId(null)
          .withDockerImageTag(DOCKER_IMAGE_TAG)
          .withDockerRepository(DOCKER_REPOSITORY)
          .withSpec(specWithoutFileTransfer)
          .withDocumentationUrl(DOCUMENTATION_URL.toString())
          .withSupportLevel(SupportLevel.NONE)
          .withInternalSupportLevel(100L)
          .withProtocolVersion(VALID_PROTOCOL_VERSION)
          .withReleaseStage(ReleaseStage.CUSTOM)
          .withSupportsFileTransfer(false)

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
          DOCKER_REPOSITORY,
          DOCKER_IMAGE_TAG,
          DOCUMENTATION_URL,
          WORKSPACE_ID,
        )
      Mockito
        .verify(synchronousSchedulerClient)
        .createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID)
      Assertions.assertEquals(expectedNewVersion, newVersion)

      Mockito.verifyNoMoreInteractions(synchronousSchedulerClient)
      Mockito.verifyNoInteractions(actorDefinitionVersionResolver, remoteDefinitionsProvider)
    }

    @Test
    @DisplayName("Creating an ActorDefinitionVersion from create with an invalid protocol version should throw an exception")
    fun testDefaultDefinitionVersionFromCreateInvalidProtocolVersionThrows() {
      every {
        synchronousSchedulerClient.createGetSpecJob(
          getDockerImageForTag(
            DOCKER_IMAGE_TAG,
          ),
          true,
          WORKSPACE_ID,
        )
      } returns
        SynchronousResponse(
          ConnectorSpecification().withProtocolVersion(INVALID_PROTOCOL_VERSION),
          mock(ConfigType.GET_SPEC),
        )

      Assertions.assertThrows(
        UnsupportedProtocolVersionException::class.java,
      ) {
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
          DOCKER_REPOSITORY,
          DOCKER_IMAGE_TAG,
          DOCUMENTATION_URL,
          WORKSPACE_ID,
        )
      }
      verify { synchronousSchedulerClient.createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID) }

      confirmVerified(synchronousSchedulerClient)
      verify { actorDefinitionVersionResolver wasNot Called }
      verify { remoteDefinitionsProvider wasNot Called }
    }
  }

  @Nested
  internal inner class TestDefaultDefinitionVersionFromUpdate {
    @BeforeEach
    fun setUp() {
      // default version resolver to not have the new version already
      every {
        actorDefinitionVersionResolver.resolveVersionForTag(
          actorDefinitionVersion.getActorDefinitionId(),
          ActorType.SOURCE,
          actorDefinitionVersion.getDockerRepository(),
          any(),
        )
      } returns Optional.empty()
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Creating an ActorDefinitionVersion from update with a new version gets a new spec and new protocol version")
    fun testDefaultDefinitionVersionFromUpdateNewVersion(isCustomConnector: Boolean) {
      val previousDefaultVersion: ActorDefinitionVersion = actorDefinitionVersion
      val newDockerImageTag = "newTag"
      val newValidProtocolVersion = "0.2.0"
      val newDockerImage = getDockerImageForTag(newDockerImageTag)
      val newSpec =
        ConnectorSpecification().withProtocolVersion(newValidProtocolVersion).withAdditionalProperty(SPEC_KEY, "new")

      every {
        synchronousSchedulerClient.createGetSpecJob(
          newDockerImage,
          isCustomConnector,
          WORKSPACE_ID,
        )
      } returns
        SynchronousResponse(
          newSpec,
          mock(ConfigType.GET_SPEC),
        )

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          previousDefaultVersion,
          ActorType.SOURCE,
          newDockerImageTag,
          isCustomConnector,
          WORKSPACE_ID,
        )

      verify {
        actorDefinitionVersionResolver.resolveVersionForTag(
          previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE,
          previousDefaultVersion.getDockerRepository(),
          newDockerImageTag,
        )
      }
      verify { synchronousSchedulerClient.createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID) }

      Assertions.assertNotEquals(previousDefaultVersion, newVersion)
      Assertions.assertEquals(newSpec, newVersion.getSpec())
      Assertions.assertEquals(newValidProtocolVersion, newVersion.getProtocolVersion())

      confirmVerified(synchronousSchedulerClient, actorDefinitionVersionResolver)
      verify { remoteDefinitionsProvider wasNot Called }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Creating an ActorDefinitionVersion from update should read supportsFileTransfer from new spec")
    fun testDefaultDefinitionVersionFromUpdateReadsSupportsFileTransfer(isCustomConnector: Boolean) {
      val previousDefaultVersion: ActorDefinitionVersion = actorDefinitionVersion.withSupportsFileTransfer(false)
      val newDockerImageTag = "newTag"
      val newValidProtocolVersion = "0.2.0"
      val newDockerImage = getDockerImageForTag(newDockerImageTag)
      val newSpec =
        ConnectorSpecification()
          .withProtocolVersion(newValidProtocolVersion)
          .withAdditionalProperty(SPEC_KEY, "new")
          .withAdditionalProperty("supportsFileTransfer", true)

      Mockito
        .`when`(
          synchronousSchedulerClient.createGetSpecJob(
            newDockerImage,
            isCustomConnector,
            WORKSPACE_ID,
          ),
        ).thenReturn(
          SynchronousResponse(
            newSpec,
            mock(ConfigType.GET_SPEC),
          ),
        )

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          previousDefaultVersion,
          ActorType.SOURCE,
          newDockerImageTag,
          isCustomConnector,
          WORKSPACE_ID,
        )

      Mockito.verify(actorDefinitionVersionResolver).resolveVersionForTag(
        previousDefaultVersion.getActorDefinitionId(),
        ActorType.SOURCE,
        previousDefaultVersion.getDockerRepository(),
        newDockerImageTag,
      )
      Mockito.verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID)

      Assertions.assertNotEquals(previousDefaultVersion, newVersion)
      Assertions.assertEquals(newSpec, newVersion.getSpec())
      Assertions.assertEquals(newValidProtocolVersion, newVersion.getProtocolVersion())
      Assertions.assertTrue(newVersion.getSupportsFileTransfer()) // Should be true from the new spec

      Mockito.verifyNoMoreInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver)
      Mockito.verifyNoInteractions(remoteDefinitionsProvider)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("If the 'new' version has the same dockerImageTag, we don't attempt to fetch a new spec")
    fun testDefaultDefinitionVersionFromUpdateSameVersion(isCustomConnector: Boolean) {
      val previousDefaultVersion: ActorDefinitionVersion = actorDefinitionVersion
      val newDockerImageTag = previousDefaultVersion.getDockerImageTag()

      every {
        actorDefinitionVersionResolver.resolveVersionForTag(
          previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE,
          previousDefaultVersion.getDockerRepository(),
          newDockerImageTag,
        )
      } returns Optional.of(previousDefaultVersion)

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          previousDefaultVersion,
          ActorType.SOURCE,
          newDockerImageTag,
          isCustomConnector,
          WORKSPACE_ID,
        )

      verify {
        actorDefinitionVersionResolver.resolveVersionForTag(
          previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE,
          previousDefaultVersion.getDockerRepository(),
          newDockerImageTag,
        )
      }

      Assertions.assertEquals(previousDefaultVersion, newVersion)

      confirmVerified(actorDefinitionVersionResolver)
      verify { synchronousSchedulerClient wasNot Called }
      verify { remoteDefinitionsProvider wasNot Called }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Always fetch specs for dev versions")
    fun testDefaultDefinitionVersionFromUpdateSameDevVersion(isCustomConnector: Boolean) {
      val previousDefaultVersion = clone(actorDefinitionVersion).withDockerImageTag(DEV)
      val newDockerImageTag: String = DEV
      val newDockerImage = getDockerImageForTag(newDockerImageTag)
      val newValidProtocolVersion = "0.2.0"

      val newSpec =
        ConnectorSpecification().withProtocolVersion(newValidProtocolVersion).withAdditionalProperty(SPEC_KEY, "new")
      every {
        synchronousSchedulerClient.createGetSpecJob(
          newDockerImage,
          isCustomConnector,
          WORKSPACE_ID,
        )
      } returns
        SynchronousResponse(
          newSpec,
          mock(ConfigType.GET_SPEC),
        )

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          previousDefaultVersion,
          ActorType.SOURCE,
          newDockerImageTag,
          isCustomConnector,
          WORKSPACE_ID,
        )
      verify {
        actorDefinitionVersionResolver.resolveVersionForTag(
          previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE,
          previousDefaultVersion.getDockerRepository(),
          newDockerImageTag,
        )
      }

      Assertions.assertEquals(previousDefaultVersion.getDockerImageTag(), newVersion.getDockerImageTag())
      Assertions.assertNotEquals(previousDefaultVersion, newVersion)
      verify { synchronousSchedulerClient.createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID) }

      confirmVerified(synchronousSchedulerClient, actorDefinitionVersionResolver)
      verify { remoteDefinitionsProvider wasNot Called }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Creating an ActorDefinitionVersion from update with an invalid protocol version should throw an exception")
    fun testDefaultDefinitionVersionFromUpdateInvalidProtocolVersion(isCustomConnector: Boolean) {
      val previousDefaultVersion: ActorDefinitionVersion = actorDefinitionVersion
      val newDockerImageTag = "newTag"
      val newDockerImage = getDockerImageForTag(newDockerImageTag)
      every {
        synchronousSchedulerClient.createGetSpecJob(
          newDockerImage,
          isCustomConnector,
          WORKSPACE_ID,
        )
      } returns
        SynchronousResponse(
          ConnectorSpecification().withProtocolVersion(INVALID_PROTOCOL_VERSION),
          mock(ConfigType.GET_SPEC),
        )

      Assertions.assertThrows(
        UnsupportedProtocolVersionException::class.java,
      ) {
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          previousDefaultVersion,
          ActorType.SOURCE,
          newDockerImageTag,
          isCustomConnector,
          WORKSPACE_ID,
        )
      }
      verify {
        actorDefinitionVersionResolver.resolveVersionForTag(
          previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE,
          previousDefaultVersion.getDockerRepository(),
          newDockerImageTag,
        )
      }
      verify { synchronousSchedulerClient.createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID) }

      confirmVerified(synchronousSchedulerClient, actorDefinitionVersionResolver)
      verify { remoteDefinitionsProvider wasNot Called }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Creating an ActorDefinitionVersion from update should return an already existing one from db/remote before creating a new one")
    fun testDefaultDefinitionVersionFromUpdateVersionResolved(isCustomConnector: Boolean) {
      val previousDefaultVersion: ActorDefinitionVersion = actorDefinitionVersion

      val newDockerImageTag = "newTagButPreviouslyUsed"
      val oldExistingADV =
        clone(actorDefinitionVersion)
          .withDockerImageTag(newDockerImageTag)
          .withSpec(ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION).withAdditionalProperty(SPEC_KEY, "existing"))

      every {
        actorDefinitionVersionResolver.resolveVersionForTag(
          ACTOR_DEFINITION_ID,
          ActorType.SOURCE,
          DOCKER_REPOSITORY,
          newDockerImageTag,
        )
      } returns Optional.of(oldExistingADV)

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          previousDefaultVersion,
          ActorType.SOURCE,
          newDockerImageTag,
          isCustomConnector,
          WORKSPACE_ID,
        )
      verify {
        actorDefinitionVersionResolver.resolveVersionForTag(
          previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE,
          previousDefaultVersion.getDockerRepository(),
          newDockerImageTag,
        )
      }

      Assertions.assertEquals(oldExistingADV, newVersion)

      confirmVerified(actorDefinitionVersionResolver)
      verify { synchronousSchedulerClient wasNot Called }
      verify { remoteDefinitionsProvider wasNot Called }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Re-fetch spec for dev versions resolved from the db")
    fun testUpdateVersionResolvedDevVersion(isCustomConnector: Boolean) {
      val previousDefaultVersion = clone(actorDefinitionVersion).withDockerImageTag(DEV)
      val dockerImage = getDockerImageForTag(DEV)

      val newSpec =
        ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION).withAdditionalProperty(SPEC_KEY, "new")
      every {
        synchronousSchedulerClient.createGetSpecJob(
          dockerImage,
          isCustomConnector,
          WORKSPACE_ID,
        )
      } returns
        SynchronousResponse(
          newSpec,
          mock(ConfigType.GET_SPEC),
        )

      val oldExistingADV =
        clone(actorDefinitionVersion)
          .withSpec(ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION).withAdditionalProperty(SPEC_KEY, "existing"))

      every {
        actorDefinitionVersionResolver.resolveVersionForTag(
          ACTOR_DEFINITION_ID,
          ActorType.SOURCE,
          DOCKER_REPOSITORY,
          DEV,
        )
      } returns Optional.of(oldExistingADV)

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          previousDefaultVersion,
          ActorType.SOURCE,
          DEV,
          isCustomConnector,
          WORKSPACE_ID,
        )
      verify {
        actorDefinitionVersionResolver.resolveVersionForTag(
          previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE,
          previousDefaultVersion.getDockerRepository(),
          DEV,
        )
      }
      verify { synchronousSchedulerClient.createGetSpecJob(dockerImage, isCustomConnector, WORKSPACE_ID) }

      Assertions.assertEquals(oldExistingADV.withSpec(newSpec), newVersion)

      confirmVerified(actorDefinitionVersionResolver, synchronousSchedulerClient)
      verify { remoteDefinitionsProvider wasNot Called }
    }
  }

  @Nested
  internal inner class TestGetBreakingChanges {
    @Test
    fun testGetBreakingChanges() {
      val sourceRegistryBreakingChanges =
        BreakingChanges().withAdditionalProperty(
          "1.0.0",
          VersionBreakingChange()
            .withMessage("A breaking change was made")
            .withUpgradeDeadline("2000-01-01")
            .withMigrationDocumentationUrl("https://docs.airbyte.io/migration"),
        )
      val sourceDefWithBreakingChanges =
        clone(connectorRegistrySourceDefinition)
          .withReleases(ConnectorReleasesSource().withBreakingChanges(sourceRegistryBreakingChanges))

      every {
        remoteDefinitionsProvider.getSourceDefinitionByVersion(
          DOCKER_REPOSITORY,
          LATEST,
        )
      } returns Optional.of(sourceDefWithBreakingChanges)

      val breakingChanges: List<ActorDefinitionBreakingChange> =
        actorDefinitionHandlerHelper.getBreakingChanges(
          actorDefinitionVersion,
          ActorType.SOURCE,
        )
      Assertions.assertEquals(toActorDefinitionBreakingChanges(sourceDefWithBreakingChanges), breakingChanges)

      verify { remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST) }

      confirmVerified(remoteDefinitionsProvider)
      verify { synchronousSchedulerClient wasNot Called }
      verify { actorDefinitionVersionResolver wasNot Called }
    }
  }

  @Nested
  internal inner class TestGetVersionBreakingChanges {
    @Test
    fun testGetVersionBreakingChanges() {
      val breakingChangeList =
        listOf(
          ActorDefinitionBreakingChange()
            .withActorDefinitionId(ACTOR_DEFINITION_ID)
            .withMigrationDocumentationUrl("https://docs.airbyte.io/2")
            .withVersion(Version("2.0.0"))
            .withUpgradeDeadline("2023-01-01")
            .withMessage("This is a breaking change")
            .withDeadlineAction("auto_upgrade"),
          ActorDefinitionBreakingChange()
            .withActorDefinitionId(ACTOR_DEFINITION_ID)
            .withMigrationDocumentationUrl("https://docs.airbyte.io/3")
            .withVersion(Version("3.0.0"))
            .withUpgradeDeadline("2023-05-01")
            .withMessage("This is another breaking change"),
        )

      every {
        actorDefinitionService.listBreakingChangesForActorDefinitionVersion(
          actorDefinitionVersion,
        )
      } returns breakingChangeList as MutableList<ActorDefinitionBreakingChange>

      val expected =
        ActorDefinitionVersionBreakingChanges(
          minUpgradeDeadline = LocalDate.parse("2023-01-01"),
          deadlineAction = DeadlineAction.AUTO_UPGRADE,
          upcomingBreakingChanges =
            listOf(
              io.airbyte.api.server.generated.models.ActorDefinitionBreakingChange(
                migrationDocumentationUrl = "https://docs.airbyte.io/2",
                version = "2.0.0",
                upgradeDeadline = LocalDate.parse(("2023-01-01")),
                message = "This is a breaking change",
                deadlineAction = DeadlineAction.AUTO_UPGRADE,
              ),
              io.airbyte.api.server.generated.models.ActorDefinitionBreakingChange(
                migrationDocumentationUrl = "https://docs.airbyte.io/3",
                version = "3.0.0",
                upgradeDeadline = LocalDate.parse("2023-05-01"),
                message = "This is another breaking change",
                deadlineAction = DeadlineAction.DISABLE,
              ),
            ),
        )

      val actual: Optional<ActorDefinitionVersionBreakingChanges> =
        actorDefinitionHandlerHelper.getVersionBreakingChanges(
          actorDefinitionVersion,
        )
      Assertions.assertEquals(expected, actual.orElseThrow())
    }

    @Test
    fun testGetVersionBreakingChangesNoBreakingChanges() {
      every {
        actorDefinitionService.listBreakingChangesForActorDefinitionVersion(
          actorDefinitionVersion,
        )
      } returns mutableListOf()

      val actual: Optional<ActorDefinitionVersionBreakingChanges> =
        actorDefinitionHandlerHelper.getVersionBreakingChanges(
          actorDefinitionVersion,
        )
      Assertions.assertEquals(Optional.empty<ActorDefinitionVersionBreakingChanges>(), actual)
    }
  }

  @Test
  fun testGetNoBreakingChangesAvailable() {
    every {
      remoteDefinitionsProvider.getSourceDefinitionByVersion(
        DOCKER_REPOSITORY,
        LATEST,
      )
    } returns Optional.of(connectorRegistrySourceDefinition)

    val breakingChanges: List<ActorDefinitionBreakingChange> =
      actorDefinitionHandlerHelper.getBreakingChanges(
        actorDefinitionVersion,
        ActorType.SOURCE,
      )
    Assertions.assertEquals(toActorDefinitionBreakingChanges(connectorRegistrySourceDefinition), breakingChanges)

    verify { remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST) }

    confirmVerified(remoteDefinitionsProvider)
    verify { synchronousSchedulerClient wasNot Called }
    verify { actorDefinitionVersionResolver wasNot Called }
  }

  @Test
  fun testGetBreakingChangesIfDefinitionNotFound() {
    every {
      remoteDefinitionsProvider.getSourceDefinitionByVersion(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
      )
    } returns Optional.empty()
    every { remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST) } returns Optional.empty()

    val breakingChanges: List<ActorDefinitionBreakingChange?> =
      actorDefinitionHandlerHelper.getBreakingChanges(
        actorDefinitionVersion,
        ActorType.SOURCE,
      )
    verify { remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST) }
    Assertions.assertEquals(mutableListOf<ActorDefinitionBreakingChange>(), breakingChanges)

    confirmVerified(remoteDefinitionsProvider)
    verify { synchronousSchedulerClient wasNot Called }
    verify { actorDefinitionVersionResolver wasNot Called }
  }

  private fun getDockerImageForTag(dockerImageTag: String?): String = String.format("%s:%s", DOCKER_REPOSITORY, dockerImageTag)

  companion object {
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private const val DOCKER_REPOSITORY = "source-test"
    private const val DOCKER_IMAGE_TAG = "0.1.0"
    private const val VALID_PROTOCOL_VERSION = "0.1.0"
    private const val INVALID_PROTOCOL_VERSION = "123.0.0"
    private val DOCUMENTATION_URL: URI =
      UriBuilder
        .of("")
        .scheme("https")
        .host("docs.com")
        .build()
    private const val LATEST = "latest"
    private const val DEV = "dev"
    private const val SPEC_KEY = "Something"

    private val connectorRegistrySourceDefinition: ConnectorRegistrySourceDefinition =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDocumentationUrl(DOCUMENTATION_URL.toString())
        .withSpec(ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION))
        .withProtocolVersion(VALID_PROTOCOL_VERSION)

    private val actorDefinitionVersion: ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDocumentationUrl(DOCUMENTATION_URL.toString())
        .withSpec(ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION))
        .withProtocolVersion(VALID_PROTOCOL_VERSION)
  }
}
