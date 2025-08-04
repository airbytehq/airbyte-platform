/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges
import io.airbyte.api.model.generated.DeadlineAction
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
import io.airbyte.config.persistence.ActorDefinitionVersionResolver
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.micronaut.http.uri.UriBuilder
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import java.io.IOException
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
    synchronousSchedulerClient = Mockito.mock(SynchronousSchedulerClient::class.java)
    val protocolVersionRange = AirbyteProtocolVersionRange(Version("0.0.0"), Version("0.3.0"))
    actorDefinitionVersionResolver = Mockito.mock(ActorDefinitionVersionResolver::class.java)
    remoteDefinitionsProvider = Mockito.mock(RemoteDefinitionsProvider::class.java)
    actorDefinitionService = Mockito.mock(ActorDefinitionService::class.java)
    actorDefinitionHandlerHelper =
      ActorDefinitionHandlerHelper(
        synchronousSchedulerClient,
        protocolVersionRange,
        actorDefinitionVersionResolver,
        remoteDefinitionsProvider,
        actorDefinitionService,
        apiPojoConverters,
      )
  }

  @Nested
  internal inner class TestDefaultDefinitionVersionFromCreate {
    @Test
    @DisplayName("The ActorDefinitionVersion created fromCreate should always be custom")
    @Throws(IOException::class)
    fun testDefaultDefinitionVersionFromCreate() {
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
            ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION),
            mock(ConfigType.GET_SPEC),
          ),
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
      Mockito
        .verify(synchronousSchedulerClient)
        .createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID)
      Assertions.assertEquals(expectedNewVersion, newVersion)

      Mockito.verifyNoMoreInteractions(synchronousSchedulerClient)
      Mockito.verifyNoInteractions(actorDefinitionVersionResolver, remoteDefinitionsProvider)
    }

    @Test
    @DisplayName("Creating an ActorDefinitionVersion from create with an invalid protocol version should throw an exception")
    @Throws(
      IOException::class,
    )
    fun testDefaultDefinitionVersionFromCreateInvalidProtocolVersionThrows() {
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
            ConnectorSpecification().withProtocolVersion(INVALID_PROTOCOL_VERSION),
            mock(ConfigType.GET_SPEC),
          ),
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
      Mockito
        .verify(synchronousSchedulerClient)
        .createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID)

      Mockito.verifyNoMoreInteractions(synchronousSchedulerClient)
      Mockito.verifyNoInteractions(actorDefinitionVersionResolver, remoteDefinitionsProvider)
    }
  }

  @Nested
  internal inner class TestDefaultDefinitionVersionFromUpdate {
    @BeforeEach
    @Throws(IOException::class)
    fun setUp() {
      // default version resolver to not have the new version already
      Mockito
        .`when`(
          actorDefinitionVersionResolver.resolveVersionForTag(
            eq(actorDefinitionVersion.getActorDefinitionId()),
            eq(ActorType.SOURCE),
            eq(actorDefinitionVersion.getDockerRepository()),
            anyOrNull(),
          ),
        ).thenReturn(Optional.empty())
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Creating an ActorDefinitionVersion from update with a new version gets a new spec and new protocol version")
    @Throws(
      IOException::class,
    )
    fun testDefaultDefinitionVersionFromUpdateNewVersion(isCustomConnector: Boolean) {
      val previousDefaultVersion: ActorDefinitionVersion = actorDefinitionVersion
      val newDockerImageTag = "newTag"
      val newValidProtocolVersion = "0.2.0"
      val newDockerImage = getDockerImageForTag(newDockerImageTag)
      val newSpec =
        ConnectorSpecification().withProtocolVersion(newValidProtocolVersion).withAdditionalProperty(SPEC_KEY, "new")

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

      Mockito.verifyNoMoreInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver)
      Mockito.verifyNoInteractions(remoteDefinitionsProvider)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("If the 'new' version has the same dockerImageTag, we don't attempt to fetch a new spec")
    @Throws(
      IOException::class,
    )
    fun testDefaultDefinitionVersionFromUpdateSameVersion(isCustomConnector: Boolean) {
      val previousDefaultVersion: ActorDefinitionVersion = actorDefinitionVersion
      val newDockerImageTag = previousDefaultVersion.getDockerImageTag()

      Mockito
        .`when`(
          actorDefinitionVersionResolver.resolveVersionForTag(
            previousDefaultVersion.getActorDefinitionId(),
            ActorType.SOURCE,
            previousDefaultVersion.getDockerRepository(),
            newDockerImageTag,
          ),
        ).thenReturn(Optional.of(previousDefaultVersion))

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

      Assertions.assertEquals(previousDefaultVersion, newVersion)

      Mockito.verifyNoMoreInteractions(actorDefinitionVersionResolver)
      Mockito.verifyNoInteractions(synchronousSchedulerClient, remoteDefinitionsProvider)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Always fetch specs for dev versions")
    @Throws(IOException::class)
    fun testDefaultDefinitionVersionFromUpdateSameDevVersion(isCustomConnector: Boolean) {
      val previousDefaultVersion = clone(actorDefinitionVersion).withDockerImageTag(DEV)
      val newDockerImageTag: String = DEV
      val newDockerImage = getDockerImageForTag(newDockerImageTag)
      val newValidProtocolVersion = "0.2.0"

      val newSpec =
        ConnectorSpecification().withProtocolVersion(newValidProtocolVersion).withAdditionalProperty(SPEC_KEY, "new")
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

      Assertions.assertEquals(previousDefaultVersion.getDockerImageTag(), newVersion.getDockerImageTag())
      Assertions.assertNotEquals(previousDefaultVersion, newVersion)
      Mockito.verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID)

      Mockito.verifyNoMoreInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver)
      Mockito.verifyNoInteractions(remoteDefinitionsProvider)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Creating an ActorDefinitionVersion from update with an invalid protocol version should throw an exception")
    @Throws(
      IOException::class,
    )
    fun testDefaultDefinitionVersionFromUpdateInvalidProtocolVersion(isCustomConnector: Boolean) {
      val previousDefaultVersion: ActorDefinitionVersion = actorDefinitionVersion
      val newDockerImageTag = "newTag"
      val newDockerImage = getDockerImageForTag(newDockerImageTag)
      Mockito
        .`when`(
          synchronousSchedulerClient.createGetSpecJob(
            newDockerImage,
            isCustomConnector,
            WORKSPACE_ID,
          ),
        ).thenReturn(
          SynchronousResponse(
            ConnectorSpecification().withProtocolVersion(INVALID_PROTOCOL_VERSION),
            mock(ConfigType.GET_SPEC),
          ),
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
      Mockito.verify(actorDefinitionVersionResolver).resolveVersionForTag(
        previousDefaultVersion.getActorDefinitionId(),
        ActorType.SOURCE,
        previousDefaultVersion.getDockerRepository(),
        newDockerImageTag,
      )
      Mockito.verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID)

      Mockito.verifyNoMoreInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver)
      Mockito.verifyNoInteractions(remoteDefinitionsProvider)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Creating an ActorDefinitionVersion from update should return an already existing one from db/remote before creating a new one")
    @Throws(
      IOException::class,
    )
    fun testDefaultDefinitionVersionFromUpdateVersionResolved(isCustomConnector: Boolean) {
      val previousDefaultVersion: ActorDefinitionVersion = actorDefinitionVersion

      val newDockerImageTag = "newTagButPreviouslyUsed"
      val oldExistingADV =
        clone(actorDefinitionVersion)
          .withDockerImageTag(newDockerImageTag)
          .withSpec(ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION).withAdditionalProperty(SPEC_KEY, "existing"))

      Mockito
        .`when`(
          actorDefinitionVersionResolver.resolveVersionForTag(
            ACTOR_DEFINITION_ID,
            ActorType.SOURCE,
            DOCKER_REPOSITORY,
            newDockerImageTag,
          ),
        ).thenReturn(Optional.of(oldExistingADV))

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

      Assertions.assertEquals(oldExistingADV, newVersion)

      Mockito.verifyNoMoreInteractions(actorDefinitionVersionResolver)
      Mockito.verifyNoInteractions(synchronousSchedulerClient, remoteDefinitionsProvider)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Re-fetch spec for dev versions resolved from the db")
    @Throws(
      IOException::class,
    )
    fun testUpdateVersionResolvedDevVersion(isCustomConnector: Boolean) {
      val previousDefaultVersion = clone(actorDefinitionVersion).withDockerImageTag(DEV)
      val dockerImage = getDockerImageForTag(DEV)

      val newSpec =
        ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION).withAdditionalProperty(SPEC_KEY, "new")
      Mockito
        .`when`(
          synchronousSchedulerClient.createGetSpecJob(
            dockerImage,
            isCustomConnector,
            WORKSPACE_ID,
          ),
        ).thenReturn(
          SynchronousResponse(
            newSpec,
            mock(ConfigType.GET_SPEC),
          ),
        )

      val oldExistingADV =
        clone(actorDefinitionVersion)
          .withSpec(ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION).withAdditionalProperty(SPEC_KEY, "existing"))

      Mockito
        .`when`(
          actorDefinitionVersionResolver.resolveVersionForTag(
            ACTOR_DEFINITION_ID,
            ActorType.SOURCE,
            DOCKER_REPOSITORY,
            DEV,
          ),
        ).thenReturn(Optional.of(oldExistingADV))

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          previousDefaultVersion,
          ActorType.SOURCE,
          DEV,
          isCustomConnector,
          WORKSPACE_ID,
        )
      Mockito.verify(actorDefinitionVersionResolver).resolveVersionForTag(
        previousDefaultVersion.getActorDefinitionId(),
        ActorType.SOURCE,
        previousDefaultVersion.getDockerRepository(),
        DEV,
      )
      Mockito.verify(synchronousSchedulerClient).createGetSpecJob(dockerImage, isCustomConnector, WORKSPACE_ID)

      Assertions.assertEquals(oldExistingADV.withSpec(newSpec), newVersion)

      Mockito.verifyNoMoreInteractions(actorDefinitionVersionResolver, synchronousSchedulerClient)
      Mockito.verifyNoInteractions(remoteDefinitionsProvider)
    }
  }

  @Nested
  internal inner class TestGetBreakingChanges {
    @Test
    @Throws(IOException::class)
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

      Mockito
        .`when`(
          remoteDefinitionsProvider.getSourceDefinitionByVersion(
            DOCKER_REPOSITORY,
            LATEST,
          ),
        ).thenReturn(Optional.of(sourceDefWithBreakingChanges))

      val breakingChanges: List<ActorDefinitionBreakingChange> =
        actorDefinitionHandlerHelper.getBreakingChanges(
          actorDefinitionVersion,
          ActorType.SOURCE,
        )
      Assertions.assertEquals(toActorDefinitionBreakingChanges(sourceDefWithBreakingChanges), breakingChanges)

      Mockito.verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST)

      Mockito.verifyNoMoreInteractions(remoteDefinitionsProvider)
      Mockito.verifyNoInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver)
    }
  }

  @Nested
  internal inner class TestGetVersionBreakingChanges {
    @Test
    @Throws(IOException::class)
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

      Mockito
        .`when`(
          actorDefinitionService.listBreakingChangesForActorDefinitionVersion(
            actorDefinitionVersion,
          ),
        ).thenReturn(breakingChangeList as MutableList<ActorDefinitionBreakingChange>)

      val expected =
        ActorDefinitionVersionBreakingChanges()
          .minUpgradeDeadline(LocalDate.parse("2023-01-01"))
          .deadlineAction(DeadlineAction.AUTO_UPGRADE)
          .upcomingBreakingChanges(
            listOf(
              io.airbyte.api.model.generated
                .ActorDefinitionBreakingChange()
                .migrationDocumentationUrl("https://docs.airbyte.io/2")
                .version("2.0.0")
                .upgradeDeadline(LocalDate.parse(("2023-01-01")))
                .message("This is a breaking change")
                .deadlineAction(DeadlineAction.AUTO_UPGRADE),
              io.airbyte.api.model.generated
                .ActorDefinitionBreakingChange()
                .migrationDocumentationUrl("https://docs.airbyte.io/3")
                .version("3.0.0")
                .upgradeDeadline(LocalDate.parse("2023-05-01"))
                .message("This is another breaking change")
                .deadlineAction(DeadlineAction.DISABLE),
            ),
          )

      val actual: Optional<ActorDefinitionVersionBreakingChanges> =
        actorDefinitionHandlerHelper.getVersionBreakingChanges(
          actorDefinitionVersion,
        )
      Assertions.assertEquals(expected, actual.orElseThrow())
    }

    @Test
    @Throws(IOException::class)
    fun testGetVersionBreakingChangesNoBreakingChanges() {
      Mockito
        .`when`(
          actorDefinitionService.listBreakingChangesForActorDefinitionVersion(
            actorDefinitionVersion,
          ),
        ).thenReturn(mutableListOf())

      val actual: Optional<ActorDefinitionVersionBreakingChanges> =
        actorDefinitionHandlerHelper.getVersionBreakingChanges(
          actorDefinitionVersion,
        )
      Assertions.assertEquals(Optional.empty<ActorDefinitionVersionBreakingChanges>(), actual)
    }
  }

  @Test
  @Throws(IOException::class)
  fun testGetNoBreakingChangesAvailable() {
    Mockito
      .`when`(
        remoteDefinitionsProvider.getSourceDefinitionByVersion(
          DOCKER_REPOSITORY,
          LATEST,
        ),
      ).thenReturn(Optional.of(connectorRegistrySourceDefinition))

    val breakingChanges: List<ActorDefinitionBreakingChange> =
      actorDefinitionHandlerHelper.getBreakingChanges(
        actorDefinitionVersion,
        ActorType.SOURCE,
      )
    Assertions.assertEquals(toActorDefinitionBreakingChanges(connectorRegistrySourceDefinition), breakingChanges)

    Mockito.verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST)

    Mockito.verifyNoMoreInteractions(remoteDefinitionsProvider)
    Mockito.verifyNoInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver)
  }

  @Test
  @Throws(IOException::class)
  fun testGetBreakingChangesIfDefinitionNotFound() {
    Mockito
      .`when`(
        remoteDefinitionsProvider.getSourceDefinitionByVersion(
          DOCKER_REPOSITORY,
          DOCKER_IMAGE_TAG,
        ),
      ).thenReturn(
        Optional.empty(),
      )

    val breakingChanges: List<ActorDefinitionBreakingChange?> =
      actorDefinitionHandlerHelper.getBreakingChanges(
        actorDefinitionVersion,
        ActorType.SOURCE,
      )
    Mockito.verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST)
    Assertions.assertEquals(mutableListOf<ActorDefinitionBreakingChange>(), breakingChanges)

    Mockito.verifyNoMoreInteractions(remoteDefinitionsProvider)
    Mockito.verifyNoInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver)
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
