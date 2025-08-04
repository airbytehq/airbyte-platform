/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges
import io.airbyte.api.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionRequestBody
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionResponse
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SupportState
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.errors.NotFoundException
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ReleaseStage
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.SupportLevel
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.config.persistence.ActorDefinitionVersionResolver
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mockito
import java.io.IOException
import java.util.Optional
import java.util.UUID

internal class ActorDefinitionVersionHandlerTest {
  private var mSourceService: SourceService? = null
  private var mDestinationService: DestinationService? = null
  private var mActorDefinitionService: ActorDefinitionService? = null
  private var mActorDefinitionVersionResolver: ActorDefinitionVersionResolver? = null
  private var mActorDefinitionVersionHelper: ActorDefinitionVersionHelper? = null
  private var mActorDefinitionHandlerHelper: ActorDefinitionHandlerHelper? = null

  private var actorDefinitionVersionHandler: ActorDefinitionVersionHandler? = null
  private val apiPojoConverters = ApiPojoConverters(CatalogConverter(FieldGenerator(), mutableListOf()))

  @BeforeEach
  fun setUp() {
    mSourceService = Mockito.mock(SourceService::class.java)
    mDestinationService = Mockito.mock(DestinationService::class.java)
    mActorDefinitionService = Mockito.mock(ActorDefinitionService::class.java)
    mActorDefinitionVersionResolver = Mockito.mock(ActorDefinitionVersionResolver::class.java)
    mActorDefinitionVersionHelper = Mockito.mock(ActorDefinitionVersionHelper::class.java)
    mActorDefinitionHandlerHelper = Mockito.mock(ActorDefinitionHandlerHelper::class.java)
    actorDefinitionVersionHandler =
      ActorDefinitionVersionHandler(
        mSourceService!!,
        mDestinationService!!,
        mActorDefinitionService!!,
        mActorDefinitionVersionResolver!!,
        mActorDefinitionVersionHelper!!,
        mActorDefinitionHandlerHelper!!,
        apiPojoConverters,
      )
  }

  private fun createActorDefinitionVersion(): ActorDefinitionVersion =
    ActorDefinitionVersion()
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withVersionId(UUID.randomUUID())
      .withSupportLevel(SupportLevel.NONE)
      .withInternalSupportLevel(100L)
      .withReleaseStage(ReleaseStage.BETA)
      .withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED)
      .withDockerRepository("airbyte/source-faker")
      .withDockerImageTag("1.0.2")
      .withDocumentationUrl("https://docs.airbyte.io")

  private fun createActorDefinitionVersionWithNormalization(): ActorDefinitionVersion = createActorDefinitionVersion()

  @ParameterizedTest
  @CsvSource("true", "false")
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testGetActorDefinitionVersionForSource(isVersionOverrideApplied: Boolean) {
    val sourceId = UUID.randomUUID()
    val actorDefinitionVersion = createActorDefinitionVersion()
    val sourceConnection =
      SourceConnection()
        .withSourceId(sourceId)
        .withWorkspaceId(WORKSPACE_ID)

    Mockito
      .`when`(mSourceService!!.getSourceConnection(sourceId))
      .thenReturn(sourceConnection)
    Mockito
      .`when`(mSourceService!!.getSourceDefinitionFromSource(sourceId))
      .thenReturn(SOURCE_DEFINITION)
    Mockito
      .`when`(
        mActorDefinitionVersionHelper!!.getSourceVersionWithOverrideStatus(
          SOURCE_DEFINITION,
          WORKSPACE_ID,
          sourceId,
        ),
      ).thenReturn(ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, isVersionOverrideApplied))

    val sourceIdRequestBody = SourceIdRequestBody().sourceId(sourceId)
    val actorDefinitionVersionRead =
      actorDefinitionVersionHandler!!.getActorDefinitionVersionForSourceId(sourceIdRequestBody)
    val expectedRead =
      ActorDefinitionVersionRead()
        .isVersionOverrideApplied(isVersionOverrideApplied)
        .supportLevel(io.airbyte.api.model.generated.SupportLevel.NONE)
        .supportState(SupportState.SUPPORTED)
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportsRefreshes(false)
        .supportsFileTransfer(false)
        .supportsDataActivation(false)

    Assertions.assertEquals(expectedRead, actorDefinitionVersionRead)
    Mockito.verify<SourceService?>(mSourceService).getSourceConnection(sourceId)
    Mockito.verify<SourceService?>(mSourceService).getSourceDefinitionFromSource(sourceId)
    Mockito
      .verify<ActorDefinitionVersionHelper?>(mActorDefinitionVersionHelper)
      .getSourceVersionWithOverrideStatus(SOURCE_DEFINITION, WORKSPACE_ID, sourceId)
    Mockito.verify<ActorDefinitionHandlerHelper?>(mActorDefinitionHandlerHelper).getVersionBreakingChanges(actorDefinitionVersion)
    Mockito.verifyNoMoreInteractions(mSourceService)
    Mockito.verifyNoMoreInteractions(mActorDefinitionHandlerHelper)
    Mockito.verifyNoMoreInteractions(mActorDefinitionVersionHelper)
    Mockito.verifyNoInteractions(mDestinationService)
  }

  @ParameterizedTest
  @CsvSource("true", "false")
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testGetActorDefinitionVersionForDestination(isVersionOverrideApplied: Boolean) {
    val destinationId = UUID.randomUUID()
    val actorDefinitionVersion = createActorDefinitionVersion()
    val destinationConnection =
      DestinationConnection()
        .withDestinationId(destinationId)
        .withWorkspaceId(WORKSPACE_ID)

    Mockito
      .`when`(mDestinationService!!.getDestinationConnection(destinationId))
      .thenReturn(destinationConnection)
    Mockito
      .`when`(mDestinationService!!.getDestinationDefinitionFromDestination(destinationId))
      .thenReturn(DESTINATION_DEFINITION)
    Mockito
      .`when`(
        mActorDefinitionVersionHelper!!.getDestinationVersionWithOverrideStatus(
          DESTINATION_DEFINITION,
          WORKSPACE_ID,
          destinationId,
        ),
      ).thenReturn(ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, isVersionOverrideApplied))

    val destinationIdRequestBody = DestinationIdRequestBody().destinationId(destinationId)
    val actorDefinitionVersionRead =
      actorDefinitionVersionHandler!!.getActorDefinitionVersionForDestinationId(destinationIdRequestBody)
    val expectedRead =
      ActorDefinitionVersionRead()
        .isVersionOverrideApplied(isVersionOverrideApplied)
        .supportLevel(io.airbyte.api.model.generated.SupportLevel.NONE)
        .supportState(SupportState.SUPPORTED)
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportsRefreshes(false)
        .supportsFileTransfer(false)
        .supportsDataActivation(false)

    Assertions.assertEquals(expectedRead, actorDefinitionVersionRead)
    Mockito.verify<DestinationService?>(mDestinationService).getDestinationConnection(destinationId)
    Mockito.verify<DestinationService?>(mDestinationService).getDestinationDefinitionFromDestination(destinationId)
    Mockito
      .verify<ActorDefinitionVersionHelper?>(mActorDefinitionVersionHelper)
      .getDestinationVersionWithOverrideStatus(DESTINATION_DEFINITION, WORKSPACE_ID, destinationId)
    Mockito.verify<ActorDefinitionHandlerHelper?>(mActorDefinitionHandlerHelper).getVersionBreakingChanges(actorDefinitionVersion)
    Mockito.verifyNoMoreInteractions(mDestinationService)
    Mockito.verifyNoMoreInteractions(mActorDefinitionHandlerHelper)
    Mockito.verifyNoMoreInteractions(mActorDefinitionVersionHelper)
  }

  @ParameterizedTest
  @CsvSource("true", "false")
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testGetActorDefinitionVersionForDestinationWithNormalization(isVersionOverrideApplied: Boolean) {
    val destinationId = UUID.randomUUID()
    val actorDefinitionVersion = createActorDefinitionVersionWithNormalization()
    val destinationConnection =
      DestinationConnection()
        .withDestinationId(destinationId)
        .withWorkspaceId(WORKSPACE_ID)

    Mockito
      .`when`(mDestinationService!!.getDestinationConnection(destinationId))
      .thenReturn(destinationConnection)
    Mockito
      .`when`(mDestinationService!!.getDestinationDefinitionFromDestination(destinationId))
      .thenReturn(DESTINATION_DEFINITION)
    Mockito
      .`when`(
        mActorDefinitionVersionHelper!!.getDestinationVersionWithOverrideStatus(
          DESTINATION_DEFINITION,
          WORKSPACE_ID,
          destinationId,
        ),
      ).thenReturn(ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, isVersionOverrideApplied))

    val destinationIdRequestBody = DestinationIdRequestBody().destinationId(destinationId)
    val actorDefinitionVersionRead =
      actorDefinitionVersionHandler!!.getActorDefinitionVersionForDestinationId(destinationIdRequestBody)
    val expectedRead =
      ActorDefinitionVersionRead()
        .isVersionOverrideApplied(isVersionOverrideApplied)
        .supportLevel(io.airbyte.api.model.generated.SupportLevel.NONE)
        .supportState(SupportState.SUPPORTED)
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportsRefreshes(actorDefinitionVersion.getSupportsRefreshes())
        .supportsFileTransfer(false)
        .supportsDataActivation(false)

    Assertions.assertEquals(expectedRead, actorDefinitionVersionRead)
    Mockito.verify<DestinationService?>(mDestinationService).getDestinationConnection(destinationId)
    Mockito.verify<DestinationService?>(mDestinationService).getDestinationDefinitionFromDestination(destinationId)
    Mockito
      .verify<ActorDefinitionVersionHelper?>(mActorDefinitionVersionHelper)
      .getDestinationVersionWithOverrideStatus(DESTINATION_DEFINITION, WORKSPACE_ID, destinationId)
    Mockito.verify<ActorDefinitionHandlerHelper?>(mActorDefinitionHandlerHelper).getVersionBreakingChanges(actorDefinitionVersion)
    Mockito.verifyNoMoreInteractions(mDestinationService)
    Mockito.verifyNoMoreInteractions(mActorDefinitionHandlerHelper)
    Mockito.verifyNoMoreInteractions(mActorDefinitionVersionHelper)
    Mockito.verifyNoInteractions(mSourceService)
  }

  @Test
  @Throws(IOException::class)
  fun testCreateActorDefinitionVersionReadWithBreakingChange() {
    val breakingChanges = Mockito.mock(ActorDefinitionVersionBreakingChanges::class.java)

    val actorDefinitionVersion = createActorDefinitionVersion().withSupportState(ActorDefinitionVersion.SupportState.DEPRECATED)
    Mockito
      .`when`(
        mActorDefinitionHandlerHelper!!.getVersionBreakingChanges(
          actorDefinitionVersion,
        ),
      ).thenReturn(
        Optional.of(breakingChanges),
      )

    val versionWithOverrideStatus =
      ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, false)
    val actorDefinitionVersionRead =
      actorDefinitionVersionHandler!!.createActorDefinitionVersionRead(versionWithOverrideStatus)

    val expectedRead =
      ActorDefinitionVersionRead()
        .isVersionOverrideApplied(false)
        .supportLevel(io.airbyte.api.model.generated.SupportLevel.NONE)
        .supportState(SupportState.DEPRECATED)
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportsRefreshes(false)
        .breakingChanges(breakingChanges)
        .supportsFileTransfer(false)
        .supportsDataActivation(false)

    Assertions.assertEquals(expectedRead, actorDefinitionVersionRead)
    Mockito.verify<ActorDefinitionHandlerHelper?>(mActorDefinitionHandlerHelper).getVersionBreakingChanges(actorDefinitionVersion)
    Mockito.verifyNoMoreInteractions(mActorDefinitionHandlerHelper)
    Mockito.verifyNoInteractions(mActorDefinitionVersionHelper)
    Mockito.verifyNoInteractions(mSourceService)
    Mockito.verifyNoInteractions(mDestinationService)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testResolveActorDefinitionVersionByTag() {
    val actorDefinitionId = UUID.randomUUID()
    val actorDefinitionVersion = createActorDefinitionVersion()
    val resolveActorDefinitionVersionResponse =
      ResolveActorDefinitionVersionResponse()
        .versionId(actorDefinitionVersion.getVersionId())
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportRefreshes(false)
        .supportFileTransfer(false)
        .supportDataActivation(false)

    Mockito
      .`when`(mSourceService!!.getStandardSourceDefinition(actorDefinitionId))
      .thenReturn(clone(SOURCE_DEFINITION).withDefaultVersionId(actorDefinitionVersion.getVersionId()))
    Mockito
      .`when`(mActorDefinitionService!!.getActorDefinitionVersion(actorDefinitionVersion.getVersionId()))
      .thenReturn(actorDefinitionVersion)
    Mockito
      .`when`(
        mActorDefinitionVersionResolver!!.resolveVersionForTag(
          actorDefinitionId,
          ActorType.SOURCE,
          actorDefinitionVersion.getDockerRepository(),
          actorDefinitionVersion.getDockerImageTag(),
        ),
      ).thenReturn(Optional.of<ActorDefinitionVersion>(actorDefinitionVersion))

    val resolvedActorDefinitionVersion =
      actorDefinitionVersionHandler!!.resolveActorDefinitionVersionByTag(
        ResolveActorDefinitionVersionRequestBody()
          .actorDefinitionId(actorDefinitionId)
          .actorType(io.airbyte.api.model.generated.ActorType.SOURCE)
          .dockerImageTag(actorDefinitionVersion.getDockerImageTag()),
      )

    Assertions.assertEquals(resolveActorDefinitionVersionResponse, resolvedActorDefinitionVersion)
    Mockito.verify<ActorDefinitionVersionResolver?>(mActorDefinitionVersionResolver).resolveVersionForTag(
      actorDefinitionId,
      ActorType.SOURCE,
      actorDefinitionVersion.getDockerRepository(),
      actorDefinitionVersion.getDockerImageTag(),
    )
    Mockito.verify<ActorDefinitionService?>(mActorDefinitionService).getActorDefinitionVersion(actorDefinitionVersion.getVersionId())
    Mockito.verifyNoMoreInteractions(mActorDefinitionVersionResolver)
    Mockito.verifyNoMoreInteractions(mActorDefinitionService)
    Mockito.verifyNoInteractions(mActorDefinitionVersionHelper)
    Mockito.verifyNoInteractions(mDestinationService)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testResolveMissingActorDefinitionVersionByTag() {
    val actorDefinitionId = UUID.randomUUID()
    val defaultVersionId = UUID.randomUUID()
    val dockerRepository = "airbyte/source-pg"
    val dockerImageTag = "1.0.2"

    Mockito
      .`when`(mSourceService!!.getStandardSourceDefinition(actorDefinitionId))
      .thenReturn(clone(SOURCE_DEFINITION).withDefaultVersionId(defaultVersionId))
    Mockito
      .`when`(mActorDefinitionService!!.getActorDefinitionVersion(defaultVersionId))
      .thenReturn(ActorDefinitionVersion().withDockerRepository(dockerRepository))

    Mockito
      .`when`(
        mActorDefinitionVersionResolver!!.resolveVersionForTag(
          actorDefinitionId,
          ActorType.SOURCE,
          dockerRepository,
          dockerImageTag,
        ),
      ).thenReturn(Optional.empty<ActorDefinitionVersion>())

    val resolveVersionRequestBody =
      ResolveActorDefinitionVersionRequestBody()
        .actorDefinitionId(actorDefinitionId)
        .actorType(io.airbyte.api.model.generated.ActorType.SOURCE)
        .dockerImageTag(dockerImageTag)

    val exception =
      Assertions.assertThrows(
        NotFoundException::class.java,
        Executable { actorDefinitionVersionHandler!!.resolveActorDefinitionVersionByTag(resolveVersionRequestBody) },
      )
    Assertions.assertEquals(
      String.format(
        "Could not find actor definition version for actor definition id %s and tag %s",
        actorDefinitionId,
        dockerImageTag,
      ),
      exception.message,
    )
    Mockito
      .verify<ActorDefinitionVersionResolver?>(mActorDefinitionVersionResolver)
      .resolveVersionForTag(actorDefinitionId, ActorType.SOURCE, dockerRepository, dockerImageTag)
    Mockito.verifyNoMoreInteractions(mActorDefinitionVersionResolver)
    Mockito.verify<ActorDefinitionService?>(mActorDefinitionService).getActorDefinitionVersion(defaultVersionId)
    Mockito.verifyNoMoreInteractions(mActorDefinitionService)
    Mockito.verifyNoInteractions(mActorDefinitionVersionHelper)
    Mockito.verifyNoInteractions(mDestinationService)
  }

  companion object {
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private val SOURCE_DEFINITION: StandardSourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
    private val DESTINATION_DEFINITION: StandardDestinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
  }
}
