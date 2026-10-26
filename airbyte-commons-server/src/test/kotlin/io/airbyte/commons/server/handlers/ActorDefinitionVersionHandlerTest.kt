/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.ActorDefinitionVersionBreakingChanges
import io.airbyte.api.server.generated.models.ActorDefinitionVersionRead
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.ResolveActorDefinitionVersionRequestBody
import io.airbyte.api.server.generated.models.ResolveActorDefinitionVersionResponse
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.SupportState
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
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.mockk.Called
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.time.LocalDate
import java.util.Optional
import java.util.UUID
import io.airbyte.api.server.generated.models.ActorType as KotlinActorType
import io.airbyte.api.server.generated.models.SupportLevel as KotlinSupportLevel

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
    mSourceService = mockk<SourceService>()
    mDestinationService = mockk<DestinationService>()
    mActorDefinitionService = mockk<ActorDefinitionService>()
    mActorDefinitionVersionResolver = mockk<ActorDefinitionVersionResolver>()
    mActorDefinitionVersionHelper = mockk<ActorDefinitionVersionHelper>()
    mActorDefinitionHandlerHelper = mockk<ActorDefinitionHandlerHelper>()
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
  fun testGetActorDefinitionVersionForSource(isVersionOverrideApplied: Boolean) {
    val sourceId = UUID.randomUUID()
    val actorDefinitionVersion = createActorDefinitionVersion()
    val sourceConnection =
      SourceConnection()
        .withSourceId(sourceId)
        .withWorkspaceId(WORKSPACE_ID)

    every { mSourceService!!.getSourceConnection(sourceId) } returns sourceConnection
    every { mSourceService!!.getSourceDefinitionFromSource(sourceId) } returns SOURCE_DEFINITION
    every {
      mActorDefinitionVersionHelper!!.getSourceVersionWithOverrideStatus(
        SOURCE_DEFINITION,
        WORKSPACE_ID,
        sourceId,
      )
    } returns ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, isVersionOverrideApplied)
    every { mActorDefinitionHandlerHelper!!.getVersionBreakingChanges(actorDefinitionVersion) } returns Optional.empty()

    val sourceIdRequestBody = SourceIdRequestBody(sourceId = sourceId)
    val actorDefinitionVersionRead =
      actorDefinitionVersionHandler!!.getActorDefinitionVersionForSourceId(sourceIdRequestBody)
    val expectedRead =
      ActorDefinitionVersionRead(
        isVersionOverrideApplied = isVersionOverrideApplied,
        supportLevel = KotlinSupportLevel.NONE,
        supportState = SupportState.SUPPORTED,
        dockerRepository = actorDefinitionVersion.dockerRepository,
        dockerImageTag = actorDefinitionVersion.dockerImageTag,
        supportsRefreshes = false,
        supportsFileTransfer = false,
        supportsDataActivation = false,
      )

    Assertions.assertEquals(expectedRead, actorDefinitionVersionRead)
    verify { mSourceService!!.getSourceConnection(sourceId) }
    verify { mSourceService!!.getSourceDefinitionFromSource(sourceId) }
    verify { mActorDefinitionVersionHelper!!.getSourceVersionWithOverrideStatus(SOURCE_DEFINITION, WORKSPACE_ID, sourceId) }
    verify { mActorDefinitionHandlerHelper!!.getVersionBreakingChanges(actorDefinitionVersion) }
    confirmVerified(mSourceService!!)
    confirmVerified(mActorDefinitionHandlerHelper!!)
    confirmVerified(mActorDefinitionVersionHelper!!)
    verify { mDestinationService!! wasNot Called }
  }

  @ParameterizedTest
  @CsvSource("true", "false")
  fun testGetActorDefinitionVersionForDestination(isVersionOverrideApplied: Boolean) {
    val destinationId = UUID.randomUUID()
    val actorDefinitionVersion = createActorDefinitionVersion()
    val destinationConnection =
      DestinationConnection()
        .withDestinationId(destinationId)
        .withWorkspaceId(WORKSPACE_ID)

    every { mDestinationService!!.getDestinationConnection(destinationId) } returns destinationConnection
    every { mDestinationService!!.getDestinationDefinitionFromDestination(destinationId) } returns DESTINATION_DEFINITION
    every {
      mActorDefinitionVersionHelper!!.getDestinationVersionWithOverrideStatus(
        DESTINATION_DEFINITION,
        WORKSPACE_ID,
        destinationId,
      )
    } returns ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, isVersionOverrideApplied)
    every { mActorDefinitionHandlerHelper!!.getVersionBreakingChanges(actorDefinitionVersion) } returns Optional.empty()

    val destinationIdRequestBody = DestinationIdRequestBody(destinationId = destinationId)
    val actorDefinitionVersionRead =
      actorDefinitionVersionHandler!!.getActorDefinitionVersionForDestinationId(destinationIdRequestBody)
    val expectedRead =
      ActorDefinitionVersionRead(
        isVersionOverrideApplied = isVersionOverrideApplied,
        supportLevel = KotlinSupportLevel.NONE,
        supportState = SupportState.SUPPORTED,
        dockerRepository = actorDefinitionVersion.dockerRepository,
        dockerImageTag = actorDefinitionVersion.dockerImageTag,
        supportsRefreshes = false,
        supportsFileTransfer = false,
        supportsDataActivation = false,
      )

    Assertions.assertEquals(expectedRead, actorDefinitionVersionRead)
    verify { mDestinationService!!.getDestinationConnection(destinationId) }
    verify { mDestinationService!!.getDestinationDefinitionFromDestination(destinationId) }
    verify { mActorDefinitionVersionHelper!!.getDestinationVersionWithOverrideStatus(DESTINATION_DEFINITION, WORKSPACE_ID, destinationId) }
    verify { mActorDefinitionHandlerHelper!!.getVersionBreakingChanges(actorDefinitionVersion) }
    confirmVerified(mDestinationService!!)
    confirmVerified(mActorDefinitionHandlerHelper!!)
    confirmVerified(mActorDefinitionVersionHelper!!)
  }

  @ParameterizedTest
  @CsvSource("true", "false")
  fun testGetActorDefinitionVersionForDestinationWithNormalization(isVersionOverrideApplied: Boolean) {
    val destinationId = UUID.randomUUID()
    val actorDefinitionVersion = createActorDefinitionVersionWithNormalization()
    val destinationConnection =
      DestinationConnection()
        .withDestinationId(destinationId)
        .withWorkspaceId(WORKSPACE_ID)

    every { mDestinationService!!.getDestinationConnection(destinationId) } returns destinationConnection
    every { mDestinationService!!.getDestinationDefinitionFromDestination(destinationId) } returns DESTINATION_DEFINITION
    every {
      mActorDefinitionVersionHelper!!.getDestinationVersionWithOverrideStatus(
        DESTINATION_DEFINITION,
        WORKSPACE_ID,
        destinationId,
      )
    } returns ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, isVersionOverrideApplied)
    every { mActorDefinitionHandlerHelper!!.getVersionBreakingChanges(actorDefinitionVersion) } returns Optional.empty()

    val destinationIdRequestBody = DestinationIdRequestBody(destinationId = destinationId)
    val actorDefinitionVersionRead =
      actorDefinitionVersionHandler!!.getActorDefinitionVersionForDestinationId(destinationIdRequestBody)
    val expectedRead =
      ActorDefinitionVersionRead(
        isVersionOverrideApplied = isVersionOverrideApplied,
        supportLevel = KotlinSupportLevel.NONE,
        supportState = SupportState.SUPPORTED,
        dockerRepository = actorDefinitionVersion.dockerRepository,
        dockerImageTag = actorDefinitionVersion.dockerImageTag,
        supportsRefreshes = actorDefinitionVersion.supportsRefreshes,
        supportsFileTransfer = false,
        supportsDataActivation = false,
      )

    Assertions.assertEquals(expectedRead, actorDefinitionVersionRead)
    verify { mDestinationService!!.getDestinationConnection(destinationId) }
    verify { mDestinationService!!.getDestinationDefinitionFromDestination(destinationId) }
    verify { mActorDefinitionVersionHelper!!.getDestinationVersionWithOverrideStatus(DESTINATION_DEFINITION, WORKSPACE_ID, destinationId) }
    verify { mActorDefinitionHandlerHelper!!.getVersionBreakingChanges(actorDefinitionVersion) }
    confirmVerified(mDestinationService!!)
    confirmVerified(mActorDefinitionHandlerHelper!!)
    confirmVerified(mActorDefinitionVersionHelper!!)
    verify { mSourceService!! wasNot Called }
  }

  @Test
  fun testCreateActorDefinitionVersionReadWithBreakingChange() {
    val breakingChanges = ActorDefinitionVersionBreakingChanges(upcomingBreakingChanges = emptyList(), minUpgradeDeadline = LocalDate.of(2026, 1, 1))

    val actorDefinitionVersion = createActorDefinitionVersion().withSupportState(ActorDefinitionVersion.SupportState.DEPRECATED)
    every { mActorDefinitionHandlerHelper!!.getVersionBreakingChanges(actorDefinitionVersion) } returns Optional.of(breakingChanges)

    val versionWithOverrideStatus =
      ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, false)
    val actorDefinitionVersionRead =
      actorDefinitionVersionHandler!!.createActorDefinitionVersionRead(versionWithOverrideStatus)

    val expectedRead =
      ActorDefinitionVersionRead(
        isVersionOverrideApplied = false,
        supportLevel = KotlinSupportLevel.NONE,
        supportState = SupportState.DEPRECATED,
        dockerRepository = actorDefinitionVersion.dockerRepository,
        dockerImageTag = actorDefinitionVersion.dockerImageTag,
        supportsRefreshes = false,
        breakingChanges = breakingChanges,
        supportsFileTransfer = false,
        supportsDataActivation = false,
      )

    Assertions.assertEquals(expectedRead, actorDefinitionVersionRead)
    verify { mActorDefinitionHandlerHelper!!.getVersionBreakingChanges(actorDefinitionVersion) }
    confirmVerified(mActorDefinitionHandlerHelper!!)
    verify { mActorDefinitionVersionHelper!! wasNot Called }
    verify { mSourceService!! wasNot Called }
    verify { mDestinationService!! wasNot Called }
  }

  @Test
  fun testResolveActorDefinitionVersionByTag() {
    val actorDefinitionId = UUID.randomUUID()
    val actorDefinitionVersion = createActorDefinitionVersion()
    val resolveActorDefinitionVersionResponse =
      ResolveActorDefinitionVersionResponse(
        versionId = actorDefinitionVersion.versionId,
        dockerRepository = actorDefinitionVersion.dockerRepository,
        dockerImageTag = actorDefinitionVersion.dockerImageTag,
        supportRefreshes = false,
        supportFileTransfer = false,
        supportDataActivation = false,
      )

    every { mSourceService!!.getStandardSourceDefinition(actorDefinitionId) } returns
      clone(SOURCE_DEFINITION).withDefaultVersionId(actorDefinitionVersion.versionId)
    every { mActorDefinitionService!!.getActorDefinitionVersion(actorDefinitionVersion.versionId) } returns actorDefinitionVersion
    every {
      mActorDefinitionVersionResolver!!.resolveVersionForTag(
        actorDefinitionId,
        ActorType.SOURCE,
        actorDefinitionVersion.dockerRepository,
        actorDefinitionVersion.dockerImageTag,
      )
    } returns Optional.of<ActorDefinitionVersion>(actorDefinitionVersion)

    val resolvedActorDefinitionVersion =
      actorDefinitionVersionHandler!!.resolveActorDefinitionVersionByTag(
        ResolveActorDefinitionVersionRequestBody(
          actorDefinitionId = actorDefinitionId,
          actorType = KotlinActorType.SOURCE,
          dockerImageTag = actorDefinitionVersion.dockerImageTag,
        ),
      )

    Assertions.assertEquals(resolveActorDefinitionVersionResponse, resolvedActorDefinitionVersion)
    verify {
      mActorDefinitionVersionResolver!!.resolveVersionForTag(
        actorDefinitionId,
        ActorType.SOURCE,
        actorDefinitionVersion.dockerRepository,
        actorDefinitionVersion.dockerImageTag,
      )
    }
    verify { mActorDefinitionService!!.getActorDefinitionVersion(actorDefinitionVersion.versionId) }
    confirmVerified(mActorDefinitionVersionResolver!!)
    confirmVerified(mActorDefinitionService!!)
    verify { mActorDefinitionVersionHelper!! wasNot Called }
    verify { mDestinationService!! wasNot Called }
  }

  @Test
  fun testResolveMissingActorDefinitionVersionByTag() {
    val actorDefinitionId = UUID.randomUUID()
    val defaultVersionId = UUID.randomUUID()
    val dockerRepository = "airbyte/source-pg"
    val dockerImageTag = "1.0.2"

    every { mSourceService!!.getStandardSourceDefinition(actorDefinitionId) } returns clone(SOURCE_DEFINITION).withDefaultVersionId(defaultVersionId)
    every { mActorDefinitionService!!.getActorDefinitionVersion(defaultVersionId) } returns
      ActorDefinitionVersion().withDockerRepository(dockerRepository)

    every {
      mActorDefinitionVersionResolver!!.resolveVersionForTag(
        actorDefinitionId,
        ActorType.SOURCE,
        dockerRepository,
        dockerImageTag,
      )
    } returns Optional.empty<ActorDefinitionVersion>()

    val resolveVersionRequestBody =
      ResolveActorDefinitionVersionRequestBody(
        actorDefinitionId = actorDefinitionId,
        actorType = KotlinActorType.SOURCE,
        dockerImageTag = dockerImageTag,
      )

    val exception =
      Assertions.assertThrows(
        NotFoundException::class.java,
        { actorDefinitionVersionHandler!!.resolveActorDefinitionVersionByTag(resolveVersionRequestBody) },
      )
    Assertions.assertEquals(
      String.format(
        "Could not find actor definition version for actor definition id %s and tag %s",
        actorDefinitionId,
        dockerImageTag,
      ),
      exception.message,
    )
    verify { mActorDefinitionVersionResolver!!.resolveVersionForTag(actorDefinitionId, ActorType.SOURCE, dockerRepository, dockerImageTag) }
    confirmVerified(mActorDefinitionVersionResolver!!)
    verify { mActorDefinitionService!!.getActorDefinitionVersion(defaultVersionId) }
    confirmVerified(mActorDefinitionService!!)
    verify { mActorDefinitionVersionHelper!! wasNot Called }
    verify { mDestinationService!! wasNot Called }
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
