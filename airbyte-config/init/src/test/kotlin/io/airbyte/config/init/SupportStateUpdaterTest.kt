/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init

import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.Configs
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.init.BreakingChangeNotificationHelper.BreakingChangeNotificationData
import io.airbyte.config.init.SupportStateUpdater.SupportStateUpdate
import io.airbyte.config.persistence.BreakingChangesHelper
import io.airbyte.config.persistence.BreakingChangesHelper.WorkspaceBreakingChangeInfo
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.NotifyBreakingChangesOnSupportStateUpdate
import io.airbyte.featureflag.Workspace
import io.airbyte.validation.json.JsonValidationException
import io.mockk.Called
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.time.LocalDate
import java.util.UUID

internal class SupportStateUpdaterTest {
  private val mActorDefinitionService: ActorDefinitionService = mockk()
  private val mSourceService: SourceService = mockk()
  private val mDestinationService: DestinationService = mockk()
  private val mBreakingChangesHelper: BreakingChangesHelper = mockk()
  private val mBreakingChangeNotificationHelper: BreakingChangeNotificationHelper = mockk()
  private lateinit var supportStateUpdater: SupportStateUpdater

  @BeforeEach
  fun setup() {
    val featureFlagClient: FeatureFlagClient = mockk()
    every { featureFlagClient.boolVariation(NotifyBreakingChangesOnSupportStateUpdate, Workspace(ANONYMOUS)) } returns true
    supportStateUpdater =
      SupportStateUpdater(
        mActorDefinitionService,
        mSourceService,
        mDestinationService,
        Configs.AirbyteEdition.CLOUD,
        mBreakingChangesHelper,
        mBreakingChangeNotificationHelper,
        featureFlagClient,
      )

    justRun { mActorDefinitionService.setActorDefinitionVersionSupportStates(any(), any()) }
    justRun { mBreakingChangeNotificationHelper.notifyDeprecatedSyncs(any()) }
    justRun { mBreakingChangeNotificationHelper.notifyUpcomingUpgradeSyncs(any()) }
  }

  private fun createBreakingChange(
    version: String,
    upgradeDeadline: String,
  ): ActorDefinitionBreakingChange =
    ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withVersion(Version(version))
      .withMessage("This is a breaking change for version $version")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#$version")
      .withUpgradeDeadline(upgradeDeadline)

  private fun createActorDefinitionVersion(version: String): ActorDefinitionVersion {
    return ActorDefinitionVersion()
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withVersionId(UUID.randomUUID())
      .withDockerRepository("airbyte/source-connector")
      .withDockerImageTag(version)
      .withSupportState(null) // clear support state to always need a SupportStateUpdate.
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class)
  fun `updating support state for a custom destination should be a no op`() {
    supportStateUpdater.updateSupportStatesForDestinationDefinition(StandardDestinationDefinition().withCustom(true))
    verify { mActorDefinitionService wasNot Called }
    verify { mSourceService wasNot Called }
    verify { mDestinationService wasNot Called }
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class)
  fun `updating support state for a custom source should be a no op`() {
    supportStateUpdater.updateSupportStatesForSourceDefinition(StandardSourceDefinition().withCustom(true))
    verify { mActorDefinitionService wasNot Called }
    verify { mSourceService wasNot Called }
    verify { mDestinationService wasNot Called }
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun `update support state for a destination`() {
    val v0MinorADV = createActorDefinitionVersion(V0_1_0)
    val v1MajorADV = createActorDefinitionVersion(V1_0_0)
    val v1BreakingChange = createBreakingChange(V1_0_0, "2020-01-01")
    val destinationDefinition =
      StandardDestinationDefinition()
        .withName("some destination")
        .withDefaultVersionId(v1MajorADV.versionId)
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)

    every { mActorDefinitionService.getActorDefinitionVersion(v1MajorADV.versionId) } returns v1MajorADV
    every { mActorDefinitionService.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID) } returns listOf(v1BreakingChange)
    every { mActorDefinitionService.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID) } returns listOf(v0MinorADV, v1MajorADV)
    supportStateUpdater.updateSupportStatesForDestinationDefinition(destinationDefinition)

    verify { mActorDefinitionService.getActorDefinitionVersion(v1MajorADV.versionId) }
    verify { mActorDefinitionService.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID) }
    verify { mActorDefinitionService.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID) }
    verify {
      mActorDefinitionService.setActorDefinitionVersionSupportStates(listOf(v0MinorADV.versionId), ActorDefinitionVersion.SupportState.UNSUPPORTED)
    }
    verify {
      mActorDefinitionService.setActorDefinitionVersionSupportStates(
        listOf(v1MajorADV.versionId),
        ActorDefinitionVersion.SupportState.SUPPORTED,
      )
    }
    confirmVerified(mActorDefinitionService)
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun `update support state for a source`() {
    val v0MinorADV = createActorDefinitionVersion(V0_1_0)
    val v1MajorADV = createActorDefinitionVersion(V1_0_0)
    val v1BreakingChange = createBreakingChange(V1_0_0, "2020-01-01")
    val sourceDefinition =
      StandardSourceDefinition()
        .withName("some source")
        .withDefaultVersionId(v1MajorADV.versionId)
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)

    every { mActorDefinitionService.getActorDefinitionVersion(v1MajorADV.versionId) } returns v1MajorADV
    every { mActorDefinitionService.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID) } returns listOf(v1BreakingChange)
    every { mActorDefinitionService.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID) } returns listOf(v0MinorADV, v1MajorADV)
    supportStateUpdater.updateSupportStatesForSourceDefinition(sourceDefinition)

    verify { mActorDefinitionService.getActorDefinitionVersion(v1MajorADV.versionId) }
    verify { mActorDefinitionService.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID) }
    verify { mActorDefinitionService.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID) }
    verify {
      mActorDefinitionService.setActorDefinitionVersionSupportStates(listOf(v0MinorADV.versionId), ActorDefinitionVersion.SupportState.UNSUPPORTED)
    }
    verify {
      mActorDefinitionService.setActorDefinitionVersionSupportStates(
        listOf(v1MajorADV.versionId),
        ActorDefinitionVersion.SupportState.SUPPORTED,
      )
    }
    confirmVerified(mActorDefinitionService)
  }

  @Test
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun `update multiple support states`() {
    val sourceV0MinorADV = createActorDefinitionVersion(V0_1_0)
    val sourceV1MajorADV = createActorDefinitionVersion(V1_0_0)
    val sourceDefinition =
      StandardSourceDefinition()
        .withName("source")
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(sourceV1MajorADV.versionId)

    val destinationDefinitionId = UUID.randomUUID()
    val destV0MinorADV =
      createActorDefinitionVersion(V0_1_0)
        .withActorDefinitionId(destinationDefinitionId)
        .withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED)
    val destV1MajorADV =
      createActorDefinitionVersion(V1_0_0)
        .withActorDefinitionId(destinationDefinitionId)
    val destinationDefinition =
      StandardDestinationDefinition()
        .withName("destination")
        .withDestinationDefinitionId(destinationDefinitionId)
        .withDefaultVersionId(destV1MajorADV.versionId)

    val sourceV1BreakingChange = createBreakingChange(V1_0_0, "2020-01-01")
    val destV1BreakingChange =
      createBreakingChange(V1_0_0, "2020-02-01")
        .withActorDefinitionId(destinationDefinitionId)
    every { mSourceService.listPublicSourceDefinitions(false) } returns listOf(sourceDefinition)
    every { mDestinationService.listPublicDestinationDefinitions(false) } returns listOf(destinationDefinition)
    every { mActorDefinitionService.listBreakingChanges() } returns listOf(sourceV1BreakingChange, destV1BreakingChange)
    every { mActorDefinitionService.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID) } returns listOf(sourceV0MinorADV, sourceV1MajorADV)
    every { mActorDefinitionService.listActorDefinitionVersionsForDefinition(destinationDefinitionId) } returns listOf(destV0MinorADV, destV1MajorADV)
    every { mActorDefinitionService.getActorDefinitionVersion(destV1MajorADV.versionId) } returns destV1MajorADV

    val workspaceIdsToNotify = listOf(UUID.randomUUID(), UUID.randomUUID())
    every {
      mBreakingChangesHelper.getBreakingActiveSyncsPerWorkspace(
        ActorType.DESTINATION,
        destinationDefinition.destinationDefinitionId,
        listOf(destV0MinorADV.versionId),
      )
    } returns workspaceIdsToNotify.stream().map { id: UUID -> WorkspaceBreakingChangeInfo(id, listOf(UUID.randomUUID()), listOf()) }.toList()
    supportStateUpdater.updateSupportStates(LocalDate.parse("2020-01-15"))

    verify {
      mActorDefinitionService.setActorDefinitionVersionSupportStates(
        listOf(sourceV0MinorADV.versionId),
        ActorDefinitionVersion.SupportState.UNSUPPORTED,
      )
    }
    verify {
      mActorDefinitionService.setActorDefinitionVersionSupportStates(
        listOf(destV0MinorADV.versionId),
        ActorDefinitionVersion.SupportState.DEPRECATED,
      )
    }
    verify {
      mActorDefinitionService.setActorDefinitionVersionSupportStates(
        listOf(sourceV1MajorADV.versionId, destV1MajorADV.versionId),
        ActorDefinitionVersion.SupportState.SUPPORTED,
      )
    }
    verify {
      mBreakingChangeNotificationHelper.notifyDeprecatedSyncs(
        listOf(
          BreakingChangeNotificationData(ActorType.DESTINATION, destinationDefinition.name, workspaceIdsToNotify, destV1BreakingChange),
        ),
      )
      mBreakingChangeNotificationHelper.notifyUpcomingUpgradeSyncs(listOf())
      mSourceService.listPublicSourceDefinitions(false)
      mDestinationService.listPublicDestinationDefinitions(false)
      mActorDefinitionService.listBreakingChanges()
      mActorDefinitionService.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID)
      mActorDefinitionService.listActorDefinitionVersionsForDefinition(destinationDefinitionId)
      mActorDefinitionService.getActorDefinitionVersion(destV1MajorADV.versionId)
      mBreakingChangesHelper.getBreakingActiveSyncsPerWorkspace(
        ActorType.DESTINATION,
        destinationDefinition.destinationDefinitionId,
        listOf(destV0MinorADV.versionId),
      )
    }

    confirmVerified(
      mActorDefinitionService,
      mSourceService,
      mDestinationService,
      mBreakingChangesHelper,
      mBreakingChangeNotificationHelper,
    )
  }

  @Test
  fun `get support state update`() {
    val referenceDate = LocalDate.parse("2023-01-01")

    val v1BreakingChange = createBreakingChange(V1_0_0, "2021-02-01")
    val v2BreakingChange = createBreakingChange(V2_0_0, "2022-02-01")
    val v3BreakingChange = createBreakingChange(V3_0_0, "2024-01-01")
    val v4BreakingChange = createBreakingChange("4.0.0", "2025-01-01")
    val v5BreakingChange = createBreakingChange("5.0.0", "2026-01-01")

    val breakingChanges =
      listOf(
        v1BreakingChange,
        v2BreakingChange,
        v3BreakingChange,
        v4BreakingChange,
        v5BreakingChange,
      )

    val v0MinorADV = createActorDefinitionVersion(V0_1_0).withSupportState(ActorDefinitionVersion.SupportState.UNSUPPORTED)
    val v1MajorADV = createActorDefinitionVersion(V1_0_0)
    val v1MinorADV = createActorDefinitionVersion(V1_1_0)
    val v2MajorADV = createActorDefinitionVersion(V2_0_0)
    val v2MinorADV = createActorDefinitionVersion("2.1.0").withSupportState(ActorDefinitionVersion.SupportState.DEPRECATED)
    val v3MajorADV = createActorDefinitionVersion(V3_0_0)
    val v3MinorADV = createActorDefinitionVersion("3.1.0")
    val v4MajorADV = createActorDefinitionVersion("4.0.0")
    val v4MinorADV = createActorDefinitionVersion("4.1.0").withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED)
    val v5MajorADV = createActorDefinitionVersion("5.0.0")

    val actorDefinitionVersions =
      listOf(
        v0MinorADV,
        v1MajorADV,
        v1MinorADV,
        v2MajorADV,
        v2MinorADV,
        v3MajorADV,
        v3MinorADV,
        v4MajorADV,
        v4MinorADV,
        v5MajorADV,
      )

    val expectedSupportStateUpdate =
      SupportStateUpdate(
        listOf(v1MajorADV.versionId, v1MinorADV.versionId),
        listOf(v2MajorADV.versionId, v3MajorADV.versionId, v3MinorADV.versionId),
        listOf(v4MajorADV.versionId, v5MajorADV.versionId),
      )

    val currentActorDefinitionDefaultVersion = Version("4.1.0")
    val supportStateUpdate =
      supportStateUpdater.getSupportStateUpdate(
        currentActorDefinitionDefaultVersion,
        referenceDate,
        breakingChanges,
        actorDefinitionVersions,
      )

    Assertions.assertEquals(expectedSupportStateUpdate, supportStateUpdate)
    verify { mActorDefinitionService wasNot Called }
    verify { mSourceService wasNot Called }
    verify { mDestinationService wasNot Called }
  }

  @Test
  fun `get support state update with no breaking changes`() {
    val referenceDate = LocalDate.parse("2023-01-01")

    val breakingChanges: List<ActorDefinitionBreakingChange> = listOf<ActorDefinitionBreakingChange>()

    val v0MinorADV = createActorDefinitionVersion(V0_1_0)
    val v1MajorADV = createActorDefinitionVersion(V1_0_0)
    val v1MinorADV = createActorDefinitionVersion(V1_1_0)

    val actorDefinitionVersions =
      listOf(
        v0MinorADV,
        v1MajorADV,
        v1MinorADV,
      )

    val expectedSupportStateUpdate = SupportStateUpdate(listOf(), listOf(), listOf())

    val currentActorDefinitionDefaultVersion = Version(V1_1_0)
    val supportStateUpdate =
      supportStateUpdater.getSupportStateUpdate(
        currentActorDefinitionDefaultVersion,
        referenceDate,
        breakingChanges,
        actorDefinitionVersions,
      )

    Assertions.assertEquals(expectedSupportStateUpdate, supportStateUpdate)
    verify { mActorDefinitionService wasNot Called }
    verify { mSourceService wasNot Called }
    verify { mDestinationService wasNot Called }
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun `build source notification data`() {
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("Test Source")

    val v1MajorADV = createActorDefinitionVersion(V1_0_0).withSupportState(ActorDefinitionVersion.SupportState.UNSUPPORTED)
    val v2MajorADV = createActorDefinitionVersion(V2_0_0).withSupportState(ActorDefinitionVersion.SupportState.DEPRECATED)
    val v3MajorADV = createActorDefinitionVersion(V3_0_0).withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED)

    val versionsBeforeUpdate = listOf(v1MajorADV, v2MajorADV, v3MajorADV)
    val supportStateUpdate =
      SupportStateUpdate(listOf(), listOf(v1MajorADV.versionId, v3MajorADV.versionId), listOf())
    val workspaceIds = listOf(UUID.randomUUID(), UUID.randomUUID())
    every {
      mBreakingChangesHelper.getBreakingActiveSyncsPerWorkspace(
        ActorType.SOURCE,
        sourceDefinition.sourceDefinitionId,
        listOf(v3MajorADV.versionId),
      )
    } returns
      workspaceIds
        .stream()
        .map { id: UUID -> WorkspaceBreakingChangeInfo(id, listOf(UUID.randomUUID(), UUID.randomUUID()), listOf()) }
        .toList()
    val latestBreakingChange =
      ActorDefinitionBreakingChange()
        .withMessage("Test Breaking Change")

    val expectedNotificationData =
      BreakingChangeNotificationData(ActorType.SOURCE, sourceDefinition.name, workspaceIds, latestBreakingChange)
    val notificationData =
      supportStateUpdater.buildSourceNotificationData(sourceDefinition, latestBreakingChange, versionsBeforeUpdate, supportStateUpdate)
    Assertions.assertEquals(expectedNotificationData, notificationData)

    verify {
      mBreakingChangesHelper.getBreakingActiveSyncsPerWorkspace(
        ActorType.SOURCE,
        sourceDefinition.sourceDefinitionId,
        listOf(v3MajorADV.versionId),
      )
    }
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun `build destination notification data`() {
    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("Test Destination")

    val v1MajorADV = createActorDefinitionVersion(V1_0_0).withSupportState(ActorDefinitionVersion.SupportState.UNSUPPORTED)
    val v2MajorADV = createActorDefinitionVersion(V2_0_0).withSupportState(ActorDefinitionVersion.SupportState.DEPRECATED)
    val v3MajorADV = createActorDefinitionVersion(V3_0_0).withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED)

    val versionsBeforeUpdate = listOf(v1MajorADV, v2MajorADV, v3MajorADV)
    val supportStateUpdate =
      SupportStateUpdate(listOf(), listOf(v1MajorADV.versionId, v3MajorADV.versionId), listOf())
    val workspaceIds = listOf(UUID.randomUUID(), UUID.randomUUID())
    every {
      mBreakingChangesHelper.getBreakingActiveSyncsPerWorkspace(
        ActorType.DESTINATION,
        destinationDefinition.destinationDefinitionId,
        listOf(v3MajorADV.versionId),
      )
    } returns
      workspaceIds
        .stream()
        .map { id: UUID -> WorkspaceBreakingChangeInfo(id, listOf(UUID.randomUUID(), UUID.randomUUID()), listOf()) }
        .toList()
    val latestBreakingChange =
      ActorDefinitionBreakingChange()
        .withMessage("Test Breaking Change 2")

    val expectedNotificationData =
      BreakingChangeNotificationData(ActorType.DESTINATION, destinationDefinition.name, workspaceIds, latestBreakingChange)
    val notificationData =
      supportStateUpdater.buildDestinationNotificationData(
        destinationDefinition,
        latestBreakingChange,
        versionsBeforeUpdate,
        supportStateUpdate,
      )
    Assertions.assertEquals(expectedNotificationData, notificationData)

    verify {
      mBreakingChangesHelper.getBreakingActiveSyncsPerWorkspace(
        ActorType.DESTINATION,
        destinationDefinition.destinationDefinitionId,
        listOf(v3MajorADV.versionId),
      )
    }
  }

  companion object {
    private val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private const val V0_1_0 = "0.1.0"
    private const val V1_0_0 = "1.0.0"
    private const val V1_1_0 = "1.1.0"
    private const val V2_0_0 = "2.0.0"
    private const val V3_0_0 = "3.0.0"
  }
}
