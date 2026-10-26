/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.SourceConnection
import io.airbyte.config.persistence.BreakingChangesHelper.WorkspaceBreakingChangeInfo
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.airbyte.data.services.shared.StandardSyncQuery
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.stream.Stream

internal class BreakingChangesHelperTest {
  private lateinit var mWorkspaceService: WorkspaceService
  private lateinit var mScopedConfigurationService: ScopedConfigurationService
  private lateinit var mSourceService: SourceService
  private lateinit var mDestinationService: DestinationService

  private lateinit var breakingChangesHelper: BreakingChangesHelper

  @BeforeEach
  fun setup() {
    mWorkspaceService = mockk<WorkspaceService>()
    mScopedConfigurationService = mockk<ScopedConfigurationService>()
    mSourceService = mockk<SourceService>()
    mDestinationService = mockk<DestinationService>()

    breakingChangesHelper = BreakingChangesHelper(mScopedConfigurationService, mWorkspaceService, mDestinationService, mSourceService)
  }

  @Test
  fun testGetBreakingActiveSyncsPerWorkspaceWithSource() {
    val unsupportedVersionId1 = UUID.randomUUID()
    val unsupportedVersionId2 = UUID.randomUUID()

    val unsupportedVersionIds = listOf(unsupportedVersionId1, unsupportedVersionId2)

    val workspaceId1 = UUID.randomUUID()
    val source1 =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(workspaceId1)
    val source2 =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(workspaceId1)

    val workspaceId2 = UUID.randomUUID()
    val source3 =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(workspaceId2)

    val workspaceId3 = UUID.randomUUID()
    val source4 =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(workspaceId3)

    val sourcesOnUnsupportedVersions = listOf(source1, source2, source3, source4)
    val sourceIds = sourcesOnUnsupportedVersions.map { it.getSourceId() }

    every { mSourceService.listSourcesWithIds(sourceIds) } returns sourcesOnUnsupportedVersions

    val existingPins =
      sourceIds
        .map { sourceId ->
          ScopedConfiguration()
            .withScopeType(ConfigScopeType.ACTOR)
            .withScopeId(sourceId)
        }.toList()

    every {
      mScopedConfigurationService.listScopedConfigurationsWithValues(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        ConfigScopeType.ACTOR,
        ConfigOriginType.BREAKING_CHANGE,
        unsupportedVersionIds.map { it.toString() },
      )
    } returns existingPins

    val workspace1SyncQuery =
      StandardSyncQuery(workspaceId1, listOf(source1.getSourceId(), source2.getSourceId()), null, false)
    val workspace1ActiveSyncIds = listOf(UUID.randomUUID(), UUID.randomUUID())
    every { mWorkspaceService.listWorkspaceActiveSyncIds(workspace1SyncQuery) } returns workspace1ActiveSyncIds

    val workspace2SyncQuery =
      StandardSyncQuery(workspaceId2, listOf(source3.getSourceId()), null, false)
    val workspace2ActiveSyncIds = listOf(UUID.randomUUID())
    every { mWorkspaceService.listWorkspaceActiveSyncIds(workspace2SyncQuery) } returns workspace2ActiveSyncIds

    val workspace3SyncQuery =
      StandardSyncQuery(workspaceId3, listOf(source4.getSourceId()), null, false)
    val workspace3ActiveSyncIds = mutableListOf<UUID>()
    every { mWorkspaceService.listWorkspaceActiveSyncIds(workspace3SyncQuery) } returns workspace3ActiveSyncIds

    val expectedResult =
      Stream
        .of<WorkspaceBreakingChangeInfo?>(
          WorkspaceBreakingChangeInfo(
            workspaceId1,
            workspace1ActiveSyncIds,
            listOf(
              ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(source1.getSourceId()),
              ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(source2.getSourceId()),
            ),
          ),
          WorkspaceBreakingChangeInfo(
            workspaceId2,
            workspace2ActiveSyncIds,
            listOf(ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(source3.getSourceId())),
          ),
        ).sorted(Comparator.comparing<WorkspaceBreakingChangeInfo?, UUID?>(WorkspaceBreakingChangeInfo::workspaceId))
        .toList()

    val result: List<WorkspaceBreakingChangeInfo> =
      breakingChangesHelper.getBreakingActiveSyncsPerWorkspace(ActorType.SOURCE, ACTOR_DEFINITION_ID, unsupportedVersionIds)

    Assertions.assertEquals(2, result.size)

    val sortedResult =
      result.stream().sorted(Comparator.comparing<WorkspaceBreakingChangeInfo?, UUID?>(WorkspaceBreakingChangeInfo::workspaceId)).toList()
    Assertions.assertEquals(expectedResult, sortedResult)

    verify { mSourceService.listSourcesWithIds(sourceIds) }
    verify {
      mScopedConfigurationService.listScopedConfigurationsWithValues(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        ConfigScopeType.ACTOR,
        ConfigOriginType.BREAKING_CHANGE,
        unsupportedVersionIds.stream().map<String?> { obj: UUID? -> obj.toString() }.toList(),
      )
    }
    verify { mWorkspaceService.listWorkspaceActiveSyncIds(workspace1SyncQuery) }
    verify { mWorkspaceService.listWorkspaceActiveSyncIds(workspace2SyncQuery) }
    verify { mWorkspaceService.listWorkspaceActiveSyncIds(workspace3SyncQuery) }
    confirmVerified(mSourceService, mScopedConfigurationService, mWorkspaceService)
  }

  @Test
  fun testGetBreakingActiveSyncsPerWorkspaceWithDestination() {
    val unsupportedVersionId1 = UUID.randomUUID()
    val unsupportedVersionId2 = UUID.randomUUID()

    val unsupportedVersionIds = listOf(unsupportedVersionId1, unsupportedVersionId2)

    val workspaceId1 = UUID.randomUUID()
    val destination1 =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId1)
    val destination2 =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId1)

    val workspaceId2 = UUID.randomUUID()
    val destination3 =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId2)

    val workspaceId3 = UUID.randomUUID()
    val destination4 =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId3)

    val destinationsOnUnsupportedVersions = listOf(destination1, destination2, destination3, destination4)
    val destinationIds = destinationsOnUnsupportedVersions.map { it.getDestinationId() }

    every { mDestinationService.listDestinationsWithIds(destinationIds) } returns destinationsOnUnsupportedVersions

    val existingPins =
      destinationIds
        .stream()
        .map<ScopedConfiguration?> { destinationId: UUID? ->
          ScopedConfiguration()
            .withScopeType(ConfigScopeType.ACTOR)
            .withScopeId(destinationId)
        }.toList()

    every {
      mScopedConfigurationService.listScopedConfigurationsWithValues(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        ConfigScopeType.ACTOR,
        ConfigOriginType.BREAKING_CHANGE,
        unsupportedVersionIds.map { it.toString() },
      )
    } returns existingPins

    val workspace1SyncQuery =
      StandardSyncQuery(
        workspaceId1,
        null,
        listOf(destination1.getDestinationId(), destination2.getDestinationId()),
        false,
      )
    val workspace1ActiveSyncIds = listOf(UUID.randomUUID(), UUID.randomUUID())
    every { mWorkspaceService.listWorkspaceActiveSyncIds(workspace1SyncQuery) } returns workspace1ActiveSyncIds

    val workspace2SyncQuery =
      StandardSyncQuery(
        workspaceId2,
        null,
        listOf(destination3.getDestinationId()),
        false,
      )
    val workspace2ActiveSyncIds = listOf(UUID.randomUUID())
    every { mWorkspaceService.listWorkspaceActiveSyncIds(workspace2SyncQuery) } returns workspace2ActiveSyncIds

    val workspace3SyncQuery =
      StandardSyncQuery(
        workspaceId3,
        null,
        listOf(destination4.getDestinationId()),
        false,
      )
    val workspace3ActiveSyncIds = mutableListOf<UUID>()
    every { mWorkspaceService.listWorkspaceActiveSyncIds(workspace3SyncQuery) } returns workspace3ActiveSyncIds

    val expectedResult =
      Stream
        .of<WorkspaceBreakingChangeInfo?>(
          WorkspaceBreakingChangeInfo(
            workspaceId1,
            workspace1ActiveSyncIds,
            listOf(
              ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(destination1.getDestinationId()),
              ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(destination2.getDestinationId()),
            ),
          ),
          WorkspaceBreakingChangeInfo(
            workspaceId2,
            workspace2ActiveSyncIds,
            listOf(ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(destination3.getDestinationId())),
          ),
        ).sorted(Comparator.comparing<WorkspaceBreakingChangeInfo?, UUID?>(WorkspaceBreakingChangeInfo::workspaceId))
        .toList()

    val result: List<WorkspaceBreakingChangeInfo> =
      breakingChangesHelper.getBreakingActiveSyncsPerWorkspace(ActorType.DESTINATION, ACTOR_DEFINITION_ID, unsupportedVersionIds)

    Assertions.assertEquals(2, result.size)

    val sortedResult =
      result.stream().sorted(Comparator.comparing<WorkspaceBreakingChangeInfo?, UUID?>(WorkspaceBreakingChangeInfo::workspaceId)).toList()
    Assertions.assertEquals(expectedResult, sortedResult)

    verify { mDestinationService.listDestinationsWithIds(destinationIds) }
    verify {
      mScopedConfigurationService.listScopedConfigurationsWithValues(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        ConfigScopeType.ACTOR,
        ConfigOriginType.BREAKING_CHANGE,
        unsupportedVersionIds.stream().map<String?> { obj: UUID? -> obj.toString() }.toList(),
      )
    }
    verify { mWorkspaceService.listWorkspaceActiveSyncIds(workspace1SyncQuery) }
    verify { mWorkspaceService.listWorkspaceActiveSyncIds(workspace2SyncQuery) }
    verify { mWorkspaceService.listWorkspaceActiveSyncIds(workspace3SyncQuery) }
    confirmVerified(mSourceService, mScopedConfigurationService, mWorkspaceService)
  }

  @Test
  fun testGetLastApplicableBreakingChange() {
    val defaultVersion =
      ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withDockerImageTag("2.0.0")

    val firstBreakingChange =
      ActorDefinitionBreakingChange()
        .withVersion(Version("1.0.0"))
    val lastBreakingChange =
      ActorDefinitionBreakingChange()
        .withVersion(Version("2.0.0"))
    val inapplicableBreakingChange =
      ActorDefinitionBreakingChange()
        .withVersion(Version("3.0.0"))
    val breakingChanges = listOf(firstBreakingChange, lastBreakingChange, inapplicableBreakingChange)

    val mActorDefinitionService = mockk<ActorDefinitionService>()
    every { mActorDefinitionService.getActorDefinitionVersion(defaultVersion.getVersionId()) } returns defaultVersion

    val result =
      BreakingChangesHelper.getLastApplicableBreakingChange(mActorDefinitionService, defaultVersion.getVersionId(), breakingChanges)
    Assertions.assertEquals(lastBreakingChange, result)

    verify { mActorDefinitionService.getActorDefinitionVersion(defaultVersion.getVersionId()) }
    confirmVerified(mActorDefinitionService)
  }

  companion object {
    private val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
  }
}
