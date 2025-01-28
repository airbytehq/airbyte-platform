/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConfigOriginType;
import io.airbyte.config.ConfigResourceType;
import io.airbyte.config.ConfigScopeType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.ScopedConfiguration;
import io.airbyte.config.SourceConnection;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.shared.ConnectorVersionKey;
import io.airbyte.data.services.shared.StandardSyncQuery;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BreakingChangesHelperTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();

  private WorkspaceService mWorkspaceService;
  private ScopedConfigurationService mScopedConfigurationService;
  private SourceService mSourceService;
  private DestinationService mDestinationService;

  private BreakingChangesHelper breakingChangesHelper;

  @BeforeEach
  void setup() {
    mWorkspaceService = mock(WorkspaceService.class);
    mScopedConfigurationService = mock(ScopedConfigurationService.class);
    mSourceService = mock(SourceService.class);
    mDestinationService = mock(DestinationService.class);

    breakingChangesHelper = new BreakingChangesHelper(mScopedConfigurationService, mWorkspaceService, mDestinationService, mSourceService);
  }

  @Test
  void testGetBreakingActiveSyncsPerWorkspaceWithSource() throws IOException {
    final UUID unsupportedVersionId1 = UUID.randomUUID();
    final UUID unsupportedVersionId2 = UUID.randomUUID();

    final List<UUID> unsupportedVersionIds = List.of(unsupportedVersionId1, unsupportedVersionId2);

    final UUID workspaceId1 = UUID.randomUUID();
    final SourceConnection source1 = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(workspaceId1);
    final SourceConnection source2 = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(workspaceId1);

    final UUID workspaceId2 = UUID.randomUUID();
    final SourceConnection source3 = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(workspaceId2);

    final UUID workspaceId3 = UUID.randomUUID();
    final SourceConnection source4 = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(workspaceId3);

    final List<SourceConnection> sourcesOnUnsupportedVersions = List.of(source1, source2, source3, source4);
    final List<UUID> sourceIds = sourcesOnUnsupportedVersions.stream().map(SourceConnection::getSourceId).toList();

    when(mSourceService.listSourcesWithIds(sourceIds)).thenReturn(sourcesOnUnsupportedVersions);

    final List<ScopedConfiguration> existingPins = sourceIds.stream().map(sourceId -> new ScopedConfiguration()
        .withScopeType(ConfigScopeType.ACTOR)
        .withScopeId(sourceId)).toList();

    when(mScopedConfigurationService.listScopedConfigurationsWithValues(ConnectorVersionKey.INSTANCE.getKey(), ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID, ConfigScopeType.ACTOR, ConfigOriginType.BREAKING_CHANGE, unsupportedVersionIds.stream().map(UUID::toString).toList()))
            .thenReturn(existingPins);

    final StandardSyncQuery workspace1SyncQuery =
        new StandardSyncQuery(workspaceId1, List.of(source1.getSourceId(), source2.getSourceId()), null, false);
    final List<UUID> workspace1ActiveSyncIds = List.of(UUID.randomUUID(), UUID.randomUUID());
    when(mWorkspaceService.listWorkspaceActiveSyncIds(workspace1SyncQuery)).thenReturn(workspace1ActiveSyncIds);

    final StandardSyncQuery workspace2SyncQuery =
        new StandardSyncQuery(workspaceId2, List.of(source3.getSourceId()), null, false);
    final List<UUID> workspace2ActiveSyncIds = List.of(UUID.randomUUID());
    when(mWorkspaceService.listWorkspaceActiveSyncIds(workspace2SyncQuery)).thenReturn(workspace2ActiveSyncIds);

    final StandardSyncQuery workspace3SyncQuery =
        new StandardSyncQuery(workspaceId3, List.of(source4.getSourceId()), null, false);
    final List<UUID> workspace3ActiveSyncIds = List.of();
    when(mWorkspaceService.listWorkspaceActiveSyncIds(workspace3SyncQuery)).thenReturn(workspace3ActiveSyncIds);

    final List<BreakingChangesHelper.WorkspaceBreakingChangeInfo> expectedResult = Stream.of(
        new BreakingChangesHelper.WorkspaceBreakingChangeInfo(workspaceId1, workspace1ActiveSyncIds,
            List.of(new ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(source1.getSourceId()),
                new ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(source2.getSourceId()))),
        new BreakingChangesHelper.WorkspaceBreakingChangeInfo(workspaceId2, workspace2ActiveSyncIds,
            List.of(new ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(source3.getSourceId()))))
        .sorted(Comparator.comparing(BreakingChangesHelper.WorkspaceBreakingChangeInfo::workspaceId)).toList();

    final List<BreakingChangesHelper.WorkspaceBreakingChangeInfo> result =
        breakingChangesHelper.getBreakingActiveSyncsPerWorkspace(ActorType.SOURCE, ACTOR_DEFINITION_ID, unsupportedVersionIds);

    assertEquals(2, result.size());

    final List<BreakingChangesHelper.WorkspaceBreakingChangeInfo> sortedResult =
        result.stream().sorted(Comparator.comparing(BreakingChangesHelper.WorkspaceBreakingChangeInfo::workspaceId)).toList();
    assertEquals(expectedResult, sortedResult);

    verify(mSourceService).listSourcesWithIds(sourceIds);
    verify(mScopedConfigurationService).listScopedConfigurationsWithValues(ConnectorVersionKey.INSTANCE.getKey(), ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID, ConfigScopeType.ACTOR, ConfigOriginType.BREAKING_CHANGE, unsupportedVersionIds.stream().map(UUID::toString).toList());
    verify(mWorkspaceService).listWorkspaceActiveSyncIds(workspace1SyncQuery);
    verify(mWorkspaceService).listWorkspaceActiveSyncIds(workspace2SyncQuery);
    verify(mWorkspaceService).listWorkspaceActiveSyncIds(workspace3SyncQuery);
    verifyNoMoreInteractions(mSourceService, mScopedConfigurationService, mWorkspaceService);
  }

  @Test
  void testGetBreakingActiveSyncsPerWorkspaceWithDestination() throws IOException {
    final UUID unsupportedVersionId1 = UUID.randomUUID();
    final UUID unsupportedVersionId2 = UUID.randomUUID();

    final List<UUID> unsupportedVersionIds = List.of(unsupportedVersionId1, unsupportedVersionId2);

    final UUID workspaceId1 = UUID.randomUUID();
    final DestinationConnection destination1 = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId1);
    final DestinationConnection destination2 = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId1);

    final UUID workspaceId2 = UUID.randomUUID();
    final DestinationConnection destination3 = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId2);

    final UUID workspaceId3 = UUID.randomUUID();
    final DestinationConnection destination4 = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId3);

    final List<DestinationConnection> destinationsOnUnsupportedVersions = List.of(destination1, destination2, destination3, destination4);
    final List<UUID> destinationIds = destinationsOnUnsupportedVersions.stream().map(DestinationConnection::getDestinationId).toList();

    when(mDestinationService.listDestinationsWithIds(destinationIds)).thenReturn(destinationsOnUnsupportedVersions);

    final List<ScopedConfiguration> existingPins = destinationIds.stream().map(destinationId -> new ScopedConfiguration()
        .withScopeType(ConfigScopeType.ACTOR)
        .withScopeId(destinationId)).toList();

    when(mScopedConfigurationService.listScopedConfigurationsWithValues(ConnectorVersionKey.INSTANCE.getKey(), ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID, ConfigScopeType.ACTOR, ConfigOriginType.BREAKING_CHANGE, unsupportedVersionIds.stream().map(UUID::toString).toList()))
            .thenReturn(existingPins);

    final StandardSyncQuery workspace1SyncQuery =
        new StandardSyncQuery(workspaceId1, null,
            List.of(destination1.getDestinationId(), destination2.getDestinationId()), false);
    final List<UUID> workspace1ActiveSyncIds = List.of(UUID.randomUUID(), UUID.randomUUID());
    when(mWorkspaceService.listWorkspaceActiveSyncIds(workspace1SyncQuery)).thenReturn(workspace1ActiveSyncIds);

    final StandardSyncQuery workspace2SyncQuery = new StandardSyncQuery(workspaceId2, null,
        List.of(destination3.getDestinationId()), false);
    final List<UUID> workspace2ActiveSyncIds = List.of(UUID.randomUUID());
    when(mWorkspaceService.listWorkspaceActiveSyncIds(workspace2SyncQuery)).thenReturn(workspace2ActiveSyncIds);

    final StandardSyncQuery workspace3SyncQuery = new StandardSyncQuery(workspaceId3, null,
        List.of(destination4.getDestinationId()), false);
    final List<UUID> workspace3ActiveSyncIds = List.of();
    when(mWorkspaceService.listWorkspaceActiveSyncIds(workspace3SyncQuery)).thenReturn(workspace3ActiveSyncIds);

    final List<BreakingChangesHelper.WorkspaceBreakingChangeInfo> expectedResult = Stream.of(
        new BreakingChangesHelper.WorkspaceBreakingChangeInfo(workspaceId1, workspace1ActiveSyncIds,
            List.of(new ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(destination1.getDestinationId()),
                new ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(destination2.getDestinationId()))),
        new BreakingChangesHelper.WorkspaceBreakingChangeInfo(workspaceId2, workspace2ActiveSyncIds,
            List.of(new ScopedConfiguration().withScopeType(ConfigScopeType.ACTOR).withScopeId(destination3.getDestinationId()))))
        .sorted(Comparator.comparing(BreakingChangesHelper.WorkspaceBreakingChangeInfo::workspaceId)).toList();

    final List<BreakingChangesHelper.WorkspaceBreakingChangeInfo> result =
        breakingChangesHelper.getBreakingActiveSyncsPerWorkspace(ActorType.DESTINATION, ACTOR_DEFINITION_ID, unsupportedVersionIds);

    assertEquals(2, result.size());

    final List<BreakingChangesHelper.WorkspaceBreakingChangeInfo> sortedResult =
        result.stream().sorted(Comparator.comparing(BreakingChangesHelper.WorkspaceBreakingChangeInfo::workspaceId)).toList();
    assertEquals(expectedResult, sortedResult);

    verify(mDestinationService).listDestinationsWithIds(destinationIds);
    verify(mScopedConfigurationService).listScopedConfigurationsWithValues(ConnectorVersionKey.INSTANCE.getKey(), ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID, ConfigScopeType.ACTOR, ConfigOriginType.BREAKING_CHANGE, unsupportedVersionIds.stream().map(UUID::toString).toList());
    verify(mWorkspaceService).listWorkspaceActiveSyncIds(workspace1SyncQuery);
    verify(mWorkspaceService).listWorkspaceActiveSyncIds(workspace2SyncQuery);
    verify(mWorkspaceService).listWorkspaceActiveSyncIds(workspace3SyncQuery);
    verifyNoMoreInteractions(mSourceService, mScopedConfigurationService, mWorkspaceService);
  }

  @Test
  void testGetLastApplicableBreakingChange() throws ConfigNotFoundException, IOException {
    final ActorDefinitionVersion defaultVersion = new ActorDefinitionVersion()
        .withDockerImageTag("2.0.0");

    final ActorDefinitionBreakingChange firstBreakingChange = new ActorDefinitionBreakingChange()
        .withVersion(new Version("1.0.0"));
    final ActorDefinitionBreakingChange lastBreakingChange = new ActorDefinitionBreakingChange()
        .withVersion(new Version("2.0.0"));
    final ActorDefinitionBreakingChange inapplicableBreakingChange = new ActorDefinitionBreakingChange()
        .withVersion(new Version("3.0.0"));
    final List<ActorDefinitionBreakingChange> breakingChanges = List.of(firstBreakingChange, lastBreakingChange, inapplicableBreakingChange);

    final ActorDefinitionService mActorDefinitionService = mock(ActorDefinitionService.class);
    when(mActorDefinitionService.getActorDefinitionVersion(defaultVersion.getVersionId()))
        .thenReturn(defaultVersion);

    final ActorDefinitionBreakingChange result =
        BreakingChangesHelper.getLastApplicableBreakingChange(mActorDefinitionService, defaultVersion.getVersionId(), breakingChanges);
    assertEquals(lastBreakingChange, result);

    verify(mActorDefinitionService).getActorDefinitionVersion(defaultVersion.getVersionId());
    verifyNoMoreInteractions(mActorDefinitionService);
  }

}
