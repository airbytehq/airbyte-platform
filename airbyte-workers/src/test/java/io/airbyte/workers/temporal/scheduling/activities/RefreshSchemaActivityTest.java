/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ActorCatalogWithUpdatedAt;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.CatalogDiff;
import io.airbyte.api.client.model.generated.ConnectionAutoPropagateResult;
import io.airbyte.api.client.model.generated.ConnectionAutoPropagateSchemaChange;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceAutoPropagateChange;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.StreamTransform;
import io.airbyte.api.client.model.generated.SynchronousJobRead;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.featureflag.AutoBackfillOnNewColumns;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.RefreshSchemaPeriod;
import io.airbyte.featureflag.ShouldRunRefreshSchema;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.workers.models.RefreshSchemaActivityInput;
import io.airbyte.workers.temporal.sync.RefreshSchemaActivityImpl;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
class RefreshSchemaActivityTest {

  private SourceApi mSourceApi;
  private ConnectionApi mConnectionApi;
  private WorkspaceApi mWorkspaceApi;
  private EnvVariableFeatureFlags mEnvVariableFeatureFlags;
  private TestClient mFeatureFlagClient;

  private RefreshSchemaActivityImpl refreshSchemaActivity;

  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID CATALOG_ID = UUID.randomUUID();
  private static final UUID SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final AirbyteCatalog CATALOG = new AirbyteCatalog()
      .addStreamsItem(new AirbyteStreamAndConfiguration()
          .stream(new AirbyteStream().name("test stream")));

  private static final CatalogDiff CATALOG_DIFF = new CatalogDiff()
      .addTransformsItem(new StreamTransform()
          .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
          .streamDescriptor(new StreamDescriptor().name("test stream")));

  @BeforeEach
  void setUp() throws ApiException {
    mSourceApi = mock(SourceApi.class);
    mEnvVariableFeatureFlags = mock(EnvVariableFeatureFlags.class);
    mSourceApi = mock(SourceApi.class, withSettings().strictness(Strictness.LENIENT));
    mConnectionApi = mock(ConnectionApi.class);
    mFeatureFlagClient = mock(TestClient.class, withSettings().strictness(Strictness.LENIENT));
    mWorkspaceApi = mock(WorkspaceApi.class, withSettings().strictness(Strictness.LENIENT));
    when(mEnvVariableFeatureFlags.autoDetectSchema()).thenReturn(true);
    when(mWorkspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(CONNECTION_ID)))
        .thenReturn(new WorkspaceRead().workspaceId(WORKSPACE_ID));
    when(mSourceApi.getSource(new SourceIdRequestBody().sourceId(SOURCE_ID))).thenReturn(new SourceRead().sourceDefinitionId(SOURCE_DEFINITION_ID));
    when(mSourceApi.discoverSchemaForSource(
        new SourceDiscoverSchemaRequestBody().sourceId(SOURCE_ID).disableCache(true).connectionId(CONNECTION_ID).notifySchemaChange(true)))
            .thenReturn(new SourceDiscoverSchemaRead()
                .breakingChange(false)
                .catalog(CATALOG)
                .catalogDiff(CATALOG_DIFF)
                .catalogId(CATALOG_ID)
                .jobInfo(new SynchronousJobRead().succeeded(true)));
    refreshSchemaActivity = new RefreshSchemaActivityImpl(mSourceApi, mConnectionApi, mWorkspaceApi, mEnvVariableFeatureFlags, mFeatureFlagClient);
  }

  @Test
  void testShouldRefreshSchemaNoRecentRefresh() throws ApiException {
    when(mSourceApi.getMostRecentSourceActorCatalog(any())).thenReturn(new ActorCatalogWithUpdatedAt());
    Assertions.assertThat(true).isEqualTo(refreshSchemaActivity.shouldRefreshSchema(SOURCE_ID));
  }

  @Test
  void testShouldRefreshSchemaRecentRefreshOver24HoursAgo() throws ApiException {
    final Long twoDaysAgo = OffsetDateTime.now().minusHours(48L).toEpochSecond();
    final ActorCatalogWithUpdatedAt actorCatalogWithUpdatedAt = new ActorCatalogWithUpdatedAt().updatedAt(twoDaysAgo);

    when(mSourceApi.getMostRecentSourceActorCatalog(any())).thenReturn(actorCatalogWithUpdatedAt);
    when(mSourceApi.getSource(any())).thenReturn(new SourceRead().workspaceId(WORKSPACE_ID));
    when(mFeatureFlagClient.intVariation(RefreshSchemaPeriod.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(24);

    Assertions.assertThat(true).isEqualTo(refreshSchemaActivity.shouldRefreshSchema(SOURCE_ID));
  }

  @Test
  void testShouldRefreshSchemaRecentRefreshLessThan24HoursAgo() throws ApiException {
    final Long twelveHoursAgo = OffsetDateTime.now().minusHours(12L).toEpochSecond();
    final ActorCatalogWithUpdatedAt actorCatalogWithUpdatedAt = new ActorCatalogWithUpdatedAt().updatedAt(twelveHoursAgo);

    when(mSourceApi.getSource(any())).thenReturn(new SourceRead().workspaceId(WORKSPACE_ID));
    when(mFeatureFlagClient.intVariation(RefreshSchemaPeriod.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(24);
    when(mSourceApi.getMostRecentSourceActorCatalog(any())).thenReturn(actorCatalogWithUpdatedAt);

    Assertions.assertThat(false).isEqualTo(refreshSchemaActivity.shouldRefreshSchema(SOURCE_ID));
  }

  @Test
  void testShouldRefreshSchemaRecentRefreshLessThanValueFromFF() throws ApiException {
    final Long twelveHoursAgo = OffsetDateTime.now().minusHours(12L).toEpochSecond();
    final ActorCatalogWithUpdatedAt actorCatalogWithUpdatedAt = new ActorCatalogWithUpdatedAt().updatedAt(twelveHoursAgo);

    when(mSourceApi.getSource(any())).thenReturn(new SourceRead().workspaceId(WORKSPACE_ID));
    when(mFeatureFlagClient.intVariation(RefreshSchemaPeriod.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(10);
    when(mSourceApi.getMostRecentSourceActorCatalog(any())).thenReturn(actorCatalogWithUpdatedAt);

    Assertions.assertThat(true).isEqualTo(refreshSchemaActivity.shouldRefreshSchema(SOURCE_ID));
  }

  @Test
  void testRefreshSchema() throws Exception {
    final List<Context> expectedRefreshFeatureFlagContexts = List.of(new SourceDefinition(SOURCE_DEFINITION_ID), new Connection(CONNECTION_ID));

    when(mFeatureFlagClient.boolVariation(ShouldRunRefreshSchema.INSTANCE, new Multi(expectedRefreshFeatureFlagContexts))).thenReturn(true);

    refreshSchemaActivity.refreshSchema(SOURCE_ID, CONNECTION_ID);

    verify(mSourceApi).discoverSchemaForSource(
        new SourceDiscoverSchemaRequestBody().sourceId(SOURCE_ID).disableCache(true).connectionId(CONNECTION_ID).notifySchemaChange(true));
    verify(mWorkspaceApi).getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(CONNECTION_ID));
    verify(mSourceApi).applySchemaChangeForSource(new SourceAutoPropagateChange()
        .catalogId(CATALOG_ID)
        .sourceId(SOURCE_ID)
        .catalog(CATALOG)
        .workspaceId(WORKSPACE_ID));
  }

  @Test
  void testRefreshSchemaWithRefreshSchemaFeatureFlagAsFalse() throws Exception {
    final UUID sourceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();
    final List<Context> expectedRefreshFeatureFlagContexts = List.of(new SourceDefinition(sourceDefinitionId), new Connection(connectionId));

    when(mSourceApi.getSource(new SourceIdRequestBody().sourceId(sourceId))).thenReturn(new SourceRead().sourceDefinitionId(sourceDefinitionId));
    when(mFeatureFlagClient.boolVariation(ShouldRunRefreshSchema.INSTANCE, new Multi(expectedRefreshFeatureFlagContexts))).thenReturn(false);

    refreshSchemaActivity.refreshSchema(sourceId, connectionId);

    verify(mSourceApi, times(0)).discoverSchemaForSource(any());
    verify(mSourceApi, times(0)).applySchemaChangeForSource(any());
  }

  @Test
  void testRefreshSchemaWithAutoPropagateFeatureFlagAsFalse() throws Exception {
    final UUID sourceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();
    final List<Context> expectedRefreshFeatureFlagContexts = List.of(new SourceDefinition(sourceDefinitionId), new Connection(connectionId));

    when(mSourceApi.getSource(new SourceIdRequestBody().sourceId(sourceId))).thenReturn(new SourceRead().sourceDefinitionId(sourceDefinitionId));
    when(mFeatureFlagClient.boolVariation(ShouldRunRefreshSchema.INSTANCE, new Multi(expectedRefreshFeatureFlagContexts))).thenReturn(true);
    when(mWorkspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(connectionId)))
        .thenReturn(new WorkspaceRead().workspaceId(workspaceId));

    refreshSchemaActivity.refreshSchema(sourceId, connectionId);

    verify(mSourceApi, times(0)).applySchemaChangeForSource(any());
  }

  @Test
  void testRefreshSchemaForAutoBackfillOnNewColumns() throws Exception {
    // Test the version of schema refresh that will be used when we want to run backfills.

    when(mFeatureFlagClient.boolVariation(eq(ShouldRunRefreshSchema.INSTANCE), any())).thenReturn(true);
    when(mFeatureFlagClient.boolVariation(eq(AutoBackfillOnNewColumns.INSTANCE), any())).thenReturn(true);

    when(mConnectionApi.applySchemaChangeForConnection(new ConnectionAutoPropagateSchemaChange()
        .connectionId(CONNECTION_ID)
        .catalogId(CATALOG_ID)
        .catalog(CATALOG)
        .workspaceId(WORKSPACE_ID))).thenReturn(new ConnectionAutoPropagateResult()
            .propagatedDiff(CATALOG_DIFF));

    final var result = refreshSchemaActivity.refreshSchemaV2(new RefreshSchemaActivityInput(SOURCE_ID, CONNECTION_ID, WORKSPACE_ID));

    verify(mSourceApi, times(0)).applySchemaChangeForSource(any());
    verify(mConnectionApi, times(1)).applySchemaChangeForConnection(new ConnectionAutoPropagateSchemaChange()
        .connectionId(CONNECTION_ID)
        .workspaceId(WORKSPACE_ID)
        .catalogId(CATALOG_ID)
        .catalog(CATALOG));
    assertEquals(CATALOG_DIFF, result.getAppliedDiff());
  }

}
