/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.model.generated.ActorCatalogWithUpdatedAt;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.CatalogDiff;
import io.airbyte.api.client.model.generated.ConnectionAutoPropagateResult;
import io.airbyte.api.client.model.generated.ConnectionAutoPropagateSchemaChange;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.JobConfigType;
import io.airbyte.api.client.model.generated.SourceAutoPropagateChange;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.StreamTransform;
import io.airbyte.api.client.model.generated.SynchronousJobRead;
import io.airbyte.api.client.model.generated.WorkloadPriority;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.utils.PayloadChecker;
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
import io.airbyte.workers.models.RefreshSchemaActivityOutput;
import io.airbyte.workers.temporal.sync.RefreshSchemaActivityImpl;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
class RefreshSchemaActivityTest {

  private AirbyteApiClient mAirbyteApiClient;
  private SourceApi mSourceApi;
  private ConnectionApi mConnectionApi;
  private WorkspaceApi mWorkspaceApi;
  private EnvVariableFeatureFlags mEnvVariableFeatureFlags;
  private TestClient mFeatureFlagClient;
  private PayloadChecker mPayloadChecker;

  private RefreshSchemaActivityImpl refreshSchemaActivity;

  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID CATALOG_ID = UUID.randomUUID();
  private static final UUID SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final AirbyteCatalog CATALOG =
      new AirbyteCatalog(List.of(new AirbyteStreamAndConfiguration(new AirbyteStream("test stream", null, null, null, null, null, null), null)));
  private static final CatalogDiff CATALOG_DIFF =
      new CatalogDiff(List.of(new StreamTransform(StreamTransform.TransformType.UPDATE_STREAM, new StreamDescriptor("test stream", null), null)));

  @BeforeEach
  void setUp() throws IOException {
    mAirbyteApiClient = mock(AirbyteApiClient.class);
    mEnvVariableFeatureFlags = mock(EnvVariableFeatureFlags.class);
    mSourceApi = mock(SourceApi.class, withSettings().strictness(Strictness.LENIENT));
    mConnectionApi = mock(ConnectionApi.class);
    mFeatureFlagClient = mock(TestClient.class, withSettings().strictness(Strictness.LENIENT));
    mWorkspaceApi = mock(WorkspaceApi.class, withSettings().strictness(Strictness.LENIENT));
    mPayloadChecker = mock(PayloadChecker.class, withSettings().strictness(Strictness.LENIENT));
    when(mEnvVariableFeatureFlags.autoDetectSchema()).thenReturn(true);
    when(mWorkspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody(CONNECTION_ID)))
        .thenReturn(new WorkspaceRead(WORKSPACE_ID, UUID.randomUUID(), "name", "slug", false, UUID.randomUUID(), null, null, null, null, null, null,
            null, null, null, null, null, null));
    when(mSourceApi.getSource(new SourceIdRequestBody(SOURCE_ID))).thenReturn(
        new SourceRead(SOURCE_DEFINITION_ID, SOURCE_ID, WORKSPACE_ID, Jsons.jsonNode(Map.of()), "name", "source-name", null, null, null, null));
    when(mSourceApi.discoverSchemaForSource(
        new SourceDiscoverSchemaRequestBody(SOURCE_ID, CONNECTION_ID, true, true, WorkloadPriority.DEFAULT)))
            .thenReturn(
                new SourceDiscoverSchemaRead(
                    new SynchronousJobRead(
                        UUID.randomUUID(),
                        JobConfigType.REFRESH,
                        System.currentTimeMillis(),
                        System.currentTimeMillis(),
                        true,
                        null,

                        false,
                        null,
                        null),
                    CATALOG,
                    CATALOG_ID,
                    CATALOG_DIFF,
                    false,
                    null));
    when(mAirbyteApiClient.getSourceApi()).thenReturn(mSourceApi);
    refreshSchemaActivity =
        new RefreshSchemaActivityImpl(mAirbyteApiClient, mEnvVariableFeatureFlags, mFeatureFlagClient, mPayloadChecker);
  }

  @Test
  void testShouldRefreshSchemaNoRecentRefresh() throws IOException {
    when(mSourceApi.getMostRecentSourceActorCatalog(any())).thenReturn(new ActorCatalogWithUpdatedAt());
    Assertions.assertThat(true).isEqualTo(refreshSchemaActivity.shouldRefreshSchema(SOURCE_ID));
  }

  @Test
  void testShouldRefreshSchemaRecentRefreshOver24HoursAgo() throws IOException {
    final Long twoDaysAgo = OffsetDateTime.now().minusHours(48L).toEpochSecond();
    final ActorCatalogWithUpdatedAt actorCatalogWithUpdatedAt = new ActorCatalogWithUpdatedAt(twoDaysAgo, null);

    when(mSourceApi.getMostRecentSourceActorCatalog(any())).thenReturn(actorCatalogWithUpdatedAt);
    when(mSourceApi.getSource(any())).thenReturn(
        new SourceRead(SOURCE_DEFINITION_ID, SOURCE_ID, WORKSPACE_ID, Jsons.jsonNode(Map.of()), "name", "source-name", null, null, null, null));
    when(mFeatureFlagClient.intVariation(RefreshSchemaPeriod.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(24);

    Assertions.assertThat(true).isEqualTo(refreshSchemaActivity.shouldRefreshSchema(SOURCE_ID));
  }

  @Test
  void testShouldRefreshSchemaRecentRefreshLessThan24HoursAgo() throws IOException {
    final Long twelveHoursAgo = OffsetDateTime.now().minusHours(12L).toEpochSecond();
    final ActorCatalogWithUpdatedAt actorCatalogWithUpdatedAt = new ActorCatalogWithUpdatedAt(twelveHoursAgo, null);

    when(mSourceApi.getSource(any())).thenReturn(
        new SourceRead(SOURCE_DEFINITION_ID, SOURCE_ID, WORKSPACE_ID, Jsons.jsonNode(Map.of()), "name", "source-name", null, null, null, null));
    when(mFeatureFlagClient.intVariation(RefreshSchemaPeriod.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(24);
    when(mSourceApi.getMostRecentSourceActorCatalog(any())).thenReturn(actorCatalogWithUpdatedAt);

    Assertions.assertThat(false).isEqualTo(refreshSchemaActivity.shouldRefreshSchema(SOURCE_ID));
  }

  @Test
  void testShouldRefreshSchemaRecentRefreshLessThanValueFromFF() throws IOException {
    final Long twelveHoursAgo = OffsetDateTime.now().minusHours(12L).toEpochSecond();
    final ActorCatalogWithUpdatedAt actorCatalogWithUpdatedAt = new ActorCatalogWithUpdatedAt(twelveHoursAgo, null);

    when(mSourceApi.getSource(any())).thenReturn(
        new SourceRead(SOURCE_DEFINITION_ID, SOURCE_ID, WORKSPACE_ID, Jsons.jsonNode(Map.of()), "name", "source-name", null, null, null, null));
    when(mFeatureFlagClient.intVariation(RefreshSchemaPeriod.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(10);
    when(mSourceApi.getMostRecentSourceActorCatalog(any())).thenReturn(actorCatalogWithUpdatedAt);

    Assertions.assertThat(true).isEqualTo(refreshSchemaActivity.shouldRefreshSchema(SOURCE_ID));
  }

  @Test
  void testRefreshSchema() throws IOException {
    final List<Context> expectedRefreshFeatureFlagContexts = List.of(new SourceDefinition(SOURCE_DEFINITION_ID), new Connection(CONNECTION_ID));

    when(mAirbyteApiClient.getWorkspaceApi()).thenReturn(mWorkspaceApi);
    when(mFeatureFlagClient.boolVariation(ShouldRunRefreshSchema.INSTANCE, new Multi(expectedRefreshFeatureFlagContexts))).thenReturn(true);

    refreshSchemaActivity.refreshSchema(SOURCE_ID, CONNECTION_ID);

    verify(mSourceApi).discoverSchemaForSource(
        new SourceDiscoverSchemaRequestBody(SOURCE_ID, CONNECTION_ID, true, true, WorkloadPriority.DEFAULT));
    verify(mWorkspaceApi).getWorkspaceByConnectionId(new ConnectionIdRequestBody(CONNECTION_ID));
    verify(mSourceApi).applySchemaChangeForSource(new SourceAutoPropagateChange(CATALOG, CATALOG_ID, SOURCE_ID, WORKSPACE_ID));
  }

  @Test
  void testRefreshSchemaWithRefreshSchemaFeatureFlagAsFalse() throws IOException {
    final UUID sourceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();
    final List<Context> expectedRefreshFeatureFlagContexts = List.of(new SourceDefinition(sourceDefinitionId), new Connection(connectionId));

    when(mSourceApi.getSource(new SourceIdRequestBody(sourceId))).thenReturn(
        new SourceRead(sourceDefinitionId, SOURCE_ID, WORKSPACE_ID, Jsons.jsonNode(Map.of()), "name", "source-name", null, null, null, null));
    when(mFeatureFlagClient.boolVariation(ShouldRunRefreshSchema.INSTANCE, new Multi(expectedRefreshFeatureFlagContexts))).thenReturn(false);

    refreshSchemaActivity.refreshSchema(sourceId, connectionId);

    verify(mSourceApi, times(0)).discoverSchemaForSource(any());
    verify(mSourceApi, times(0)).applySchemaChangeForSource(any());
  }

  @Test
  void testRefreshSchemaWithAutoPropagateFeatureFlagAsFalse() throws IOException {
    final UUID sourceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();
    final List<Context> expectedRefreshFeatureFlagContexts = List.of(new SourceDefinition(sourceDefinitionId), new Connection(connectionId));

    when(mSourceApi.getSource(new SourceIdRequestBody(sourceId))).thenReturn(
        new SourceRead(sourceDefinitionId, SOURCE_ID, WORKSPACE_ID, Jsons.jsonNode(Map.of()), "name", "source-name", null, null, null, null));
    when(mFeatureFlagClient.boolVariation(ShouldRunRefreshSchema.INSTANCE, new Multi(expectedRefreshFeatureFlagContexts))).thenReturn(true);
    when(mWorkspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody(connectionId)))
        .thenReturn(new WorkspaceRead(workspaceId, UUID.randomUUID(), "name", "slug", false, UUID.randomUUID(), null, null, null, null, null, null,
            null, null, null, null, null, null));

    refreshSchemaActivity.refreshSchema(sourceId, connectionId);

    verify(mSourceApi, times(0)).applySchemaChangeForSource(any());
  }

  @Test
  void testRefreshSchemaForAutoBackfillOnNewColumns() throws IOException {
    // Test the version of schema refresh that will be used when we want to run backfills.

    when(mAirbyteApiClient.getConnectionApi()).thenReturn(mConnectionApi);
    when(mFeatureFlagClient.boolVariation(eq(ShouldRunRefreshSchema.INSTANCE), any())).thenReturn(true);
    when(mFeatureFlagClient.boolVariation(eq(AutoBackfillOnNewColumns.INSTANCE), any())).thenReturn(true);

    when(mConnectionApi.applySchemaChangeForConnection(new ConnectionAutoPropagateSchemaChange(CATALOG, CATALOG_ID, CONNECTION_ID, WORKSPACE_ID)))
        .thenReturn(new ConnectionAutoPropagateResult(CATALOG_DIFF));

    final var result = refreshSchemaActivity.refreshSchemaV2(new RefreshSchemaActivityInput(SOURCE_ID, CONNECTION_ID, WORKSPACE_ID));

    verify(mSourceApi, times(0)).applySchemaChangeForSource(any());
    verify(mConnectionApi, times(1))
        .applySchemaChangeForConnection(new ConnectionAutoPropagateSchemaChange(CATALOG, CATALOG_ID, CONNECTION_ID, WORKSPACE_ID));
    assertEquals(CATALOG_DIFF, result.getAppliedDiff());
  }

  @Test
  void refreshV2ValidatesPayloadSize() throws IOException {
    when(mAirbyteApiClient.getConnectionApi()).thenReturn(mConnectionApi);
    when(mFeatureFlagClient.boolVariation(eq(ShouldRunRefreshSchema.INSTANCE), any())).thenReturn(true);
    when(mFeatureFlagClient.boolVariation(eq(AutoBackfillOnNewColumns.INSTANCE), any())).thenReturn(true);
    when(mConnectionApi.applySchemaChangeForConnection(new ConnectionAutoPropagateSchemaChange(CATALOG, CATALOG_ID, CONNECTION_ID, WORKSPACE_ID)))
        .thenReturn(new ConnectionAutoPropagateResult(CATALOG_DIFF));

    refreshSchemaActivity.refreshSchemaV2(new RefreshSchemaActivityInput(SOURCE_ID, CONNECTION_ID, WORKSPACE_ID));

    verify(mPayloadChecker, times(1)).validatePayloadSize(eq(new RefreshSchemaActivityOutput(CATALOG_DIFF)), any());
  }

}
