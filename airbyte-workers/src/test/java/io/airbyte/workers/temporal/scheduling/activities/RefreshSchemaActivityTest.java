/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ActorCatalogWithUpdatedAt;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.CatalogDiff;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceAutoPropagateChange;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.StreamTransform;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.featureflag.AutoPropagateSchema;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.ShouldRunRefreshSchema;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.workers.temporal.sync.RefreshSchemaActivityImpl;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RefreshSchemaActivityTest {

  private SourceApi mSourceApi;
  private WorkspaceApi mWorkspaceApi;
  private EnvVariableFeatureFlags mEnvVariableFeatureFlags;
  private TestClient mFeatureFlagClient;

  private RefreshSchemaActivityImpl refreshSchemaActivity;

  private static final UUID SOURCE_ID = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    mSourceApi = mock(SourceApi.class);
    mEnvVariableFeatureFlags = mock(EnvVariableFeatureFlags.class);
    mSourceApi = mock(SourceApi.class);
    mFeatureFlagClient = mock(TestClient.class);
    mWorkspaceApi = mock(WorkspaceApi.class);
    when(mEnvVariableFeatureFlags.autoDetectSchema()).thenReturn(true);
    refreshSchemaActivity = new RefreshSchemaActivityImpl(mSourceApi, mWorkspaceApi, mEnvVariableFeatureFlags, mFeatureFlagClient);
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
    Assertions.assertThat(true).isEqualTo(refreshSchemaActivity.shouldRefreshSchema(SOURCE_ID));
  }

  @Test
  void testShouldRefreshSchemaRecentRefreshLessThan24HoursAgo() throws ApiException {
    final Long twelveHoursAgo = OffsetDateTime.now().minusHours(12L).toEpochSecond();
    final ActorCatalogWithUpdatedAt actorCatalogWithUpdatedAt = new ActorCatalogWithUpdatedAt().updatedAt(twelveHoursAgo);
    when(mSourceApi.getMostRecentSourceActorCatalog(any())).thenReturn(actorCatalogWithUpdatedAt);
    Assertions.assertThat(false).isEqualTo(refreshSchemaActivity.shouldRefreshSchema(SOURCE_ID));
  }

  @Test
  void testRefreshSchema() throws ApiException {
    final UUID sourceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final UUID catalogId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final AirbyteCatalog catalog = new AirbyteCatalog()
        .addStreamsItem(new AirbyteStreamAndConfiguration()
            .stream(new AirbyteStream().name("test stream")));
    final CatalogDiff catalogDiff = new CatalogDiff()
        .addTransformsItem(new StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(new StreamDescriptor().name("test stream")));

    when(mFeatureFlagClient.boolVariation(ShouldRunRefreshSchema.INSTANCE, new Connection(connectionId))).thenReturn(true);
    when(mFeatureFlagClient.boolVariation(AutoPropagateSchema.INSTANCE, new Workspace(workspaceId))).thenReturn(true);
    final SourceDiscoverSchemaRequestBody requestBody =
        new SourceDiscoverSchemaRequestBody().sourceId(sourceId).disableCache(true).connectionId(connectionId).notifySchemaChange(true);
    final SourceDiscoverSchemaRead discoveryResult = new SourceDiscoverSchemaRead()
        .breakingChange(false)
        .catalog(catalog)
        .catalogDiff(catalogDiff)
        .catalogId(catalogId);
    when(mWorkspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(any())))
        .thenReturn(new WorkspaceRead().workspaceId(workspaceId));
    when(mSourceApi.discoverSchemaForSource(requestBody))
        .thenReturn(discoveryResult);

    refreshSchemaActivity.refreshSchema(sourceId, connectionId);
    verify(mSourceApi).discoverSchemaForSource(requestBody);
    verify(mWorkspaceApi).getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(connectionId));
    verify(mSourceApi).applySchemaChangeForSource(new SourceAutoPropagateChange()
        .catalogId(catalogId)
        .sourceId(sourceId)
        .catalog(catalog)
        .workspaceId(workspaceId));
  }

  @Test
  void testRefreshSchemaWithRefreshSchemaFeatureFlagAsFalse() {
    final UUID sourceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    when(mFeatureFlagClient.boolVariation(ShouldRunRefreshSchema.INSTANCE, new Connection(connectionId))).thenReturn(false);
    refreshSchemaActivity.refreshSchema(sourceId, connectionId);
    verifyNoInteractions(mSourceApi);
  }

  @Test
  void testRefreshSchemaWithAutoPropagateFeatureFlagAsFalse() throws ApiException {
    final UUID sourceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(mFeatureFlagClient.boolVariation(ShouldRunRefreshSchema.INSTANCE, new Connection(connectionId))).thenReturn(true);
    when(mFeatureFlagClient.boolVariation(eq(AutoPropagateSchema.INSTANCE), any())).thenReturn(false);
    when(mWorkspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(connectionId)))
        .thenReturn(new WorkspaceRead().workspaceId(workspaceId));
    refreshSchemaActivity.refreshSchema(sourceId, connectionId);
    verify(mSourceApi, times(0)).applySchemaChangeForSource(any());
  }

}
