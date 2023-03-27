/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.model.generated.ActorCatalogWithUpdatedAt;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.metrics.lib.ApmTraceUtils;
import jakarta.inject.Singleton;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Refresh schema temporal activity impl.
 */
@Slf4j
@Singleton
public class RefreshSchemaActivityImpl implements RefreshSchemaActivity {

  private final SourceApi sourceApi;
  private final EnvVariableFeatureFlags envVariableFeatureFlags;

  public RefreshSchemaActivityImpl(final SourceApi sourceApi,
                                   final EnvVariableFeatureFlags envVariableFeatureFlags) {
    this.sourceApi = sourceApi;
    this.envVariableFeatureFlags = envVariableFeatureFlags;
  }

  @Override
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  public boolean shouldRefreshSchema(final UUID sourceCatalogId) {
    if (!envVariableFeatureFlags.autoDetectSchema()) {
      return false;
    }

    ApmTraceUtils.addTagsToTrace(Map.of(SOURCE_ID_KEY, sourceCatalogId));
    return !schemaRefreshRanRecently(sourceCatalogId);
  }

  @Override
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  public void refreshSchema(final UUID sourceCatalogId, final UUID connectionId) {
    if (!envVariableFeatureFlags.autoDetectSchema()) {
      return;
    }

    ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, connectionId, SOURCE_ID_KEY, sourceCatalogId));

    final SourceDiscoverSchemaRequestBody requestBody =
        new SourceDiscoverSchemaRequestBody().sourceId(sourceCatalogId).disableCache(true).connectionId(connectionId).notifySchemaChange(true);

    try {
      AirbyteApiClient.retryWithJitter(
          () -> sourceApi.discoverSchemaForSource(requestBody),
          "Trigger discover schema");
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      // catching this exception because we don't want to block replication due to a failed schema refresh
      log.error("Attempted schema refresh, but failed with error: ", e);
    }
  }

  private boolean schemaRefreshRanRecently(final UUID sourceCatalogId) {
    try {
      final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(sourceCatalogId);
      final ActorCatalogWithUpdatedAt mostRecentFetchEvent = AirbyteApiClient.retryWithJitter(
          () -> sourceApi.getMostRecentSourceActorCatalog(sourceIdRequestBody),
          "get the most recent source actor catalog");
      if (mostRecentFetchEvent.getUpdatedAt() == null) {
        return false;
      }
      return mostRecentFetchEvent.getUpdatedAt() > OffsetDateTime.now().minusHours(24L).toEpochSecond();
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      // catching this exception because we don't want to block replication due to a failed schema refresh
      log.info("Encountered an error fetching most recent actor catalog fetch event: ", e);
      return true;
    }
  }

}
