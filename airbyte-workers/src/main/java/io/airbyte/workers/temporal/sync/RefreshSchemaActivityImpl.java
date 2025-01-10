/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ActorCatalogWithUpdatedAt;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.RefreshSchemaPeriod;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Refresh schema temporal activity impl.
 */
@Singleton
public class RefreshSchemaActivityImpl implements RefreshSchemaActivity {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AirbyteApiClient airbyteApiClient;
  private final FeatureFlagClient featureFlagClient;

  public RefreshSchemaActivityImpl(final AirbyteApiClient airbyteApiClient,
                                   final FeatureFlagClient featureFlagClient) {
    this.airbyteApiClient = airbyteApiClient;
    this.featureFlagClient = featureFlagClient;
  }

  @Override
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  public boolean shouldRefreshSchema(final UUID sourceCatalogId) {
    ApmTraceUtils.addTagsToTrace(Map.of(SOURCE_ID_KEY, sourceCatalogId));
    return !schemaRefreshRanRecently(sourceCatalogId);
  }

  private boolean schemaRefreshRanRecently(final UUID sourceCatalogId) {
    try {
      final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody(sourceCatalogId);
      final ActorCatalogWithUpdatedAt mostRecentFetchEvent = airbyteApiClient.getSourceApi().getMostRecentSourceActorCatalog(sourceIdRequestBody);
      if (mostRecentFetchEvent.getUpdatedAt() == null) {
        return false;
      }
      final UUID workspaceId = airbyteApiClient.getSourceApi().getSource(sourceIdRequestBody).getWorkspaceId();
      int refreshPeriod = 24;
      if (workspaceId != null) {
        refreshPeriod = featureFlagClient.intVariation(RefreshSchemaPeriod.INSTANCE, new Workspace(workspaceId));
      }
      return mostRecentFetchEvent.getUpdatedAt() > OffsetDateTime.now().minusHours(refreshPeriod).toEpochSecond();
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      // catching this exception because we don't want to block replication due to a failed schema refresh
      log.info("Encountered an error fetching most recent actor catalog fetch event: ", e);
      return true;
    }
  }

}
