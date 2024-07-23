/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.PostprocessDiscoveredCatalogRequestBody;
import io.airbyte.api.client.model.generated.PostprocessDiscoveredCatalogResult;
import io.airbyte.featureflag.Empty;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.UseWorkloadApi;
import io.airbyte.featureflag.WorkloadApiServerEnabled;
import io.airbyte.featureflag.WorkloadLauncherEnabled;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.workers.helper.CatalogDiffConverter;
import io.airbyte.workers.models.PostprocessCatalogInput;
import io.airbyte.workers.models.PostprocessCatalogOutput;
import jakarta.inject.Singleton;
import java.util.Objects;
import java.util.UUID;

@Singleton
public class DiscoverCatalogHelperActivityImpl implements DiscoverCatalogHelperActivity {

  private final AirbyteApiClient airbyteApiClient;
  private final FeatureFlagClient featureFlagClient;
  private final MetricClient metricClient;

  public DiscoverCatalogHelperActivityImpl(AirbyteApiClient airbyteApiClient, FeatureFlagClient featureFlagClient, MetricClient metricClient) {
    this.airbyteApiClient = airbyteApiClient;
    this.featureFlagClient = featureFlagClient;
    this.metricClient = metricClient;
  }

  @Override
  public boolean shouldUseWorkload(final UUID workspaceId) {
    var ffCheck = featureFlagClient.boolVariation(UseWorkloadApi.INSTANCE, new Workspace(workspaceId));
    var envCheck = featureFlagClient.boolVariation(WorkloadLauncherEnabled.INSTANCE, Empty.INSTANCE)
        && featureFlagClient.boolVariation(WorkloadApiServerEnabled.INSTANCE, Empty.INSTANCE);

    return ffCheck || envCheck;
  }

  @Override
  public void reportSuccess(final Boolean workloadEnabled) {
    final var workloadEnabledStr = workloadEnabled != null ? workloadEnabled.toString() : "unknown";
    metricClient.count(OssMetricsRegistry.CATALOG_DISCOVERY, 1,
        new MetricAttribute(MetricTags.STATUS, "success"),
        new MetricAttribute("workload_enabled", workloadEnabledStr));
  }

  @Override
  public void reportFailure(final Boolean workloadEnabled) {
    final var workloadEnabledStr = workloadEnabled != null ? workloadEnabled.toString() : "unknown";
    metricClient.count(OssMetricsRegistry.CATALOG_DISCOVERY, 1,
        new MetricAttribute(MetricTags.STATUS, "failed"),
        new MetricAttribute("workload_enabled", workloadEnabledStr));
  }

  @Override
  public PostprocessCatalogOutput postprocess(final PostprocessCatalogInput input) {
    try {
      Objects.requireNonNull(input.getConnectionId());
      Objects.requireNonNull(input.getCatalogId());

      final var reqBody = new PostprocessDiscoveredCatalogRequestBody(
          input.getCatalogId(),
          input.getConnectionId());

      final PostprocessDiscoveredCatalogResult resp = airbyteApiClient.getConnectionApi().postprocessDiscoveredCatalogForConnection(reqBody);

      final var domainDiff = resp.getAppliedDiff() != null ? CatalogDiffConverter.toDomain(resp.getAppliedDiff()) : null;

      return PostprocessCatalogOutput.Companion.success(domainDiff);
    } catch (final Exception e) {
      return PostprocessCatalogOutput.Companion.failure(e);
    }
  }

}
