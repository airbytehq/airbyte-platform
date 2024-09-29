/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.PostprocessDiscoveredCatalogRequestBody;
import io.airbyte.api.client.model.generated.PostprocessDiscoveredCatalogResult;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.workers.helper.CatalogDiffConverter;
import io.airbyte.workers.models.PostprocessCatalogInput;
import io.airbyte.workers.models.PostprocessCatalogOutput;
import jakarta.inject.Singleton;
import java.util.Objects;

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
  public void reportSuccess() {
    metricClient.count(OssMetricsRegistry.CATALOG_DISCOVERY, 1,
        new MetricAttribute(MetricTags.STATUS, "success"));
  }

  @Override
  public void reportFailure() {
    metricClient.count(OssMetricsRegistry.CATALOG_DISCOVERY, 1,
        new MetricAttribute(MetricTags.STATUS, "failed"));
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
