/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.sync_persistence;

import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.UseParallelStreamStatsTracker;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.internal.book_keeping.ParallelStreamStatsTracker;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * A Factory for SyncPersistence.
 * <p>
 * Because we currently have two execution path with two different lifecycles, one being the
 * duration of a sync, the other duration of a worker. We introduce a Factory to keep an explicit
 * control of the lifecycle of this bean.
 * <p>
 * We use this factory rather to avoid having to directly inject ApplicationContext into classes
 * that would need to instantiate a SyncPersistence.
 */
@Singleton
@Slf4j
public class SyncPersistenceFactory {

  private final ApplicationContext applicationContext;
  private final FeatureFlagClient featureFlagClient;

  public SyncPersistenceFactory(final ApplicationContext applicationContext, final FeatureFlagClient featureFlagClient) {
    this.applicationContext = applicationContext;
    this.featureFlagClient = featureFlagClient;
  }

  /**
   * Get an instance of SyncPersistence.
   */
  public SyncPersistence get(final UUID connectionId,
                             final Long jobId,
                             final Integer attemptNumber,
                             final ConfiguredAirbyteCatalog configuredAirbyteCatalog) {
    final SyncPersistence syncPersistence = applicationContext.getBean(SyncPersistence.class);
    final MetricClient metricClient = MetricClientFactory.getMetricClient();

    // Check whether we want to use the new implementation and switch it up.
    // This isn't ideal but, we do not have a dynamic implementation selector that is leveraging feature
    // flags.
    final boolean useParallelSyncStatsTracker = featureFlagClient.boolVariation(UseParallelStreamStatsTracker.INSTANCE, new Connection(connectionId));
    if (useParallelSyncStatsTracker && syncPersistence instanceof SyncPersistenceImpl) {
      metricClient.count(OssMetricsRegistry.STATS_TRACKER_IMPLEMENTATION, 1, new MetricAttribute(MetricTags.IMPLEMENTATION, "parallel"));
      ((SyncPersistenceImpl) syncPersistence).setSyncStatsTracker(new ParallelStreamStatsTracker(metricClient));
      log.info("Using parallel stream stats tracking");
    } else {
      metricClient.count(OssMetricsRegistry.STATS_TRACKER_IMPLEMENTATION, 1, new MetricAttribute(MetricTags.IMPLEMENTATION, "default"));
      log.info("Using default stats tracking");
    }

    syncPersistence.setConnectionContext(connectionId, jobId, attemptNumber, configuredAirbyteCatalog);
    return syncPersistence;
  }

}
