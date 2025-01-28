/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.AirbyteConfigValidator;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.airbyte.workers.internal.stateaggregator.StateAggregatorFactory;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * Micronaut bean factory for general singletons.
 */
@Factory
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class ApplicationBeanFactory {

  @Singleton
  @Named("workspaceRoot")
  public Path workspaceRoot(@Value("${airbyte.workspace.root}") final String workspaceRoot) {
    return Path.of(workspaceRoot);
  }

  @Singleton
  @Named("currentSecondsSupplier")
  public Supplier<Long> currentSecondsSupplier() {
    return () -> Instant.now().getEpochSecond();
  }

  @Singleton
  public AirbyteProtocolVersionRange airbyteProtocolVersionRange(
                                                                 @Value("${airbyte.protocol.min-version}") final String minVersion,
                                                                 @Value("${airbyte.protocol.max-version}") final String maxVersion) {
    return new AirbyteProtocolVersionRange(new Version(minVersion), new Version(maxVersion));
  }

  @Singleton
  public AirbyteConfigValidator airbyteConfigValidator() {
    return new AirbyteConfigValidator();
  }

  @Singleton
  public MetricClient metricClient() {
    // Initialize the metric client
    MetricClientFactory.initialize(MetricEmittingApps.WORKER);
    return MetricClientFactory.getMetricClient();
  }

  @Prototype
  @Named("syncPersistenceExecutorService")
  public ScheduledExecutorService syncPersistenceExecutorService() {
    return Executors.newSingleThreadScheduledExecutor();
  }

  @Singleton
  public StateAggregatorFactory stateAggregatorFactory() {
    return new StateAggregatorFactory();
  }

}
